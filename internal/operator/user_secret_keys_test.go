package operator_test

import (
	"context"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

func generatedSecret(t *testing.T, cli client.Client, namespace, userName string) map[string]string {
	t.Helper()
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: userName + "-mqtt-credentials"}
	if err := cli.Get(context.Background(), key, &sec); err != nil {
		t.Fatalf("get generated secret %s: %v", key, err)
	}
	out := map[string]string{}
	for k, v := range sec.Data {
		out[k] = string(v)
	}
	return out
}

func keysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func assertKeys(t *testing.T, got map[string]string, want ...string) {
	t.Helper()
	sort.Strings(want)
	gotKeys := keysOf(got)
	if len(gotKeys) != len(want) {
		t.Errorf("secret keys = %v, want %v", gotKeys, want)
		return
	}
	for i := range want {
		if gotKeys[i] != want[i] {
			t.Errorf("secret keys = %v, want %v", gotKeys, want)
			return
		}
	}
}

// TestGeneratedSecret_DisabledListenerIsNotAdvertised walks one User's
// Secret through the listener combinations. A port of 0 is how the chart
// says "this listener is disabled"; the Secret must stop naming it,
// including when it named it before.
func TestGeneratedSecret_DisabledListenerIsNotAdvertised(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool, &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "app", UID: "uid-app"},
	})
	r.ServiceHost = "broker.mqtt.svc"

	settle(t, r, "mqtt", "app")
	both := generatedSecret(t, cli, "mqtt", "app")
	assertKeys(t, both, "username", "password", "host", "port", "uri", "ws-port", "ws-uri")
	password := both["password"]
	if want := "mqtt://app:" + password + "@broker.mqtt.svc:1883"; both["uri"] != want {
		t.Errorf("uri = %q, want %q", both["uri"], want)
	}
	if want := "ws://app:" + password + "@broker.mqtt.svc:8083/mqtt"; both["ws-uri"] != want {
		t.Errorf("ws-uri = %q, want %q", both["ws-uri"], want)
	}

	for _, step := range []struct {
		name          string
		tcp, ws       int
		wantEndpoints []string
	}{
		{"websocket disabled", 1883, 0, []string{"port", "uri"}},
		{"tcp disabled", 0, 8083, []string{"ws-port", "ws-uri"}},
		{"neither advertised", 0, 0, nil},
		{"both back", 1884, 8084, []string{"port", "uri", "ws-port", "ws-uri"}},
	} {
		r.ServicePort, r.WSPort = step.tcp, step.ws
		if _, err := reconcileUser(r, "mqtt", "app"); err != nil {
			t.Fatalf("%s: reconcile: %v", step.name, err)
		}
		got := generatedSecret(t, cli, "mqtt", "app")
		assertKeys(t, got, append([]string{"username", "password", "host"}, step.wantEndpoints...)...)
		if got["password"] != password {
			t.Fatalf("%s: password changed", step.name)
		}
	}
	if got := generatedSecret(t, cli, "mqtt", "app"); got["port"] != "1884" || got["ws-port"] != "8084" {
		t.Errorf("ports after re-enabling: %q / %q, want 1884 / 8084", got["port"], got["ws-port"])
	}
	assertVerifies(t, pool, "app", []byte(password))
}

// TestGeneratedSecret_TLSEndpoint covers the keys the CRD documents for a
// broker configured with a TLS endpoint, and their removal when that
// configuration goes away.
func TestGeneratedSecret_TLSEndpoint(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool, &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "app", UID: "uid-app"},
	})
	base := []string{"username", "password", "host", "port", "uri", "ws-port", "ws-uri"}

	r.TLSHost, r.TLSPort, r.WSSPort = "mqtt.example.com", 8883, 443
	settle(t, r, "mqtt", "app")
	got := generatedSecret(t, cli, "mqtt", "app")
	assertKeys(t, got, append(base, "tls-host", "tls-port", "mqtts-uri", "wss-port", "wss-uri")...)
	password := got["password"]
	for key, want := range map[string]string{
		"tls-host":  "mqtt.example.com",
		"tls-port":  "8883",
		"mqtts-uri": "mqtts://app:" + password + "@mqtt.example.com:8883",
		"wss-port":  "443",
		"wss-uri":   "wss://app:" + password + "@mqtt.example.com:443/mqtt",
	} {
		if got[key] != want {
			t.Errorf("%s = %q, want %q", key, got[key], want)
		}
	}

	r.WSSPort = 0
	if _, err := reconcileUser(r, "mqtt", "app"); err != nil {
		t.Fatalf("reconcile without wss: %v", err)
	}
	assertKeys(t, generatedSecret(t, cli, "mqtt", "app"), append(base, "tls-host", "tls-port", "mqtts-uri")...)

	r.TLSPort, r.WSSPort = 0, 443
	if _, err := reconcileUser(r, "mqtt", "app"); err != nil {
		t.Fatalf("reconcile with wss only: %v", err)
	}
	assertKeys(t, generatedSecret(t, cli, "mqtt", "app"), append(base, "tls-host", "wss-port", "wss-uri")...)

	// Ports left set on purpose: without a host there is no TLS endpoint.
	r.TLSHost, r.TLSPort = "", 8883
	if _, err := reconcileUser(r, "mqtt", "app"); err != nil {
		t.Fatalf("reconcile without TLS host: %v", err)
	}
	assertKeys(t, generatedSecret(t, cli, "mqtt", "app"), base...)
}
