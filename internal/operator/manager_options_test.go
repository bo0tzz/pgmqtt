package operator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
)

func TestManagerOptions(t *testing.T) {
	t.Run("defaults keep the previous behaviour", func(t *testing.T) {
		mo := managerOptions(Options{LeaderElectionNamespace: "mqtt"}.withDefaults())
		if mo.LeaderElectionID != "pgmqtt-operator" {
			t.Errorf("LeaderElectionID = %q, want the value that used to be hard-coded", mo.LeaderElectionID)
		}
		if !mo.LeaderElection || mo.LeaderElectionNamespace != "mqtt" {
			t.Errorf("leader election: enabled=%v namespace=%q", mo.LeaderElection, mo.LeaderElectionNamespace)
		}
		if mo.Cache.DefaultNamespaces != nil {
			t.Errorf("cache restricted to %v by default; it must cover every namespace", mo.Cache.DefaultNamespaces)
		}
	})

	t.Run("lease ID and namespaces are passed through", func(t *testing.T) {
		mo := managerOptions(Options{
			LeaderElectionNamespace: "mqtt",
			LeaderElectionID:        "release-b-pgmqtt",
			WatchNamespaces:         []string{"apps", "mqtt"},
		}.withDefaults())
		if mo.LeaderElectionID != "release-b-pgmqtt" {
			t.Errorf("LeaderElectionID = %q", mo.LeaderElectionID)
		}
		var got []string
		for ns := range mo.Cache.DefaultNamespaces {
			got = append(got, ns)
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, []string{"apps", "mqtt"}) {
			t.Errorf("cache namespaces = %v, want [apps mqtt]", got)
		}
	})

	t.Run("secrets are never read through the cache", func(t *testing.T) {
		mo := managerOptions(Options{}.withDefaults())
		if mo.Client.Cache == nil {
			t.Fatal("client reads everything through the cache")
		}
		found := false
		for _, obj := range mo.Client.Cache.DisableFor {
			if _, ok := obj.(*corev1.Secret); ok {
				found = true
			}
		}
		if !found {
			t.Error("a cached Secret read would start an informer holding every Secret in full")
		}
		if mo.Cache.DefaultTransform == nil {
			t.Error("no cache transform: Secret metadata would be cached with labels and annotations")
		}
	})

	t.Run("no listeners of its own", func(t *testing.T) {
		mo := managerOptions(Options{}.withDefaults())
		if mo.HealthProbeBindAddress != "0" || mo.Metrics.BindAddress != "0" {
			t.Errorf("health=%q metrics=%q, want both disabled", mo.HealthProbeBindAddress, mo.Metrics.BindAddress)
		}
	})
}

// TestManagerCanBeBuiltTwiceInOneProcess builds the manager and registers
// the controller twice, as the supervisor does on a restart. controller-
// runtime rejects a controller name it has already seen in the process
// unless told otherwise; without that, every restart would fail in setup.
func TestManagerCanBeBuiltTwiceInOneProcess(t *testing.T) {
	// Nothing is contacted: the manager is never started.
	cfg := &rest.Config{Host: "http://127.0.0.1:1"}
	opts := Options{LeaderElectionNamespace: "mqtt"}.withDefaults()
	for i := 1; i <= 2; i++ {
		mgr, err := ctrl.NewManager(cfg, managerOptions(opts))
		if err != nil {
			t.Fatalf("manager %d: %v", i, err)
		}
		r := &UserReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
		if err := r.SetupWithManager(mgr); err != nil {
			t.Fatalf("manager %d: setup: %v", i, err)
		}
	}
}

func TestTrimForCache(t *testing.T) {
	owner := metav1.OwnerReference{APIVersion: "pgmqtt.io/v1alpha1", Kind: "User", Name: "ha", UID: "u-1"}
	in := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ha-mqtt-credentials", Namespace: "mqtt", UID: "s-1", ResourceVersion: "42",
			Labels: map[string]string{"app": "x"},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"password":"aHVudGVyMg=="}}`,
			},
			ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
			OwnerReferences: []metav1.OwnerReference{owner},
		},
	}
	out, err := trimForCache(in)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	got := out.(*metav1.PartialObjectMetadata)
	if got.Kind != "Secret" || got.Name != "ha-mqtt-credentials" || got.Namespace != "mqtt" ||
		got.UID != "s-1" || got.ResourceVersion != "42" {
		t.Errorf("identity lost: %+v", got)
	}
	if !reflect.DeepEqual(got.OwnerReferences, []metav1.OwnerReference{owner}) {
		t.Errorf("owner references lost (the Owns watch maps on them): %+v", got.OwnerReferences)
	}
	if got.Labels != nil || got.Annotations != nil || got.ManagedFields != nil {
		t.Errorf("kept more than identity: labels=%v annotations=%v managedFields=%v",
			got.Labels, got.Annotations, got.ManagedFields)
	}

	user := &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ha", Namespace: "mqtt",
			Labels:        map[string]string{"team": "a"},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
		},
		Spec: pgmqttv1alpha1.UserSpec{Username: "homeassistant"},
	}
	out, err = trimForCache(user)
	if err != nil {
		t.Fatalf("trim user: %v", err)
	}
	gotUser := out.(*pgmqttv1alpha1.User)
	if gotUser.Spec.Username != "homeassistant" || gotUser.Labels["team"] != "a" {
		t.Errorf("User objects must be cached whole: %+v", gotUser)
	}
	if gotUser.ManagedFields != nil {
		t.Error("managedFields kept on a cached User")
	}
}

// TestUserCRDServed drives the discovery call against canned API server
// answers. Only a 404 for the group/version may be read as "not installed".
func TestUserCRDServed(t *testing.T) {
	const served = `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"pgmqtt.io/v1alpha1","resources":[
		{"name":"users","singularName":"user","namespaced":true,"kind":"User","verbs":["get","list","watch"]},
		{"name":"users/status","singularName":"","namespaced":true,"kind":"User","verbs":["get","patch","update"]}]}`
	const otherKind = `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"pgmqtt.io/v1alpha1","resources":[
		{"name":"things","singularName":"thing","namespaced":true,"kind":"Thing","verbs":["get"]}]}`
	const notFound = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,
		"message":"the server could not find the requested resource"}`
	const forbidden = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,
		"message":"forbidden: User \"system:serviceaccount:mqtt:pgmqtt\" cannot get path \"/apis/pgmqtt.io/v1alpha1\""}`

	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantServed bool
		wantErr    bool
	}{
		{"CRD installed", http.StatusOK, served, true, false},
		{"group has no User kind", http.StatusOK, otherKind, false, false},
		{"CRD not installed", http.StatusNotFound, notFound, false, false},
		{"forbidden", http.StatusForbidden, forbidden, false, true},
		{"API server error", http.StatusInternalServerError, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":500}`, false, true},
		{"API server overloaded", http.StatusServiceUnavailable, `service unavailable`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/apis/pgmqtt.io/v1alpha1" {
					t.Errorf("unexpected request %s", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			got, err := userCRDServed(context.Background(), &rest.Config{Host: srv.URL})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.wantServed {
				t.Errorf("served = %v, want %v", got, tc.wantServed)
			}
		})
	}

	t.Run("API server unreachable", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		addr := srv.URL
		srv.Close()
		if _, err := userCRDServed(context.Background(), &rest.Config{Host: addr}); err == nil {
			t.Fatal("a refused connection was read as an answer about the CRD")
		}
	})
}
