package operator_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestStatusCarriesNothingDerivedFromThePassword serialises the whole
// status after a reconcile and looks for the password and its SHA-256, the
// value releases before 0.3.0 published there.
func TestStatusCarriesNothingDerivedFromThePassword(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	const password = "correct horse battery staple"
	r, cli := newTestReconciler(t, pool,
		byoUser("mqtt", "u", "creds"),
		passwordSecret("mqtt", "creds", password),
	)
	settle(t, r, "mqtt", "u")

	u := getUser(t, cli, "mqtt", "u")
	raw, err := json.Marshal(u.Status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	for what, needle := range map[string]string{
		"the password":        password,
		"the password's hash": sha256Hex(password),
	} {
		if strings.Contains(string(raw), needle) {
			t.Errorf("status contains %s: %s", what, raw)
		}
	}
	if u.Status.ObservedSecretVersion == "" {
		t.Error("status.observedSecretVersion not set, so every reconcile would compare the password again")
	}
}

// TestLegacyPasswordDigestIsCleared covers Users reconciled by an older
// release: the digest has to go on the first pass, including passes that
// end in a failure or a conflict rather than Ready.
func TestLegacyPasswordDigestIsCleared(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	legacyStatus := func(username, secret string) pgmqttv1alpha1.UserStatus {
		return pgmqttv1alpha1.UserStatus{
			ObservedSecretHash:   sha256Hex("pw"),
			ObservedUsername:     username,
			CredentialsSecretRef: &corev1.LocalObjectReference{Name: secret},
		}
	}

	ok := byoUser("mqtt", "ok", "ok-creds")
	ok.Status = legacyStatus("ok", "ok-creds")

	broken := byoUser("mqtt", "broken", "missing-creds")
	broken.Status = legacyStatus("broken", "missing-creds")

	holder := namedUser("mqtt", "holder", "wanted")
	loser := namedUser("mqtt", "loser", "wanted")
	loser.Status = legacyStatus("wanted", "loser-mqtt-credentials")

	r, cli := newTestReconciler(t, pool, ok, broken, holder, loser,
		passwordSecret("mqtt", "ok-creds", "pw"))

	settle(t, r, "mqtt", "ok")
	settle(t, r, "mqtt", "holder")
	settle(t, r, "mqtt", "loser")
	if _, err := reconcileUser(r, "mqtt", "broken"); err != nil {
		t.Fatalf("finalizer pass: %v", err)
	}
	if _, err := reconcileUser(r, "mqtt", "broken"); err == nil {
		t.Fatal("expected the missing Secret to fail the reconcile")
	}

	for name, wantReady := range map[string]metav1.ConditionStatus{
		"ok":     metav1.ConditionTrue,
		"broken": metav1.ConditionFalse,
		"loser":  metav1.ConditionFalse,
	} {
		u := getUser(t, cli, "mqtt", name)
		if got := readyCondition(u).Status; got != wantReady {
			t.Errorf("%s: Ready = %s, want %s", name, got, wantReady)
		}
		if u.Status.ObservedSecretHash != "" {
			t.Errorf("%s: status.observedSecretHash still holds the digest", name)
		}
	}
}

// TestUpgradeAdoptsRowWithoutRehashing is the first reconcile after an
// upgrade for a User whose password did not change: the row gets an owner,
// the hash stays as it is.
func TestUpgradeAdoptsRowWithoutRehashing(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	existing, _ := bcrypt.GenerateFromPassword([]byte("pw"), 6)
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO users(username, password_hash) VALUES('up', $1)`, string(existing)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	u := byoUser("mqtt", "up", "up-creds")
	u.Finalizers = []string{"pgmqtt.io/user"}
	u.Status = pgmqttv1alpha1.UserStatus{
		ObservedSecretHash:   sha256Hex("pw"),
		ObservedUsername:     "up",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "up-creds"},
	}
	r, _ := newTestReconciler(t, pool, u, passwordSecret("mqtt", "up-creds", "pw"))

	if _, err := reconcileUser(r, "mqtt", "up"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := storedHash(t, pool, "up"); got != string(existing) {
		t.Errorf("hash was rewritten although the password is unchanged and its cost (6) is above the configured 4")
	}
	assertOwner(t, pool, "up", u)
}

// TestSecretChangeWithoutPasswordChangeKeepsHash: a Secret's
// resourceVersion moves for any edit. One that leaves the password alone
// must not rotate the stored hash.
func TestSecretChangeWithoutPasswordChangeKeepsHash(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool,
		byoUser("mqtt", "u", "creds"),
		passwordSecret("mqtt", "creds", "pw"),
	)
	settle(t, r, "mqtt", "u")
	hashBefore := storedHash(t, pool, "u")
	versionBefore := getUser(t, cli, "mqtt", "u").Status.ObservedSecretVersion

	var sec corev1.Secret
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: "mqtt", Name: "creds"}, &sec); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	sec.Labels = map[string]string{"touched": "yes"}
	if err := cli.Update(context.Background(), &sec); err != nil {
		t.Fatalf("label secret: %v", err)
	}

	if _, err := reconcileUser(r, "mqtt", "u"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if storedHash(t, pool, "u") != hashBefore {
		t.Error("hash rewritten although the password did not change")
	}
	if got := getUser(t, cli, "mqtt", "u").Status.ObservedSecretVersion; got == versionBefore {
		t.Error("status.observedSecretVersion did not follow the Secret, so the next pass compares the password again")
	}
}

// TestSettledReconcileWritesNothing reconciles a User that needs no
// change and counts writes to the API and the row.
func TestSettledReconcileWritesNothing(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, base := newTestReconciler(t, pool, &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "quiet", UID: "uid-quiet"},
	})
	settle(t, r, "mqtt", "quiet")
	// The pass that wrote status produces one more event in a real cluster;
	// run it so the next one is the steady state.
	if _, err := reconcileUser(r, "mqtt", "quiet"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	rowVersion := func() string {
		var xmin string
		if err := pool.QueryRow(context.Background(),
			`SELECT xmin::text FROM users WHERE username='quiet'`).Scan(&xmin); err != nil {
			t.Fatalf("read xmin: %v", err)
		}
		return xmin
	}
	rowBefore := rowVersion()

	var apiWrites atomic.Int32
	count := func() { apiWrites.Add(1) }
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			count()
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			count()
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			count()
			return c.Patch(ctx, obj, patch, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			count()
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})

	if _, err := reconcileUser(r, "mqtt", "quiet"); err != nil {
		t.Fatalf("settled reconcile: %v", err)
	}
	if n := apiWrites.Load(); n != 0 {
		t.Errorf("settled reconcile made %d API writes, want 0", n)
	}
	if rowVersion() != rowBefore {
		t.Error("settled reconcile rewrote the users row")
	}
}

// TestCostBumpRehashesSettledUser raises the bcrypt cost for a User whose
// Secret has not changed. Not parallel: it moves the cost_bump counter that
// TestRehashOnCostBump measures.
func TestCostBumpRehashesSettledUser(t *testing.T) {
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool, &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "bump", UID: "uid-bump"},
	})
	settle(t, r, "mqtt", "bump")
	if !strings.HasPrefix(storedHash(t, pool, "bump"), "$2a$04$") {
		t.Fatalf("setup: expected a cost-4 hash, got %q", storedHash(t, pool, "bump")[:7])
	}

	r.BcryptCost = 6
	if _, err := reconcileUser(r, "mqtt", "bump"); err != nil {
		t.Fatalf("reconcile after cost bump: %v", err)
	}
	if got := storedHash(t, pool, "bump"); !strings.HasPrefix(got, "$2a$06$") {
		t.Errorf("hash prefix %q after raising the cost to 6", got[:7])
	}
	assertVerifies(t, pool, "bump", generatedPassword(t, cli, "mqtt", "bump"))
}
