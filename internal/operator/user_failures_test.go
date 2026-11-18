package operator_test

import (
	"context"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

// TestReconcile_BYOEmptyPasswordCreatesNoAccount covers a bring-your-own
// Secret whose password key is present but empty. Hashing it would create
// an account that authenticates with a username alone.
func TestReconcile_BYOEmptyPasswordCreatesNoAccount(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool,
		byoUser("mqtt", "blank", "blank-creds"),
		passwordSecret("mqtt", "blank-creds", ""),
	)

	if _, err := reconcileUser(r, "mqtt", "blank"); err != nil {
		t.Fatalf("finalizer pass: %v", err)
	}
	if _, err := reconcileUser(r, "mqtt", "blank"); err == nil {
		t.Fatal("reconcile accepted an empty password")
	}
	if n := countUsers(t, pool, "blank"); n != 0 {
		t.Fatalf("an account was created from an empty password (%d rows)", n)
	}
	cond := readyCondition(getUser(t, cli, "mqtt", "blank"))
	if cond.Status != metav1.ConditionFalse || cond.Reason != "SecretError" {
		t.Errorf("Ready = %s/%s, want False/SecretError", cond.Status, cond.Reason)
	}
	if !strings.Contains(cond.Message, "blank-creds") || !strings.Contains(cond.Message, "empty") {
		t.Errorf("message should name the Secret and the problem, got %q", cond.Message)
	}
}

// TestReconcile_BYOPasswordEmptiedKeepsExistingAccount pins what happens
// when a working Secret is later blanked: the User goes not-Ready and the
// row keeps the last good hash rather than being replaced by a hash of "".
func TestReconcile_BYOPasswordEmptiedKeepsExistingAccount(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool,
		byoUser("mqtt", "u", "creds"),
		passwordSecret("mqtt", "creds", "first"),
	)
	settle(t, r, "mqtt", "u")

	var sec corev1.Secret
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: "mqtt", Name: "creds"}, &sec); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	sec.Data["password"] = nil
	if err := cli.Update(context.Background(), &sec); err != nil {
		t.Fatalf("blank secret: %v", err)
	}

	if _, err := reconcileUser(r, "mqtt", "u"); err == nil {
		t.Fatal("reconcile accepted an empty password")
	}
	h := storedHash(t, pool, "u")
	if bcrypt.CompareHashAndPassword([]byte(h), nil) == nil {
		t.Fatal("stored hash now verifies the empty password")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(h), []byte("first")); err != nil {
		t.Errorf("stored hash no longer verifies the last good password: %v", err)
	}
	if cond := readyCondition(getUser(t, cli, "mqtt", "u")); cond.Status != metav1.ConditionFalse {
		t.Errorf("Ready = %s, want False", cond.Status)
	}
}

// TestReconcile_SecretProblemsAreNamedInStatus checks that the status says
// what is wrong with the Secret. Every one of these used to read
// "internal error".
func TestReconcile_SecretProblemsAreNamedInStatus(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)

	customKey := byoUser("mqtt", "wrong-key", "creds")
	customKey.Spec.PasswordSecretRef.Key = "token"
	r, cli := newTestReconciler(t, pool,
		byoUser("mqtt", "no-secret", "absent"),
		customKey,
		passwordSecret("mqtt", "creds", "pw"),
	)

	for name, want := range map[string]string{
		"no-secret": `secret "absent": not found`,
		"wrong-key": `secret "creds" has no key "token"`,
	} {
		if _, err := reconcileUser(r, "mqtt", name); err != nil {
			t.Fatalf("%s finalizer pass: %v", name, err)
		}
		if _, err := reconcileUser(r, "mqtt", name); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		cond := readyCondition(getUser(t, cli, "mqtt", name))
		if cond.Status != metav1.ConditionFalse || cond.Reason != "SecretError" {
			t.Errorf("%s: Ready = %s/%s, want False/SecretError", name, cond.Status, cond.Reason)
		}
		if cond.Message != want {
			t.Errorf("%s: message = %q, want %q", name, cond.Message, want)
		}
	}
}

// TestReconcile_DatabaseFailureOnSettledUserSetsReadyFalse reconciles a
// User that needs no changes while the users table is unavailable. That
// path returned the error without touching the condition, so the User kept
// showing Ready=True.
func TestReconcile_DatabaseFailureOnSettledUserSetsReadyFalse(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	r, cli := newTestReconciler(t, pool, &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "settled", UID: "uid-settled"},
	})
	settle(t, r, "mqtt", "settled")
	if cond := readyCondition(getUser(t, cli, "mqtt", "settled")); cond.Status != metav1.ConditionTrue {
		t.Fatalf("setup: Ready = %s, want True", cond.Status)
	}

	if _, err := pool.Exec(context.Background(), `ALTER TABLE users RENAME TO users_gone`); err != nil {
		t.Fatalf("break users table: %v", err)
	}
	if _, err := reconcileUser(r, "mqtt", "settled"); err == nil {
		t.Fatal("expected an error with the users table gone")
	}
	cond := readyCondition(getUser(t, cli, "mqtt", "settled"))
	if cond.Status != metav1.ConditionFalse || cond.Reason != "DBError" {
		t.Errorf("Ready = %s/%s, want False/DBError", cond.Status, cond.Reason)
	}
}

func TestReconcile_HashFailureSetsReadyFalse(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)

	t.Run("password too long for bcrypt", func(t *testing.T) {
		r, cli := newTestReconciler(t, pool,
			byoUser("mqtt", "long", "long-creds"),
			passwordSecret("mqtt", "long-creds", strings.Repeat("x", 73)),
		)
		if _, err := reconcileUser(r, "mqtt", "long"); err != nil {
			t.Fatalf("finalizer pass: %v", err)
		}
		if _, err := reconcileUser(r, "mqtt", "long"); err == nil {
			t.Fatal("expected an error for a 73-byte password")
		}
		cond := readyCondition(getUser(t, cli, "mqtt", "long"))
		if cond.Status != metav1.ConditionFalse || cond.Reason != "HashError" {
			t.Errorf("Ready = %s/%s, want False/HashError", cond.Status, cond.Reason)
		}
		if !strings.Contains(cond.Message, "72 bytes") {
			t.Errorf("message should give the limit, got %q", cond.Message)
		}
		if n := countUsers(t, pool, "long"); n != 0 {
			t.Errorf("row written despite the hash failure (%d rows)", n)
		}
	})

	t.Run("cost bcrypt rejects", func(t *testing.T) {
		r, cli := newTestReconciler(t, pool, &pgmqttv1alpha1.User{
			ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "badcost", UID: "uid-badcost"},
		})
		r.BcryptCost = 32
		if _, err := reconcileUser(r, "mqtt", "badcost"); err != nil {
			t.Fatalf("finalizer pass: %v", err)
		}
		if _, err := reconcileUser(r, "mqtt", "badcost"); err == nil {
			t.Fatal("expected an error for bcrypt cost 32")
		}
		cond := readyCondition(getUser(t, cli, "mqtt", "badcost"))
		if cond.Status != metav1.ConditionFalse || cond.Reason != "HashError" {
			t.Errorf("Ready = %s/%s, want False/HashError", cond.Status, cond.Reason)
		}
	})
}

// TestReconcile_DeleteFailureSetsReadyFalse: a User stuck in Terminating
// because the row cannot be removed should say so.
func TestReconcile_DeleteFailureSetsReadyFalse(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	u := &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: "mqtt", Name: "stuck", UID: "uid-stuck"},
	}
	r, cli := newTestReconciler(t, pool, u)
	settle(t, r, "mqtt", "stuck")

	if err := cli.Delete(context.Background(), u); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `ALTER TABLE users RENAME TO users_gone`); err != nil {
		t.Fatalf("break users table: %v", err)
	}
	if _, err := reconcileUser(r, "mqtt", "stuck"); err == nil {
		t.Fatal("expected an error with the users table gone")
	}
	got := getUser(t, cli, "mqtt", "stuck")
	if len(got.Finalizers) == 0 {
		t.Fatal("finalizer removed although the row could not be deleted")
	}
	cond := readyCondition(got)
	if cond.Status != metav1.ConditionFalse || cond.Reason != "DBError" {
		t.Errorf("Ready = %s/%s, want False/DBError", cond.Status, cond.Reason)
	}
}
