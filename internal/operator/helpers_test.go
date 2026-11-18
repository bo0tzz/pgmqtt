package operator_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	"github.com/bo0tzz/pgmqtt/internal/operator"
)

// newTestReconciler builds a reconciler over a fake client seeded with objs.
func newTestReconciler(t *testing.T, pool *pgxpool.Pool, objs ...client.Object) (*operator.UserReconciler, client.Client) {
	t.Helper()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(objs...).WithStatusSubresource(&pgmqttv1alpha1.User{}).Build()
	return &operator.UserReconciler{
		Client: cli, Scheme: scheme, Pool: pool,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		ServiceHost: "pgmqtt", ServicePort: 1883, WSPort: 8083,
		BcryptCost: 4,
	}, cli
}

func reconcileUser(r *operator.UserReconciler, namespace, name string) (ctrl.Result, error) {
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
	})
}

// settle reconciles until the User has its finalizer and has been through
// one full pass, failing the test on any error.
func settle(t *testing.T, r *operator.UserReconciler, namespace, name string) {
	t.Helper()
	for i := 0; i < 2; i++ {
		if _, err := reconcileUser(r, namespace, name); err != nil {
			t.Fatalf("reconcile %s/%s (pass %d): %v", namespace, name, i+1, err)
		}
	}
}

func getUser(t *testing.T, cli client.Client, namespace, name string) pgmqttv1alpha1.User {
	t.Helper()
	var u pgmqttv1alpha1.User
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, &u); err != nil {
		t.Fatalf("get user %s/%s: %v", namespace, name, err)
	}
	return u
}

func readyCondition(u pgmqttv1alpha1.User) metav1.Condition {
	for _, c := range u.Status.Conditions {
		if c.Type == "Ready" {
			return c
		}
	}
	return metav1.Condition{}
}

func countUsers(t *testing.T, pool *pgxpool.Pool, username string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM users WHERE username=$1`, username).Scan(&n); err != nil {
		t.Fatalf("count users %q: %v", username, err)
	}
	return n
}

func storedHash(t *testing.T, pool *pgxpool.Pool, username string) string {
	t.Helper()
	var h string
	if err := pool.QueryRow(context.Background(),
		`SELECT password_hash FROM users WHERE username=$1`, username).Scan(&h); err != nil {
		t.Fatalf("read hash for %q: %v", username, err)
	}
	return h
}

func byoUser(namespace, name, secretName string) *pgmqttv1alpha1.User {
	return &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID("uid-" + namespace + "-" + name)},
		Spec: pgmqttv1alpha1.UserSpec{
			PasswordSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
			},
		},
	}
}

func passwordSecret(namespace, name, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data:       map[string][]byte{"password": []byte(password)},
	}
}
