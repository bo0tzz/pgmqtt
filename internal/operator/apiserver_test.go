package operator

// Tests in this file run the operator against a real kube-apiserver and
// etcd started by controller-runtime's envtest. They cover what the fake
// client cannot: leader election, informers and what they ask the API
// server for, discovery, and the CRD's schema.
//
// They need the control-plane binaries and are skipped without them:
//
//	KUBEBUILDER_ASSETS=$(setup-envtest use -p path) go test ./internal/operator/ -run APIServer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

const crdDir = "../../config/crd/bases"

// apiRequest is one request the operator sent to the API server.
type apiRequest struct {
	method, path, query, accept string
}

type requestLog struct {
	mu   sync.Mutex
	reqs []apiRequest
}

func (l *requestLog) wrap(rt http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		l.mu.Lock()
		l.reqs = append(l.reqs, apiRequest{req.Method, req.URL.Path, req.URL.RawQuery, req.Header.Get("Accept")})
		l.mu.Unlock()
		return rt.RoundTrip(req)
	})
}

func (l *requestLog) all() []apiRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]apiRequest(nil), l.reqs...)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// apiServer is a throwaway control plane plus a client for the test's own
// use.
type apiServer struct {
	cfg *rest.Config
	cli client.Client
}

func startAPIServer(t *testing.T) *apiServer {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; this test needs kube-apiserver and etcd (setup-envtest use -p path)")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start control plane: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	cli, err := client.New(cfg, client.Options{Scheme: newScheme()})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return &apiServer{cfg: cfg, cli: cli}
}

func (a *apiServer) installCRD(t *testing.T) {
	t.Helper()
	if _, err := envtest.InstallCRDs(a.cfg, envtest.CRDInstallOptions{
		Paths:              []string{crdDir},
		ErrorIfPathMissing: true,
	}); err != nil {
		t.Fatalf("install CRD: %v", err)
	}
}

func (a *apiServer) namespace(t *testing.T, name string) {
	t.Helper()
	err := a.cli.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

// runningOperator is a supervisor started against the API server.
type runningOperator struct {
	logs *syncBuffer
	reqs *requestLog
	pool *pgxpool.Pool
	stop func()
}

func (a *apiServer) startOperator(t *testing.T, opts Options) *runningOperator {
	t.Helper()
	op := &runningOperator{logs: &syncBuffer{}, reqs: &requestLog{}, pool: dbtest.FreshPool(t)}

	cfg := rest.CopyConfig(a.cfg)
	cfg.Wrap(op.reqs.wrap)
	opts.BcryptCost = bcrypt.MinCost
	if opts.LeaderElectionNamespace == "" {
		opts.LeaderElectionNamespace = "default"
	}
	s := newSupervisor(cfg, op.pool, slog.New(slog.NewTextHandler(op.logs, nil)), opts)
	s.crdPollInterval = 100 * time.Millisecond
	s.minBackoff = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(ctx)
	}()
	var once sync.Once
	op.stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(60 * time.Second):
				t.Error("operator did not stop within 60s of its context being cancelled")
			}
		})
	}
	t.Cleanup(op.stop)
	return op
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		if last = cond(); last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s: %v", timeout, what, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// userReconciled reports whether the User is Ready and the database holds
// a row for it that verifies the password in its Secret.
func (a *apiServer) userReconciled(pool *pgxpool.Pool, namespace, name, secretName string) error {
	ctx := context.Background()
	var u pgmqttv1alpha1.User
	if err := a.cli.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &u); err != nil {
		return err
	}
	ready := false
	for _, c := range u.Status.Conditions {
		if c.Type == "Ready" {
			if c.Status != metav1.ConditionTrue {
				return fmt.Errorf("Ready=%s reason=%s message=%q", c.Status, c.Reason, c.Message)
			}
			ready = true
		}
	}
	if !ready {
		return fmt.Errorf("no Ready condition yet")
	}
	var sec corev1.Secret
	if err := a.cli.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &sec); err != nil {
		return err
	}
	var hash string
	if err := pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE username=$1`, usernameFor(&u)).Scan(&hash); err != nil {
		return fmt.Errorf("users row: %w", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), sec.Data["password"]); err != nil {
		return fmt.Errorf("row does not verify the Secret's password: %w", err)
	}
	return nil
}

func (a *apiServer) createUser(t *testing.T, namespace, name string, spec pgmqttv1alpha1.UserSpec) {
	t.Helper()
	u := &pgmqttv1alpha1.User{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: spec}
	if err := a.cli.Create(context.Background(), u); err != nil {
		t.Fatalf("create User %s/%s: %v", namespace, name, err)
	}
}

// TestAPIServer_CRDInstalledAfterStart starts the operator against a
// cluster without the CRD, installs it, and expects Users to be reconciled
// without a restart.
func TestAPIServer_CRDInstalledAfterStart(t *testing.T) {
	api := startAPIServer(t)
	api.namespace(t, "late")
	op := api.startOperator(t, Options{LeaderElectionID: "late-crd", WatchNamespaces: []string{"late"}})

	eventually(t, 10*time.Second, "the operator to report the missing CRD", func() error {
		if !strings.Contains(op.logs.String(), "is not installed") {
			return fmt.Errorf("not logged yet")
		}
		return nil
	})
	if strings.Contains(op.logs.String(), "starting operator manager") {
		t.Fatal("a manager was started without the CRD")
	}

	api.installCRD(t)
	api.createUser(t, "late", "ha", pgmqttv1alpha1.UserSpec{})
	eventually(t, 60*time.Second, "the User to be reconciled once the CRD exists", func() error {
		return api.userReconciled(op.pool, "late", "ha", "ha-mqtt-credentials")
	})

	// The fields the reconciler relies on must survive the API server's
	// pruning against the installed schema.
	var u pgmqttv1alpha1.User
	if err := api.cli.Get(context.Background(), types.NamespacedName{Namespace: "late", Name: "ha"}, &u); err != nil {
		t.Fatalf("get user: %v", err)
	}
	if u.Status.ObservedSecretVersion == "" || u.Status.ObservedUsername != "ha" {
		t.Errorf("status fields pruned by the CRD schema: %+v", u.Status)
	}
}

// TestAPIServer_ManagerRestartsAfterLosingLease takes the Lease away from
// a leading manager, which is what an API server outage longer than the
// renew deadline does. The manager's Start returns "leader election lost";
// the same process must lead and reconcile again afterwards.
func TestAPIServer_ManagerRestartsAfterLosingLease(t *testing.T) {
	api := startAPIServer(t)
	api.installCRD(t)
	api.namespace(t, "lease")
	op := api.startOperator(t, Options{
		LeaderElectionID: "lease-test",
		WatchNamespaces:  []string{"lease"},
		leaseDuration:    4 * time.Second,
		renewDeadline:    2 * time.Second,
		retryPeriod:      500 * time.Millisecond,
	})

	api.createUser(t, "lease", "before", pgmqttv1alpha1.UserSpec{})
	eventually(t, 60*time.Second, "the first User to be reconciled", func() error {
		return api.userReconciled(op.pool, "lease", "before", "before-mqtt-credentials")
	})
	restartsBefore := metricValue(t, managerRestartsTotal)

	// Hand the Lease to a holder that never renews it.
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: "lease-test"}
	eventually(t, 10*time.Second, "the Lease to be taken over", func() error {
		var lease coordinationv1.Lease
		if err := api.cli.Get(ctx, key, &lease); err != nil {
			return err
		}
		intruder := "someone-else"
		now := metav1.NewMicroTime(time.Now())
		lease.Spec.HolderIdentity = &intruder
		lease.Spec.RenewTime = &now
		return api.cli.Update(ctx, &lease)
	})

	eventually(t, 30*time.Second, "the manager to stop on losing the lease", func() error {
		if !strings.Contains(op.logs.String(), "leader election lost") {
			return fmt.Errorf("not logged yet")
		}
		return nil
	})
	eventually(t, 10*time.Second, "the restart to be counted", func() error {
		if got := metricValue(t, managerRestartsTotal) - restartsBefore; got < 1 {
			return fmt.Errorf("restart counter moved by %v", got)
		}
		return nil
	})

	// Created while this process is not the leader. It can only be
	// reconciled by the manager started after the first one stopped.
	api.createUser(t, "lease", "after", pgmqttv1alpha1.UserSpec{})
	eventually(t, 90*time.Second, "a User created after the restart to be reconciled", func() error {
		return api.userReconciled(op.pool, "lease", "after", "after-mqtt-credentials")
	})
	if n := strings.Count(op.logs.String(), "starting operator manager"); n < 2 {
		t.Errorf("manager started %d times, want at least 2", n)
	}
}

// TestAPIServer_SecretsAreNotCached checks what the operator asks the API
// server for. Listing or watching Secrets in full would put every Secret
// in the watched scope into the process's memory.
func TestAPIServer_SecretsAreNotCached(t *testing.T) {
	api := startAPIServer(t)
	api.installCRD(t)
	api.namespace(t, "sec")
	ctx := context.Background()

	for _, s := range []*corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "sec", Name: "unrelated-helm-release"},
			Data: map[string][]byte{"release": []byte(strings.Repeat("x", 64<<10))}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "sec", Name: "byo-creds"},
			Data: map[string][]byte{"password": []byte("first")}},
	} {
		if err := api.cli.Create(ctx, s); err != nil {
			t.Fatalf("create secret %s: %v", s.Name, err)
		}
	}
	api.createUser(t, "sec", "generated", pgmqttv1alpha1.UserSpec{})
	api.createUser(t, "sec", "byo", pgmqttv1alpha1.UserSpec{
		PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "byo-creds"}},
	})

	// No namespace restriction: the default, and the worst case for memory.
	op := api.startOperator(t, Options{LeaderElectionID: "secrets-test"})
	eventually(t, 60*time.Second, "both Users to be reconciled", func() error {
		if err := api.userReconciled(op.pool, "sec", "generated", "generated-mqtt-credentials"); err != nil {
			return err
		}
		return api.userReconciled(op.pool, "sec", "byo", "byo-creds")
	})

	// Rotation of a bring-your-own Secret is only noticed through the
	// Secret watch, so this also shows the metadata-only watch delivers.
	var byo corev1.Secret
	if err := api.cli.Get(ctx, types.NamespacedName{Namespace: "sec", Name: "byo-creds"}, &byo); err != nil {
		t.Fatalf("get byo secret: %v", err)
	}
	byo.Data["password"] = []byte("second")
	if err := api.cli.Update(ctx, &byo); err != nil {
		t.Fatalf("rotate byo secret: %v", err)
	}
	eventually(t, 30*time.Second, "the rotated password to reach the database", func() error {
		var hash string
		if err := op.pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE username='byo'`).Scan(&hash); err != nil {
			return err
		}
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte("second"))
	})
	op.stop()

	sawSecretWatch := false
	for _, r := range op.reqs.all() {
		if !strings.Contains(r.path, "/secrets") {
			continue
		}
		collection := strings.HasSuffix(r.path, "/secrets")
		switch {
		case collection && r.method == http.MethodGet:
			// A list or watch: what an informer issues to fill its cache.
			sawSecretWatch = true
			if !strings.Contains(r.accept, "PartialObjectMetadata") {
				t.Errorf("Secrets listed or watched in full: GET %s?%s (Accept: %s)", r.path, r.query, r.accept)
			}
		case strings.HasSuffix(r.path, "/secrets/unrelated-helm-release"):
			t.Errorf("read a Secret no User refers to: %s %s", r.method, r.path)
		}
	}
	if !sawSecretWatch {
		t.Error("no Secret list/watch was recorded; the assertion above checked nothing")
	}
}

// TestAPIServer_TwoReleasesSideBySide runs two operators the way two Helm
// releases would: their own Lease ID, their own namespace to watch, their
// own database. Each must reconcile its own Users and leave the other's
// alone.
func TestAPIServer_TwoReleasesSideBySide(t *testing.T) {
	api := startAPIServer(t)
	api.installCRD(t)
	api.namespace(t, "rel-a")
	api.namespace(t, "rel-b")
	api.namespace(t, "unwatched")

	a := api.startOperator(t, Options{LeaderElectionID: "release-a", WatchNamespaces: []string{"rel-a"}})
	b := api.startOperator(t, Options{LeaderElectionID: "release-b", WatchNamespaces: []string{"rel-b"}})

	api.createUser(t, "rel-a", "app", pgmqttv1alpha1.UserSpec{Username: "app-a"})
	api.createUser(t, "rel-b", "app", pgmqttv1alpha1.UserSpec{Username: "app-b"})
	api.createUser(t, "unwatched", "app", pgmqttv1alpha1.UserSpec{Username: "app-nobody"})

	eventually(t, 60*time.Second, "release A's User to be reconciled", func() error {
		return api.userReconciled(a.pool, "rel-a", "app", "app-mqtt-credentials")
	})
	eventually(t, 60*time.Second, "release B's User to be reconciled", func() error {
		return api.userReconciled(b.pool, "rel-b", "app", "app-mqtt-credentials")
	})

	ctx := context.Background()
	for name, tc := range map[string]struct {
		pool    *pgxpool.Pool
		foreign []string
	}{
		"release A": {a.pool, []string{"app-b", "app-nobody"}},
		"release B": {b.pool, []string{"app-a", "app-nobody"}},
	} {
		var n int
		if err := tc.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE username = ANY($1)`, tc.foreign).Scan(&n); err != nil {
			t.Fatalf("%s: count: %v", name, err)
		}
		if n != 0 {
			t.Errorf("%s reconciled %d User(s) outside its namespace", name, n)
		}
	}

	var outside pgmqttv1alpha1.User
	if err := api.cli.Get(ctx, types.NamespacedName{Namespace: "unwatched", Name: "app"}, &outside); err != nil {
		t.Fatalf("get unwatched user: %v", err)
	}
	if len(outside.Finalizers) != 0 || len(outside.Status.Conditions) != 0 {
		t.Errorf("a User outside every watched namespace was touched: finalizers=%v status=%+v",
			outside.Finalizers, outside.Status)
	}

	a.stop()
	b.stop()
	for name, op := range map[string]*runningOperator{"release A": a, "release B": b} {
		own := map[string]string{"release A": "/namespaces/rel-a/", "release B": "/namespaces/rel-b/"}[name]
		for _, r := range op.reqs.all() {
			isUsers := strings.Contains(r.path, "/pgmqtt.io/v1alpha1/") && strings.Contains(r.path, "users")
			isSecrets := strings.Contains(r.path, "/secrets")
			if (isUsers || isSecrets) && !strings.Contains(r.path, own) {
				t.Errorf("%s reached outside its namespace: %s %s", name, r.method, r.path)
			}
		}
	}
}
