package operator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	pgmqttconfig "github.com/bo0tzz/pgmqtt/internal/config"
)

// inClusterNamespacePath is the canonical service-account namespace file
// that Kubernetes injects into every Pod. Overridable for tests.
var inClusterNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// resolveLeaderElectionNamespace returns the namespace the operator
// should hold its Lease in. Order: explicit opts value, then the
// in-cluster service-account file. Empty return means "neither was
// resolvable; caller should disable the operator".
func resolveLeaderElectionNamespace(explicit string) string {
	if explicit != "" {
		return explicit
	}
	b, err := os.ReadFile(inClusterNamespacePath)
	if err != nil {
		return ""
	}
	return string(bytes.TrimSpace(b))
}

// Options configures the operator manager.
type Options struct {
	// Endpoints written into generated credentials Secrets. A port of 0
	// means the endpoint is not offered and is left out; TLSPort and
	// WSSPort only apply when TLSHost is set.
	ServiceHost string
	ServicePort int
	WSPort      int
	TLSHost     string
	TLSPort     int
	WSSPort     int

	BcryptCost int

	// LeaderElectionNamespace is the namespace where the controller-runtime
	// Lease resource lives. Empty means "auto-detect from the in-cluster
	// config" (the namespace this Pod runs in), which is what production
	// wants. Override only for tests or split-namespace setups.
	LeaderElectionNamespace string

	// LeaderElectionID is the name of the Lease. Empty means
	// config.DefaultLeaderElectionID. Two releases installed in one
	// namespace must not share it, or only one of them ever reconciles.
	LeaderElectionID string

	// WatchNamespaces limits the manager to User objects, and the Secrets
	// next to them, in these namespaces. Empty means every namespace.
	WatchNamespaces []string

	// Leader election timings. Zero keeps controller-runtime's defaults
	// (15s / 10s / 2s); tests that have to wait out a lease shorten them.
	leaseDuration, renewDeadline, retryPeriod time.Duration
}

func (o Options) withDefaults() Options {
	if o.LeaderElectionID == "" {
		o.LeaderElectionID = pgmqttconfig.DefaultLeaderElectionID
	}
	return o
}

// Run keeps a controller-runtime manager with the User reconciler running
// until ctx is cancelled. The manager uses K8s Lease leader election, so
// multiple Pods can call Run concurrently and exactly one reconciles at a
// time.
//
// A manager is single-use and stops for reasons that have nothing to do
// with shutdown — most commonly "leader election lost" after the API
// server was unreachable for longer than the renew deadline. Run starts a
// new one, with backoff, instead of returning: the process is also the MQTT
// broker, so exiting to get a fresh manager would drop every client.
//
// If a Kubernetes client config can't be loaded (e.g., dev workstation with
// no kubeconfig and no in-cluster config), or there is no namespace to hold
// the Lease in, Run logs and returns nil — the broker continues to function
// without CRD-driven user management.
func Run(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, opts Options) error {
	logger = logger.With("component", "operator")
	cfg, err := config.GetConfig()
	if err != nil {
		logger.Info("kubernetes config not found; operator disabled", "err", err)
		return nil
	}

	// Resolved before anything is sent to the API server. On a dev
	// workstation config.GetConfig happily returns whatever cluster the
	// kubeconfig points at; without POD_NAMESPACE the operator must not
	// touch it at all, not even for discovery.
	opts.LeaderElectionNamespace = resolveLeaderElectionNamespace(opts.LeaderElectionNamespace)
	if opts.LeaderElectionNamespace == "" {
		logger.Info("operator disabled: POD_NAMESPACE unset and not in-cluster",
			"hint", "wire POD_NAMESPACE via the Downward API (helm chart's deployment template already does)")
		return nil
	}

	newSupervisor(cfg, pool, logger, opts).run(ctx)
	return nil
}

// newSupervisor wires a supervisor to a real API server.
func newSupervisor(cfg *rest.Config, pool *pgxpool.Pool, logger *slog.Logger, opts Options) *supervisor {
	opts = opts.withDefaults()
	setControllerRuntimeLogger()
	return &supervisor{
		logger: logger,
		crdServed: func(ctx context.Context) (bool, error) {
			return userCRDServed(ctx, cfg)
		},
		runManager: func(ctx context.Context) error {
			return runManager(ctx, cfg, pool, logger, opts)
		},
	}
}

var setLoggerOnce sync.Once

func setControllerRuntimeLogger() {
	setLoggerOnce.Do(func() {
		ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
		log.SetLogger(zap.New(zap.UseDevMode(false)))
	})
}

// userCRDServed asks the API server whether it serves the User kind.
//
// The two "no" cases are kept apart on purpose. A 404 for the group/version
// means the CRD is not installed, which is a supported way to run the broker
// and is reported as (false, nil). Anything else — connection refused, a
// timeout, 401/403, 5xx — says nothing about the CRD and is returned as an
// error, so the caller retries instead of concluding there is nothing to
// reconcile.
func userCRDServed(ctx context.Context, cfg *rest.Config) (bool, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return false, fmt.Errorf("discovery client: %w", err)
	}
	groupVersion := pgmqttv1alpha1.GroupVersion.String()
	resources, err := dc.ServerResourcesForGroupVersionWithContext(ctx, groupVersion)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("discover %s: %w", groupVersion, err)
	}
	for _, res := range resources.APIResources {
		if res.Kind == "User" {
			return true, nil
		}
	}
	return false, nil
}

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(pgmqttv1alpha1.AddToScheme(scheme))
	return scheme
}

// managerOptions translates Options into controller-runtime's.
func managerOptions(opts Options) manager.Options {
	skipNameValidation := true
	mo := manager.Options{
		Scheme: newScheme(),
		// K8s Lease leader election. Exactly one pod's manager runs the
		// reconciler at a time; when it loses the lease its Start returns
		// and a peer takes over. See Run for what happens to this one.
		LeaderElection:                true,
		LeaderElectionID:              opts.LeaderElectionID,
		LeaderElectionNamespace:       opts.LeaderElectionNamespace,
		LeaderElectionReleaseOnCancel: true,
		// Nothing probes the manager (the broker has its own endpoints),
		// and NewManager binds this listener before Start: with a port it
		// would leak one socket per manager that fails between the two.
		HealthProbeBindAddress: "0",
		Metrics:                metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultTransform: trimForCache,
		},
		// Secrets are read from the API server, never from the cache. The
		// cache is unconstrained by namespace unless WatchNamespaces says
		// otherwise, and a cached read would start an informer holding
		// every Secret in the cluster, data included. The reconciler only
		// needs the one or two Secrets a User points at; the watches that
		// tell it when to look are metadata-only (SetupWithManager).
		Client: client.Options{
			Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}},
		},
		// controller-runtime refuses a second controller with a name it has
		// seen in this process, to keep metrics unambiguous. Every manager
		// after the first registers "pgmqtt-user" again; only one exists at
		// a time, so the series stay unambiguous.
		Controller: ctrlconfig.Controller{SkipNameValidation: &skipNameValidation},
	}
	if len(opts.WatchNamespaces) > 0 {
		mo.Cache.DefaultNamespaces = make(map[string]cache.Config, len(opts.WatchNamespaces))
		for _, ns := range opts.WatchNamespaces {
			mo.Cache.DefaultNamespaces[ns] = cache.Config{}
		}
	}
	if opts.leaseDuration > 0 {
		mo.LeaseDuration = &opts.leaseDuration
	}
	if opts.renewDeadline > 0 {
		mo.RenewDeadline = &opts.renewDeadline
	}
	if opts.retryPeriod > 0 {
		mo.RetryPeriod = &opts.retryPeriod
	}
	return mo
}

// trimForCache is applied to every object before it enters the manager's
// cache.
//
// Secrets arrive as metadata only and are cut down further to what the
// watch handlers read: identity, resourceVersion and owner references.
// Labels and annotations go too — kubectl's last-applied-configuration
// annotation is a full copy of the Secret's data. What stays is a few
// hundred bytes per Secret however large the Secret is.
func trimForCache(obj any) (any, error) {
	if m, ok := obj.(*metav1.PartialObjectMetadata); ok {
		m.ObjectMeta = metav1.ObjectMeta{
			Name:              m.Name,
			Namespace:         m.Namespace,
			UID:               m.UID,
			ResourceVersion:   m.ResourceVersion,
			Generation:        m.Generation,
			CreationTimestamp: m.CreationTimestamp,
			DeletionTimestamp: m.DeletionTimestamp,
			OwnerReferences:   m.OwnerReferences,
		}
		return m, nil
	}
	if accessor, err := meta.Accessor(obj); err == nil && accessor.GetManagedFields() != nil {
		accessor.SetManagedFields(nil)
	}
	return obj, nil
}

// runManager builds one manager and runs it until it stops. It returns nil
// only when ctx was cancelled.
func runManager(ctx context.Context, cfg *rest.Config, pool *pgxpool.Pool, logger *slog.Logger, opts Options) error {
	mgr, err := ctrl.NewManager(cfg, managerOptions(opts))
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	r := &UserReconciler{
		Client:      mgr.GetClient(),
		APIReader:   mgr.GetAPIReader(),
		Scheme:      mgr.GetScheme(),
		Pool:        pool,
		Logger:      logger,
		ServiceHost: opts.ServiceHost,
		ServicePort: opts.ServicePort,
		WSPort:      opts.WSPort,
		TLSHost:     opts.TLSHost,
		TLSPort:     opts.TLSPort,
		WSSPort:     opts.WSSPort,
		BcryptCost:  opts.BcryptCost,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	logger.Info("starting operator manager",
		"leader_election_namespace", opts.LeaderElectionNamespace,
		"leader_election_id", opts.LeaderElectionID,
		"watch_namespaces", opts.WatchNamespaces)
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	return nil
}
