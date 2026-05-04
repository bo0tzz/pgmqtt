package operator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
)

// Options configures the operator manager.
type Options struct {
	ServiceHost string
	ServicePort int
	WSPort      int
	BcryptCost  int

	// LeaderElectionNamespace is the namespace where the controller-runtime
	// Lease resource lives. Empty means "auto-detect from the in-cluster
	// config" (the namespace this Pod runs in), which is what production
	// wants. Override only for tests or split-namespace setups.
	LeaderElectionNamespace string
}

// Run starts the controller-runtime manager and the User reconciler. The
// manager uses K8s Lease leader election, so multiple Pods can call Run
// concurrently and exactly one wins reconciliation responsibility at a
// time. Returns when ctx is cancelled or the manager exits.
//
// If a Kubernetes client config can't be loaded (e.g., dev workstation with
// no kubeconfig and no in-cluster config) Run logs and returns nil — the
// broker continues to function without CRD-driven user management.
func Run(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, opts Options) error {
	logger = logger.With("component", "operator")
	cfg, err := config.GetConfig()
	if err != nil {
		logger.Info("kubernetes config not found; operator disabled", "err", err)
		return nil
	}

	// Bail early if the User CRD isn't registered. Avoids the controller-
	// runtime informer's repeated "no matches for kind" log spam in
	// environments where the CRD wasn't installed (e.g., a dev workstation
	// whose kubeconfig points at an unrelated cluster).
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		logger.Warn("operator: discovery client", "err", err)
		return nil
	}
	groupVersion := pgmqttv1alpha1.GroupVersion.String()
	resources, err := dc.ServerResourcesForGroupVersion(groupVersion)
	if err != nil || resources == nil || len(resources.APIResources) == 0 {
		logger.Info("operator: pgmqtt.io/v1alpha1 not registered in cluster; user reconciler disabled",
			"hint", "install the User CRD to enable")
		return nil
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(pgmqttv1alpha1.AddToScheme(scheme))

	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	log.SetLogger(zap.New(zap.UseDevMode(false)))

	mgr, err := ctrl.NewManager(cfg, manager.Options{
		Scheme: scheme,
		// K8s Lease leader election. Exactly one pod's manager runs the
		// reconciler at a time; on leader loss controller-runtime exits
		// the manager (Run returns) and a peer pod's Run takes over.
		LeaderElection:                true,
		LeaderElectionID:              "pgmqtt-operator",
		LeaderElectionNamespace:       opts.LeaderElectionNamespace,
		LeaderElectionReleaseOnCancel: true,
		HealthProbeBindAddress:        ":0",
		Metrics:                       metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	r := &UserReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Pool:        pool,
		Logger:      logger,
		ServiceHost: opts.ServiceHost,
		ServicePort: opts.ServicePort,
		WSPort:      opts.WSPort,
		BcryptCost:  opts.BcryptCost,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	logger.Info("starting operator manager", "leader_election_namespace", opts.LeaderElectionNamespace)
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	return nil
}
