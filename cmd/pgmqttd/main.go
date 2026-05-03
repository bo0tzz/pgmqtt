package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/db"
	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/leader"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
	"github.com/bo0tzz/pgmqtt/internal/operator"
)

func main() {
	level := slog.LevelInfo
	if os.Getenv("PGMQTT_LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	// Pin the K8s Pod name (from POD_NAME env / Downward API) onto every
	// log line so operators reading aggregated logs can correlate which
	// pod produced which line without hunting the random broker UUID.
	if cfg.PodName != "" {
		logger = logger.With("pod", cfg.PodName)
		slog.SetDefault(logger)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := db.Open(ctx, cfg.DatabaseURL, db.Options{
		StatementTimeout: cfg.PGStatementTimeout,
	})
	if err != nil {
		logger.Error("db open", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		logger.Error("migrate", "err", err)
		os.Exit(1)
	}

	eng, err := engine.New(ctx, cfg, pool, logger)
	if err != nil {
		logger.Error("engine new", "err", err)
		os.Exit(1)
	}

	mtx := metrics.New()
	mtx.RegisterPgxPool(pool)
	// Surface controller-runtime's package-global metrics (the User
	// reconciler's reconcile_total / reconcile_time / workqueue_*) on our
	// /metrics endpoint. Controller-runtime's manager metrics server is
	// disabled in operator.Run; we gather its registry alongside ours.
	mtx.AddGatherer(ctrlmetrics.Registry)
	eng.SetMetrics(mtx)
	if cfg.MetricsAddr != "" {
		go func() {
			logger.Info("metrics listening", "addr", cfg.MetricsAddr)
			if err := mtx.Serve(ctx, cfg.MetricsAddr); err != nil && err.Error() != "http: Server closed" {
				logger.Warn("metrics serve", "err", err)
			}
		}()
	}

	lst, err := listener.Start(ctx, cfg.DatabaseURL, eng, logger)
	if err != nil {
		logger.Error("listener", "err", err)
		os.Exit(1)
	}
	defer lst.Stop()
	eng.SetBrokerID(lst.BrokerID())
	// Publish-side pg_notify is emitted inside publishCore's tx for atomic
	// durability; no SetNotifier call is needed for cross-pod fanout.
	// The default no-op localNotifier is correct in production.
	eng.SetTakeoverNotifier(listener.NewTakeoverNotifier(pool))
	eng.SetQuotaNotifier(listener.NewQuotaNotifier(pool))

	ld, err := leader.Start(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("leader", "err", err)
		os.Exit(1)
	}
	defer ld.Stop()

	jt := janitor.New(pool, eng, logger)
	jt.SetMetrics(mtx)
	go jt.RunWith(ctx, ld)

	go func() {
		opts := operator.Options{
			ServiceHost: cfg.ServiceHost,
			ServicePort: cfg.ServicePort,
			WSPort:      cfg.WSPort,
			BcryptCost:  cfg.BcryptCost,
		}
		if err := operator.Run(ctx, ld, pool, logger, opts); err != nil {
			logger.Error("operator", "err", err)
		}
	}()

	logger.Info("pgmqttd starting", "tcp", cfg.TCPAddr, "ws", cfg.WSAddr, "broker", eng.BrokerID())
	if err := eng.Serve(ctx); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}
