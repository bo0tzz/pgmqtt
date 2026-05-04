package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/db"
	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
	"github.com/bo0tzz/pgmqtt/internal/operator"
)

func main() {
	// Parse PGMQTT_LOG_LEVEL via slog.Level.UnmarshalText so any level
	// slog recognises (DEBUG / INFO / WARN / ERROR, case-insensitive)
	// is honoured — previously only "debug" was special-cased and
	// warn/error were silently mapped to info. Unset → info; an
	// unparseable value falls back to info with a warning logged after
	// the handler is up.
	level := slog.LevelInfo
	var levelParseErr error
	if raw := os.Getenv("PGMQTT_LOG_LEVEL"); raw != "" {
		if err := level.UnmarshalText([]byte(raw)); err != nil {
			level = slog.LevelInfo
			levelParseErr = err
		}
	}
	// Pick the handler from PGMQTT_LOG_FORMAT (text|json). Read directly here
	// — config.FromEnv runs after this point, but we want the same handler
	// used for any startup-config errors so log aggregators don't see a
	// stray text line before JSON kicks in. config.FromEnv re-validates
	// the value below and rejects unknown formats.
	handlerOpts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch os.Getenv("PGMQTT_LOG_FORMAT") {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, handlerOpts)
	default:
		handler = slog.NewTextHandler(os.Stderr, handlerOpts)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	if levelParseErr != nil {
		logger.Warn("PGMQTT_LOG_LEVEL parse failed; falling back to info",
			"value", os.Getenv("PGMQTT_LOG_LEVEL"), "err", levelParseErr)
	}

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
			defer recoverPanic(logger, "metrics serve")
			logger.Info("metrics listening", "addr", cfg.MetricsAddr)
			if err := mtx.Serve(ctx, cfg.MetricsAddr); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

	// Janitor runs on every Pod independently. Sweep operations are
	// concurrency-safe by construction (per-row locks, SKIP LOCKED claims,
	// idempotent DELETEs, per-resource try_advisory_lock) — no leader gate
	// needed. See janitor package doc for the full safety analysis.
	jt := janitor.New(pool, eng, logger)
	jt.SetMetrics(mtx)
	go jt.Run(ctx)

	// Operator uses controller-runtime's K8s Lease leader election.
	// Multiple Pods call operator.Run concurrently; exactly one wins
	// reconciliation responsibility at a time. Loss/handoff is handled
	// inside controller-runtime; on loss the manager exits and a peer
	// Pod's manager takes over (no Pod restart needed).
	go func() {
		defer recoverPanic(logger, "operator run")
		opts := operator.Options{
			ServiceHost:             cfg.ServiceHost,
			ServicePort:             cfg.ServicePort,
			WSPort:                  cfg.WSPort,
			BcryptCost:              cfg.BcryptCost,
			LeaderElectionNamespace: cfg.PodNamespace,
		}
		if err := operator.Run(ctx, pool, logger, opts); err != nil {
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

// recoverPanic recovers from a panic in a long-lived background goroutine,
// logs the panic + stack at ERROR level, and returns. Used as `defer
// recoverPanic(logger, "<scope>")` at the top of every goroutine that
// would otherwise take the broker down on an unexpected panic.
func recoverPanic(logger *slog.Logger, scope string) {
	if r := recover(); r != nil {
		logger.Error("goroutine panic", "scope", scope,
			"panic", r, "stack", string(debug.Stack()))
	}
}
