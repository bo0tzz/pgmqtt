package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
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
	logger.Info("pgmqttd", "version", version)

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

	exitCode := 0
	defer exitAfterDeferred(&exitCode)

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
			if err := mtx.Serve(ctx, cfg.MetricsAddr, pool); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Warn("metrics serve", "err", err)
			}
		}()
	}

	lst, err := listener.Start(ctx, cfg.DatabaseURL, eng, logger)
	if err != nil {
		logger.Error("listener", "err", err)
		os.Exit(1)
	}
	lst.SetMetrics(mtx)
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
	jt.SetLockEpoch(lst.LockEpoch)
	if cfg.JanitorInterval > 0 {
		jt.SetInterval(cfg.JanitorInterval)
	}
	go jt.Run(ctx)

	// The operator below hashes passwords at cfg.BcryptCost. The engine's
	// comparison for an unknown username has to cost the same, or usernames
	// can be told apart by response time.
	engine.SetAuthBcryptCost(cfg.BcryptCost)

	// Operator uses controller-runtime's K8s Lease leader election.
	// Multiple Pods call operator.Run concurrently; exactly one reconciles
	// at a time. operator.Run keeps a manager running for as long as ctx
	// lives: when one stops (lost lease, API server unreachable, a panic
	// while starting it) it starts another with backoff, and it waits for
	// the User CRD if that is not installed yet. None of that restarts the
	// Pod, which is also serving MQTT clients.
	//
	// fatalRecoverOperator is for a panic that escapes operator.Run
	// itself. Recovering and carrying on would leave the broker serving
	// with no reconciler and nothing to show for it, so it cancels the
	// root context (engine/janitor/listener drain via their ctx-aware
	// loops) and main exits non-zero once Serve returns, for K8s to
	// restart the Pod.
	go func() {
		defer fatalRecoverOperator(logger, cancel)
		opts := operator.Options{
			ServiceHost:             cfg.ServiceHost,
			ServicePort:             cfg.ServicePort,
			WSPort:                  cfg.WSPort,
			TLSHost:                 cfg.TLSHost,
			TLSPort:                 cfg.TLSPort,
			WSSPort:                 cfg.WSSPort,
			BcryptCost:              cfg.BcryptCost,
			LeaderElectionNamespace: cfg.PodNamespace,
			LeaderElectionID:        cfg.LeaderElectionID,
			WatchNamespaces:         cfg.WatchNamespaces,
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
	// If the operator goroutine panicked, fatalRecoverOperator set this
	// flag and cancelled the root context (which brought Serve out
	// cleanly). Returning runs the deferred lst.Stop and pool.Close; the
	// exit code is applied by exitAfterDeferred, which runs after them.
	if operatorPanicked.Load() {
		logger.Error("exiting after operator panic")
		exitCode = 1
		return
	}
	logger.Info("shutdown complete")
}

// exitAfterDeferred is deferred at the top of main, so it runs after every
// other deferred call there, and exits with *code if it is non-zero. Calling
// os.Exit where the decision is made would skip those deferred calls.
func exitAfterDeferred(code *int) {
	if *code != 0 {
		operatorExit(*code)
	}
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

// operatorPanicked records whether the operator goroutine has bailed
// out via a panic; main() reads it after eng.Serve returns so a
// graceful shutdown still exits with code 1 to trigger a K8s restart.
// atomic.Bool because operator goroutine writes; main reads.
var operatorPanicked atomic.Bool

// operatorExit is the process-exit function called when the operator
// has panicked and the broker has finished draining. Defaults to
// os.Exit so production exits cleanly; tests swap it with a recorder
// to assert the exit was triggered without actually terminating the
// test process.
var operatorExit = os.Exit

// fatalRecoverOperator is the deferred panic recovery used by the
// operator goroutine only. Unlike recoverPanic, it does NOT swallow
// the panic and continue running the broker — a dead controller-
// runtime means no User reconciles land, BYO Secret rotations stall,
// and there's no signal to oncall. Instead it:
//
//  1. Logs the panic + stack.
//  2. Cancels the root context so engine/janitor/listener wind down
//     via their existing ctx-aware loops (graceful drain).
//  3. Sets operatorPanicked so main() knows to exit non-zero after
//     Serve returns.
//
// It does not exit itself: an os.Exit here would skip pool.Close and
// lst.Stop, which only run when main returns (see exitAfterDeferred).
func fatalRecoverOperator(logger *slog.Logger, cancel context.CancelFunc) {
	r := recover()
	if r == nil {
		return
	}
	logger.Error("operator panic; broker exiting for K8s to restart",
		"panic", r, "stack", string(debug.Stack()))
	operatorPanicked.Store(true)
	cancel()
}
