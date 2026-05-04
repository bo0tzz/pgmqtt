// Package leader provides Postgres-advisory-lock leader election. Exactly
// one Pod holds pg_advisory_lock(LeaderKey) at any time; that Pod runs the
// janitor and the User reconciler.
//
// Lost-handling policy: leadership is one-shot — once Lost() fires, this
// Leader is dead and never re-arms. The expectation is that the caller
// (cmd/pgmqttd/main.go) treats unexpected loss as a process-restart event
// (kubelet restarts the pod). A fresh leader.Start in the new process
// re-acquires the lock against whichever pod is the new leader. This
// keeps the package small and avoids the surprise of janitor/operator
// goroutines suddenly re-firing inside a pod that's been demoted.
//
// Fence safety note (audit L1): there is a window — bounded by the 10s
// Ping interval below — between PG releasing the lock (e.g. our session
// dies) and run() noticing. During that window, the new leader is up
// and writing while we still believe we're the leader. The exposure is
// bounded:
//   - findDeadBrokers / handleDeadBroker uses `pg_try_advisory_lock` per
//     dead-broker UUID, which is itself a fence — only one leader can
//     claim a given dead broker.
//   - expireSessions and fireDueWills use `FOR UPDATE` (resp. `SKIP
//     LOCKED`) on the rows they mutate; concurrent leaders serialise.
//     The second leader will see post-first-leader state.
//   - Operator Reconcile writes are idempotent (`INSERT ... ON CONFLICT
//     DO UPDATE`); K8s resourceVersion catches Status() conflicts.
// The remaining sharp edge is a clean rolling-deploy timing window:
// double-firing of will-publishes (mitigated by the publish-then-clear
// ordering in fireDueWills — duplicate-better-than-lost) and double
// session-expiry deletes (idempotent). A strict tx-fenced leader (epoch
// column CAS or routing all leader writes through l.conn) is the future
// fix and is filed as a follow-up. For v1 the bounded exposure plus the
// crash-loop-on-Lost policy is the operating compromise.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// LeaderKey is the bigint passed to pg_advisory_lock. The key is arbitrary
// (any constant works) — 42 per plan.
const LeaderKey int64 = 42

// Leader is the per-Pod handle to leader election.
type Leader struct {
	logger   *slog.Logger
	conn     *pgx.Conn
	cancel   context.CancelFunc
	done     chan struct{}
	acquired chan struct{}
	lost     chan struct{}
	once     sync.Once
	isLeader atomic.Bool
}

// Start opens a dedicated connection and tries to acquire pg_advisory_lock.
// Acquired() reports leadership; Lost() reports demotion (release or conn drop).
//
// The returned Leader runs a background goroutine until ctx cancels or Stop
// is called. If the dedicated connection breaks while we hold the lock, the
// lock auto-releases on the server side; we close Lost() and exit.
func Start(ctx context.Context, url string, logger *slog.Logger) (*Leader, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	cfg.RuntimeParams = mergeParams(cfg.RuntimeParams, map[string]string{
		"application_name": "pgmqttd-leader",
	})
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	l := &Leader{
		logger:   logger,
		conn:     conn,
		cancel:   cancel,
		done:     make(chan struct{}),
		acquired: make(chan struct{}),
		lost:     make(chan struct{}),
	}
	go l.run(subCtx)
	return l, nil
}

// Acquired returns a channel that closes when leadership is acquired.
func (l *Leader) Acquired() <-chan struct{} { return l.acquired }

// Lost returns a channel that closes when leadership is lost.
func (l *Leader) Lost() <-chan struct{} { return l.lost }

// IsLeader reports the current state.
func (l *Leader) IsLeader() bool { return l.isLeader.Load() }

// Stop releases the lock and closes the dedicated connection.
func (l *Leader) Stop() {
	l.once.Do(func() {
		l.cancel()
		<-l.done
	})
}

func (l *Leader) run(ctx context.Context) {
	defer close(l.done)

	if _, err := l.conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, LeaderKey); err != nil {
		l.logger.Warn("leader acquire", "err", err)
		l.shutdown(ctx)
		return
	}
	l.isLeader.Store(true)
	close(l.acquired)
	l.logger.Info("became leader")

	// Hold the lock. We periodically poll Ping to detect a dead connection.
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			l.shutdown(ctx)
			return
		case <-t.C:
			if err := l.conn.Ping(ctx); err != nil {
				l.logger.Warn("leader conn lost", "err", err)
				l.shutdown(ctx)
				return
			}
		}
	}
}

func (l *Leader) shutdown(ctx context.Context) {
	if l.isLeader.Swap(false) {
		// Best-effort release. If conn is dead this is a no-op; either way
		// the server-side lock auto-releases.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = l.conn.Exec(releaseCtx, `SELECT pg_advisory_unlock($1)`, LeaderKey)
		select {
		case <-l.lost:
		default:
			close(l.lost)
		}
	} else {
		// We never became leader — close lost so any waiter unblocks.
		select {
		case <-l.lost:
		default:
			close(l.lost)
		}
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = l.conn.Close(closeCtx)
	if !errors.Is(ctx.Err(), context.Canceled) && ctx.Err() != nil {
		l.logger.Warn("leader exit", "ctx", ctx.Err())
	}
}

func mergeParams(base, extra map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}
