// Package leader provides Postgres-advisory-lock leader election. Exactly
// one Pod holds pg_advisory_lock(LeaderKey) at any time; that Pod runs the
// janitor and the User reconciler.
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
