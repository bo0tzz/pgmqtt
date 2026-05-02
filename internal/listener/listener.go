// Package listener owns the per-Pod dedicated Postgres connection that:
//
//   - Holds a pg_advisory_lock keyed on the Pod's UUID (its "I am alive"
//     signal — when the connection dies, the lock auto-releases)
//   - Subscribes to LISTEN pgmqtt_<uuid> (publish-NOTIFY fanout)
//   - Subscribes to LISTEN pgmqtt_takeover_<uuid> (close-stale-socket signal)
//
// The listener also drives engine.Deliver and Conn shutdown in response.
package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bo0tzz/pgmqtt/internal/engine"
)

const (
	advisoryLockNamespace = "pgmqtt:broker:"

	// TCP keepalive aggressively configured so a dead/partitioned Pod's
	// advisory lock releases inside ~25 s rather than ~2 h.
	tcpKeepaliveIdle     = 10 // seconds
	tcpKeepaliveInterval = 5
	tcpKeepaliveCount    = 3
)

// Listener owns one dedicated Postgres connection used purely for the Pod's
// LISTEN + advisory-lock identity. Stop it to release the lock and disconnect.
type Listener struct {
	uuid     uuid.UUID
	logger   *slog.Logger
	eng      *engine.Engine
	cancel   context.CancelFunc
	doneCh   chan struct{}
	closeOnce sync.Once
	conn     *pgx.Conn
}

// Start opens a dedicated *pgx.Conn against url, takes the broker advisory
// lock, registers LISTEN, and starts the dispatch goroutine. Returns once the
// lock is held and the LISTEN is registered.
func Start(parentCtx context.Context, url string, eng *engine.Engine, logger *slog.Logger) (*Listener, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.RuntimeParams = mergeParams(cfg.RuntimeParams, map[string]string{
		// Server-side TCP keepalives; takes effect on this backend's outbound socket.
		"tcp_keepalives_idle":     strconv.Itoa(tcpKeepaliveIdle),
		"tcp_keepalives_interval": strconv.Itoa(tcpKeepaliveInterval),
		"tcp_keepalives_count":    strconv.Itoa(tcpKeepaliveCount),
		"application_name":        "pgmqttd-listener",
	})
	// Client-side TCP keepalives on the Go end of the socket as well.
	cfg.DialFunc = keepalivedDialer

	conn, err := pgx.ConnectConfig(parentCtx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	id := uuid.New()
	if err := acquireBrokerLock(parentCtx, conn, id); err != nil {
		_ = conn.Close(parentCtx)
		return nil, err
	}

	if _, err := conn.Exec(parentCtx, `LISTEN `+pubChannel(id)); err != nil {
		_ = conn.Close(parentCtx)
		return nil, fmt.Errorf("listen pub: %w", err)
	}
	if _, err := conn.Exec(parentCtx, `LISTEN `+takeoverChannel(id)); err != nil {
		_ = conn.Close(parentCtx)
		return nil, fmt.Errorf("listen takeover: %w", err)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	l := &Listener{
		uuid:   id,
		logger: logger,
		eng:    eng,
		cancel: cancel,
		doneCh: make(chan struct{}),
		conn:   conn,
	}
	go l.run(ctx)
	return l, nil
}

// BrokerID returns the per-Pod UUID. Pass into engine.SetBrokerID.
func (l *Listener) BrokerID() uuid.UUID { return l.uuid }

// Stop cancels the dispatch goroutine and closes the dedicated connection
// (which releases the advisory lock).
func (l *Listener) Stop() {
	l.closeOnce.Do(func() {
		l.cancel()
		<-l.doneCh
		// Use a fresh context so close still runs after parent cancellation.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = l.conn.Close(ctx)
	})
}

func (l *Listener) run(ctx context.Context) {
	defer close(l.doneCh)
	pubCh := unquotedPub(l.uuid)
	takeoverCh := unquotedTakeover(l.uuid)

	for {
		notif, err := l.conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			l.logger.Warn("listener wait", "err", err)
			return
		}
		switch notif.Channel {
		case pubCh:
			msgID, err := strconv.ParseInt(notif.Payload, 10, 64)
			if err != nil {
				l.logger.Warn("invalid publish payload", "payload", notif.Payload)
				continue
			}
			if err := l.eng.Deliver(ctx, msgID); err != nil {
				l.logger.Warn("deliver", "msg", msgID, "err", err)
			}
		case takeoverCh:
			if c, ok := l.eng.ConnFor(notif.Payload); ok {
				l.logger.Info("takeover from peer", "client", notif.Payload)
				c.Shutdown()
			}
		}
	}
}

// pubChannel / takeoverChannel produce a quoted identifier suitable for the
// LISTEN command (UUIDs contain hyphens and would otherwise be lexed as
// arithmetic). The matching unquotedPub / unquotedTakeover return the raw
// channel name as it appears in pg_notify payloads.

func pubChannel(id uuid.UUID) string      { return `"` + unquotedPub(id) + `"` }
func takeoverChannel(id uuid.UUID) string { return `"` + unquotedTakeover(id) + `"` }
func unquotedPub(id uuid.UUID) string     { return "pgmqtt_" + id.String() }
func unquotedTakeover(id uuid.UUID) string {
	return "pgmqtt_takeover_" + id.String()
}

func acquireBrokerLock(ctx context.Context, conn *pgx.Conn, id uuid.UUID) error {
	// hashtextextended turns a string into a stable bigint suitable for
	// pg_advisory_lock(bigint).
	_, err := conn.Exec(ctx,
		`SELECT pg_advisory_lock(hashtextextended($1, 0))`,
		advisoryLockNamespace+id.String())
	if err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	return nil
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

func keepalivedDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 10 * time.Second}
	return d.DialContext(ctx, network, addr)
}
