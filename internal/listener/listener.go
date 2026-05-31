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
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/bo0tzz/pgmqtt/internal/db"
	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

const (
	advisoryLockNamespace = "pgmqtt:broker:"

	// TCP keepalive aggressively configured so a dead/partitioned Pod's
	// advisory lock releases inside ~25 s rather than ~2 h.
	tcpKeepaliveIdle     = 10 // seconds
	tcpKeepaliveInterval = 5
	tcpKeepaliveCount    = 3

	// Reconnect backoff. On a non-EOF NOTIFY wait error we tear down the
	// dedicated conn (which auto-releases the advisory lock) and try to
	// re-acquire LISTEN + lock. First attempt fires immediately — only a
	// FAILED attempt sleeps, with backoff doubling 1→2→4→8→16 s. The
	// loop gives up after reconnectMaxAttempts and the Pod exits so
	// kubelet replaces it (advisory lock + LISTEN registration are
	// per-conn so the new Pod gets a clean state). Firing the first
	// attempt immediately closes the dead-broker-reap race: during the
	// reconnect window our advisory lock is unheld, so a peer Pod's
	// janitor that grabs it would treat us as a dead broker and kick
	// every owning client.
	reconnectInitialBackoff = 1 * time.Second
	reconnectMaxBackoff     = 16 * time.Second
	reconnectMaxAttempts    = 5
)

// osExit is a package-level indirection over os.Exit so tests can replace
// it without actually killing the test process. Production callers see the
// stdlib behaviour.
var osExit = os.Exit

// Listener owns one dedicated Postgres connection used purely for the Pod's
// LISTEN + advisory-lock identity. Stop it to release the lock and disconnect.
type Listener struct {
	uuid      uuid.UUID
	logger    *slog.Logger
	eng       *engine.Engine
	cancel    context.CancelFunc
	doneCh    chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex // guards conn + url — both swapped from outside the run goroutine.
	conn      *pgx.Conn
	url       string
	mtx       *metrics.Metrics
}

// loadURL returns the current connect URL under l.mu. The URL is
// effectively immutable in production; the lock exists only so
// SetURLForTest can race-safely swap it from a test goroutine.
func (l *Listener) loadURL() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.url
}

// SetMetrics installs a Metrics for listener counters. Call before Start
// returns or shortly after; nil is tolerated by every observation site.
func (l *Listener) SetMetrics(m *metrics.Metrics) { l.mtx = m }

// Start opens a dedicated *pgx.Conn against url, takes the broker advisory
// lock, registers LISTEN, and starts the dispatch goroutine. Returns once the
// lock is held and the LISTEN is registered.
func Start(parentCtx context.Context, url string, eng *engine.Engine, logger *slog.Logger) (*Listener, error) {
	id := uuid.New()
	conn, err := dialAndRegister(parentCtx, url, id)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(parentCtx)
	l := &Listener{
		uuid:   id,
		logger: logger,
		eng:    eng,
		url:    url,
		cancel: cancel,
		doneCh: make(chan struct{}),
		conn:   conn,
	}
	go l.run(ctx)
	return l, nil
}

// dialAndRegister opens a fresh pgx conn, takes the per-broker advisory lock
// (using the supplied id), and registers all three LISTEN channels. Caller
// owns the returned conn's lifetime. Used by Start and by reconnect.
func dialAndRegister(ctx context.Context, url string, id uuid.UUID) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", db.ScrubURLError(err, url))
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

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", db.ScrubURLError(err, url))
	}

	if err := acquireBrokerLock(ctx, conn, id); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}

	if _, err := conn.Exec(ctx, `LISTEN `+pubChannel(id)); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("listen pub: %w", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN `+takeoverChannel(id)); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("listen takeover: %w", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN `+quotaChannel(id)); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("listen quota: %w", err)
	}
	return conn, nil
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
		l.mu.Lock()
		conn := l.conn
		l.mu.Unlock()
		if conn != nil {
			_ = conn.Close(ctx)
		}
	})
}

func (l *Listener) run(ctx context.Context) {
	defer close(l.doneCh)
	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("listener goroutine panic",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	pubCh := unquotedPub(l.uuid)
	takeoverCh := unquotedTakeover(l.uuid)
	quotaCh := unquotedQuota(l.uuid)

	for {
		l.mu.Lock()
		conn := l.conn
		l.mu.Unlock()
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if l.mtx != nil {
					l.mtx.ListenerRestartsTotal.WithLabelValues("ctx_cancel").Inc()
				}
				return
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Non-EOF, non-cancellation wait error. Tear down the conn
			// (releases the advisory lock as a side-effect) and try to
			// reconnect with exponential backoff. If reconnect succeeds
			// we resume the loop on the new conn; if all attempts fail
			// we exit the process so the kubelet replaces this Pod.
			l.logger.Warn("listener wait; reconnecting", "err", err)
			if l.mtx != nil {
				l.mtx.ListenerRestartsTotal.WithLabelValues("wait_error").Inc()
			}
			if !l.reconnect(ctx) {
				// Distinguish "ctx cancelled mid-reconnect" (clean Stop())
				// from "we burned through reconnectMaxAttempts". The
				// former must return cleanly so graceful shutdown doesn't
				// crash the pod; the latter is the kubelet-replacement
				// path that signals failure via osExit.
				if ctx.Err() != nil {
					if l.mtx != nil {
						l.mtx.ListenerRestartsTotal.WithLabelValues("ctx_cancel").Inc()
					}
					return
				}
				if l.mtx != nil {
					l.mtx.ListenerRestartsTotal.WithLabelValues("exhausted_retries").Inc()
				}
				l.logger.Error("listener reconnect exhausted; exiting for kubelet replacement",
					"broker", l.uuid)
				osExit(1)
				return
			}
			continue
		}
		l.dispatchNotification(ctx, notif, pubCh, takeoverCh, quotaCh)
	}
}

// reconnect tears down the current conn and tries to bring up a fresh one
// (re-acquiring the broker advisory lock + LISTEN registrations). Returns
// true if a new conn is in place. The per-broker UUID is preserved across
// reconnect — the previous conn's death released the advisory lock so the
// new conn's pg_advisory_lock attempt either succeeds (clean reacquire) or
// blocks until any peer racing a takeover finishes.
func (l *Listener) reconnect(ctx context.Context) bool {
	// Tear down the dead conn so the advisory lock + listen registrations
	// are released cleanly.
	l.mu.Lock()
	old := l.conn
	l.conn = nil
	l.mu.Unlock()
	if old != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = old.Close(closeCtx)
		cancel()
	}

	// Reconnect strategy: fire the first attempt IMMEDIATELY, only sleep
	// after a FAILED attempt. The old code's leading 1 s sleep gave peer
	// Pods' janitor.findDeadBrokerCandidates + handleDeadBroker enough
	// time to grab our (now-unheld) per-broker advisory lock and reap
	// every session whose broker_id = us — the broker is alive, just
	// reconnecting, but every owning client gets kicked. Trying once
	// fast closes that window for the common CNPG primary-failover case
	// where Postgres comes back in <100ms.
	backoff := reconnectInitialBackoff
	for attempt := 1; attempt <= reconnectMaxAttempts; attempt++ {
		// Bail before the dial on ctx cancellation so Stop() during
		// reconnect returns promptly.
		if ctx.Err() != nil {
			return false
		}
		newConn, err := dialAndRegister(ctx, l.loadURL(), l.uuid)
		if err == nil {
			l.mu.Lock()
			l.conn = newConn
			l.mu.Unlock()
			l.logger.Info("listener reconnected", "broker", l.uuid, "attempt", attempt)
			return true
		}
		l.logger.Warn("listener reconnect failed", "attempt", attempt, "err", err)
		// Last attempt — don't sleep, just bail.
		if attempt == reconnectMaxAttempts {
			break
		}
		// Back off before the next attempt (and respect ctx).
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff < reconnectMaxBackoff {
			backoff *= 2
			if backoff > reconnectMaxBackoff {
				backoff = reconnectMaxBackoff
			}
		}
	}
	return false
}

// dispatchNotification handles a single NOTIFY. Wrapped in its own recover so
// a panic in Deliver / Shutdown / QuotaExceededLocally on one notification
// can't kill the listener — the broker stops fan-out for that pod entirely
// when the listener dies, so per-event isolation matters.
func (l *Listener) dispatchNotification(ctx context.Context, notif *pgconn.Notification, pubCh, takeoverCh, quotaCh string) {
	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("listener dispatch panic",
				"channel", notif.Channel, "payload", notif.Payload,
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	switch notif.Channel {
	case pubCh:
		msgID, err := strconv.ParseInt(notif.Payload, 10, 64)
		if err != nil {
			l.logger.Warn("invalid publish payload", "payload", notif.Payload)
			return
		}
		l.logger.Debug("listener notify", "broker", l.uuid, "msg", msgID)
		if err := l.eng.Deliver(ctx, msgID); err != nil {
			l.logger.Warn("deliver", "msg", msgID, "err", err)
		}
	case takeoverCh:
		// Payload format (current): <36-char prevToken><clientID>.
		// Forward-compat: a payload that doesn't parse as
		// "uuid-then-anything" is treated as a bare clientID — older
		// peers that haven't shipped the token-aware notify yet still
		// trigger a shutdown of any matching local Conn.
		prevToken, clientID, parsed := parseTakeoverPayload(notif.Payload)
		c, ok := l.eng.ConnFor(clientID)
		if !ok {
			return
		}
		if parsed && c.SessionToken() != prevToken {
			// Stale notification — the matching Conn over here was
			// taken over again since this NOTIFY was emitted, so
			// shutting it down would kill a healthy session.
			l.logger.Debug("stale takeover ignored",
				"client", clientID,
				"payload_token", prevToken,
				"current_token", c.SessionToken())
			return
		}
		l.logger.Info("takeover from peer", "client", clientID)
		// ShutdownForTakeover marks the Conn so handleDisconnect
		// suppresses will firing — a takeover-driven shutdown is not
		// the same as the client dying (spec MQTT-3.1.2.5).
		c.ShutdownForTakeover()
	case quotaCh:
		// Payload format mirrors takeover: <36-char token><clientID>.
		// Forward-compat: a payload that doesn't parse as
		// "uuid-then-anything" is treated as a bare clientID — older
		// peers that emitted plain client_id still trigger a kick of
		// any matching local Conn.
		prevToken, clientID, parsed := parseTakeoverPayload(notif.Payload)
		if parsed {
			l.eng.QuotaExceededLocallyForToken(clientID, prevToken)
		} else {
			l.eng.QuotaExceededLocally(clientID)
		}
	}
}

// parseTakeoverPayload splits a takeover NOTIFY payload into the token of
// the Conn-being-superseded and the clientID. New format: <36-char UUID>
// followed immediately by the clientID. The boolean return is false for
// any payload that doesn't begin with a 36-char hyphenated UUID — those
// are treated as bare clientIDs by the caller for forward-compat with
// older peers that haven't shipped the token-aware notify yet.
func parseTakeoverPayload(payload string) (prev uuid.UUID, clientID string, ok bool) {
	if len(payload) < 36 {
		return uuid.Nil, payload, false
	}
	t, err := uuid.Parse(payload[:36])
	if err != nil {
		return uuid.Nil, payload, false
	}
	return t, payload[36:], true
}

// pubChannel / takeoverChannel produce a quoted identifier suitable for the
// LISTEN command (UUIDs contain hyphens and would otherwise be lexed as
// arithmetic). The matching unquotedPub / unquotedTakeover return the raw
// channel name as it appears in pg_notify payloads.

func pubChannel(id uuid.UUID) string      { return `"` + unquotedPub(id) + `"` }
func takeoverChannel(id uuid.UUID) string { return `"` + unquotedTakeover(id) + `"` }
func quotaChannel(id uuid.UUID) string    { return `"` + unquotedQuota(id) + `"` }
func unquotedPub(id uuid.UUID) string     { return "pgmqtt_" + id.String() }
func unquotedTakeover(id uuid.UUID) string {
	return "pgmqtt_takeover_" + id.String()
}
func unquotedQuota(id uuid.UUID) string {
	return "pgmqtt_quota_" + id.String()
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
