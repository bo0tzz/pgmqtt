// Package engine is the per-Pod MQTT broker. It owns currently-connected
// sockets, runs the per-connection state machine, and handles publisher and
// receiver SQL paths against Postgres. All cross-Pod coordination flows through
// Postgres; engine itself has no peer-to-peer state.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// Notifier is a post-commit hook for additional, optional delivery
// mechanisms. Production fires the cross-Pod `pg_notify` *inside* the
// publishCore tx (atomic with message durability) so this hook is a no-op
// in production. Single-Pod tests wire `NewInProcessNotifier` to short-
// circuit the LISTEN→Deliver round-trip with an in-process Deliver call.
type Notifier interface {
	// Notify is called after a successful publish commit. brokerIDs are
	// the broker UUIDs owning currently-connected subscribers; messageID
	// is the messages.id row id.
	Notify(ctx context.Context, brokerIDs []uuid.UUID, messageID int64) error
}

// TakeoverNotifier emits a takeover signal so the prior owner of a
// client_id can close its now-stale socket. The default is a no-op
// (single-Pod tests); production wires `listener.NewTakeoverNotifier`,
// which fires `pg_notify` on `pgmqtt_takeover_<broker_id>`.
type TakeoverNotifier interface {
	NotifyTakeover(ctx context.Context, brokerID uuid.UUID, clientID string) error
}

// QuotaNotifier emits a "quota exceeded" signal for a client_id whose
// pending-deliveries depth is at the configured cap. The signal targets a
// specific broker UUID — the Pod that currently owns the slow client. The
// receiving Pod writes DISCONNECT 0x97 to that client and tears down the
// socket.
type QuotaNotifier interface {
	NotifyQuota(ctx context.Context, brokerID uuid.UUID, clientID string) error
}

// Engine is the per-Pod broker.
type Engine struct {
	cfg    *config.Config
	pool   *pgxpool.Pool
	logger *slog.Logger

	brokerIDMu sync.RWMutex
	brokerID   uuid.UUID

	notify   Notifier
	takeover TakeoverNotifier
	quota    QuotaNotifier

	metrics *metrics.Metrics

	// iplimit meters CONNECT rate + auth-failures per source IP to
	// mitigate bcrypt-CPU DoS. Always non-nil; the limiter internally
	// short-circuits when both knobs are 0. Lifecycle is bound to the
	// engine's shutdown via iplimitCancel.
	iplimit       *ipLimiter
	iplimitCancel context.CancelFunc

	connsMu     sync.RWMutex
	conns       map[string]*Conn // client_id -> *Conn
	openConns   atomic.Int64     // accepted-but-not-yet-closed sockets, regardless of CONNECT state

	// Runtime-tunable knobs. Stored as atomics so test setters don't race
	// with the accept loop / dispatch path. Populated from cfg in New().
	maxConnsAtomic       atomic.Int64
	maxQueuedAtomic      atomic.Int64
	maxInboundRateAtomic atomic.Int64
	maxPacketSizeAtomic  atomic.Int64
	receiveMaxV5Atomic   atomic.Int64
	keepaliveMaxV5Atomic atomic.Int64 // nanoseconds

	listenersMu  sync.RWMutex
	tcpListener  net.Listener
	wsListener   net.Listener
	wsServer     *http.Server
	wg           sync.WaitGroup
	shutdownOnce sync.Once
	shutdown     chan struct{}

	// Tunables — exposed for tests.
	KeepAliveGrace time.Duration // multiplier applied to keepalive when arming read deadlines (default 1.5)
}

// New constructs an Engine. SetBrokerID must be called before Serve so
// publishes know who they are.
func New(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, logger *slog.Logger) (*Engine, error) {
	e := &Engine{
		cfg:            cfg,
		pool:           pool,
		logger:         logger,
		conns:          make(map[string]*Conn),
		shutdown:       make(chan struct{}),
		KeepAliveGrace: 1500 * time.Millisecond,
		notify:         &localNotifier{},
		takeover:       noopTakeover{},
		quota:          noopQuota{},
	}
	connectsPerSec := 0
	authFailuresPerMin := 0
	if cfg != nil {
		e.maxConnsAtomic.Store(int64(cfg.MaxConnections))
		e.maxQueuedAtomic.Store(int64(cfg.MaxQueuedDeliveriesPerClient))
		e.maxInboundRateAtomic.Store(int64(cfg.MaxInboundMsgsPerSec))
		e.maxPacketSizeAtomic.Store(int64(cfg.MaxPacketSize))
		e.receiveMaxV5Atomic.Store(int64(cfg.V5ReceiveMaximum))
		e.keepaliveMaxV5Atomic.Store(int64(cfg.V5KeepaliveMax))
		connectsPerSec = cfg.MaxConnectsPerIPPerSec
		authFailuresPerMin = cfg.MaxAuthFailuresPerIPPerMin
	}
	// Bind the limiter's GC goroutine to a child of ctx that we cancel
	// at shutdown (or when the parent ctx ends). Using a separate cancel
	// rather than ctx itself means callers that pass context.Background()
	// (single-use tests) still get GC stopped when the engine drains.
	limCtx, limCancel := context.WithCancel(ctx)
	e.iplimitCancel = limCancel
	e.iplimit = newIPLimiter(limCtx, connectsPerSec, authFailuresPerMin)
	return e, nil
}

// SetBrokerID is called by the listener after the Pod's UUID is assigned.
func (e *Engine) SetBrokerID(id uuid.UUID) {
	e.brokerIDMu.Lock()
	e.brokerID = id
	e.brokerIDMu.Unlock()
}

// BrokerID returns the current Pod UUID.
func (e *Engine) BrokerID() uuid.UUID {
	e.brokerIDMu.RLock()
	defer e.brokerIDMu.RUnlock()
	return e.brokerID
}

// SetNotifier swaps the publish notifier. Call before Serve.
func (e *Engine) SetNotifier(n Notifier) {
	e.notify = n
}

// SetTakeoverNotifier swaps the takeover notifier. Call before Serve.
func (e *Engine) SetTakeoverNotifier(t TakeoverNotifier) {
	e.takeover = t
}

// SetQuotaNotifier swaps the quota-exceeded notifier. Call before Serve.
func (e *Engine) SetQuotaNotifier(q QuotaNotifier) {
	e.quota = q
}

// SetMetrics installs a Metrics instance. Engine increments
// counters/gauges on it as events occur. Calls are no-op when nil.
func (e *Engine) SetMetrics(m *metrics.Metrics) {
	e.metrics = m
}

// Serve runs the accept loops until ctx is cancelled or a fatal accept error.
// It blocks until all in-flight connections finish.
func (e *Engine) Serve(ctx context.Context) error {
	if e.cfg.TCPAddr != "" {
		ln, err := net.Listen("tcp", e.cfg.TCPAddr)
		if err != nil {
			return fmt.Errorf("listen tcp %s: %w", e.cfg.TCPAddr, err)
		}
		e.listenersMu.Lock()
		e.tcpListener = ln
		e.listenersMu.Unlock()
		e.logger.Info("tcp listening", "addr", ln.Addr())
		e.wg.Add(1)
		go e.acceptTCP(ctx, ln)
	}
	if e.cfg.WSAddr != "" {
		ln, err := net.Listen("tcp", e.cfg.WSAddr)
		if err != nil {
			return fmt.Errorf("listen ws %s: %w", e.cfg.WSAddr, err)
		}
		e.listenersMu.Lock()
		e.wsListener = ln
		e.listenersMu.Unlock()
		e.logger.Info("ws listening", "addr", ln.Addr())
		e.wg.Add(1)
		go e.serveWS(ctx, ln)
	}

	e.wg.Add(1)
	go e.runOwnershipSweep(ctx)

	// Block on context, then drain.
	<-ctx.Done()
	e.shutdownGracefully()
	e.wg.Wait()
	return nil
}

// runOwnershipSweep periodically reconciles the local conns map against
// `sessions.broker_id` to close any sockets we still hold for client_ids
// that the database now attributes to a different broker.
//
// Why: the takeover NOTIFY (`pgmqtt_takeover_<broker_id>`) is fire-and-
// forget and only delivers to currently-LISTENing peers. A new CONNECT
// for a client_id can flip `sessions.broker_id` on this pod's row while
// this pod is briefly partitioned from Postgres-NOTIFY (or its listener
// goroutine has gone, e.g. mid-recovery from the listener's connection
// being torn down). Until the prior socket's TCP keepalive trips
// (~25 s) we'd otherwise still accept PUBLISHes from the orphaned
// socket, which would then fan out and surface as duplicate-from-the-
// new-session traffic.
//
// The sweep is best-effort: snapshot the local conns map, ask PG who
// owns each client_id, and `Shutdown()` any whose `broker_id` is no
// longer us. Idempotent against in-flight CONNECTs (registerConn is
// guarded by connsMu so we won't see partial state). Cheap — a single
// `WHERE client_id = ANY($1)` query every 5s.
func (e *Engine) runOwnershipSweep(ctx context.Context) {
	defer e.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("ownership sweep panic",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.shutdown:
			return
		case <-t.C:
			if err := e.sweepOrphanedSockets(ctx); err != nil {
				e.logger.Warn("ownership sweep", "err", err)
			}
			e.updateCapacityGauge()
		}
	}
}

// updateCapacityGauge refreshes pgmqtt_connections_capacity_ratio. Cheap
// enough to call from the ownership-sweep tick.
func (e *Engine) updateCapacityGauge() {
	if e.metrics == nil {
		return
	}
	maxConns := e.maxConnsAtomic.Load()
	if maxConns <= 0 {
		// Cap is disabled (0 = unlimited). Report 0 so dashboards
		// rendering this gauge don't divide by zero.
		e.metrics.ConnectionsCapacityRatio.Set(0)
		return
	}
	current := e.openConns.Load()
	e.metrics.ConnectionsCapacityRatio.Set(float64(current) / float64(maxConns))
}

func (e *Engine) sweepOrphanedSockets(ctx context.Context) error {
	self := e.BrokerID()
	if self == uuid.Nil {
		// Listener hasn't assigned us a broker UUID yet; can't compare
		// ownership. Skip — once SetBrokerID lands the next tick handles it.
		return nil
	}
	e.connsMu.RLock()
	clientIDs := make([]string, 0, len(e.conns))
	for id := range e.conns {
		clientIDs = append(clientIDs, id)
	}
	e.connsMu.RUnlock()
	if len(clientIDs) == 0 {
		return nil
	}
	rows, err := e.pool.Query(ctx, `
		SELECT client_id, broker_id
		  FROM sessions
		 WHERE client_id = ANY($1)
		   AND broker_id IS NOT NULL
		   AND broker_id != $2
	`, clientIDs, self)
	if err != nil {
		return err
	}
	defer rows.Close()
	type orphan struct {
		clientID string
		owner    uuid.UUID
	}
	var orphans []orphan
	for rows.Next() {
		var cid string
		var bid uuid.UUID
		if err := rows.Scan(&cid, &bid); err != nil {
			return err
		}
		orphans = append(orphans, orphan{clientID: cid, owner: bid})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, o := range orphans {
		c, ok := e.ConnFor(o.clientID)
		if !ok {
			continue
		}
		e.logger.Info("orphaned socket: closing (sessions.broker_id != self)",
			"client", o.clientID, "owner", o.owner, "self", self)
		c.Shutdown()
	}
	return nil
}

// TCPAddr returns the bound TCP address (post-Listen). Useful for tests using :0.
func (e *Engine) TCPAddr() net.Addr {
	e.listenersMu.RLock()
	defer e.listenersMu.RUnlock()
	if e.tcpListener == nil {
		return nil
	}
	return e.tcpListener.Addr()
}

// WSAddr returns the bound WS address.
func (e *Engine) WSAddr() net.Addr {
	e.listenersMu.RLock()
	defer e.listenersMu.RUnlock()
	if e.wsListener == nil {
		return nil
	}
	return e.wsListener.Addr()
}

func (e *Engine) acceptTCP(ctx context.Context, ln net.Listener) {
	defer e.wg.Done()
	for {
		nc, err := ln.Accept()
		if err != nil {
			if isClosedNetErr(err) {
				return
			}
			if ctx.Err() != nil {
				return
			}
			e.logger.Warn("accept", "err", err)
			continue
		}
		if reason, ok := e.checkIPLimits(nc.RemoteAddr()); !ok {
			e.logger.Debug("connect dropped by ip-limiter",
				"addr", nc.RemoteAddr(), "reason", reason)
			if e.metrics != nil {
				e.metrics.ConnectDroppedTotal.WithLabelValues(reason).Inc()
			}
			_ = nc.Close()
			continue
		}
		if !e.tryReserveConn() {
			e.logger.Warn("max connections reached; rejecting", "addr", nc.RemoteAddr())
			e.rejectConnAtCap(nc)
			continue
		}
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer e.releaseConn()
			c := newConn(e, nc)
			c.run(ctx)
		}()
	}
}

func (e *Engine) serveWS(ctx context.Context, ln net.Listener) {
	defer e.wg.Done()
	allowed := e.cfg.WSAllowedOrigins
	if len(allowed) == 0 {
		e.logger.Warn("websocket: WSAllowedOrigins unset; accepting any Origin",
			"hint", "set PGMQTT_WS_ALLOWED_ORIGINS to a comma-separated list to mitigate CSWSH on a publicly-reachable /mqtt endpoint")
	}
	upgrader := websocket.Upgrader{
		Subprotocols: []string{"mqtt", "mqttv3.1", "mqttv3.11"},
		CheckOrigin: func(r *http.Request) bool {
			if len(allowed) == 0 {
				return true
			}
			origin := r.Header.Get("Origin")
			for _, a := range allowed {
				if a == origin {
					return true
				}
			}
			return false
		},
	}
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		// Pre-upgrade IP gate: same penalty-box / connect-rate checks
		// as the TCP listener. Returning 429 (vs upgrading-then-closing)
		// avoids the WebSocket handshake cost entirely for over-rate
		// peers and matches the "hard close, no CONNACK" pattern.
		if reason, ok := e.checkIPLimitsString(r.RemoteAddr); !ok {
			e.logger.Debug("ws connect dropped by ip-limiter",
				"remote", r.RemoteAddr, "reason", reason)
			if e.metrics != nil {
				e.metrics.ConnectDroppedTotal.WithLabelValues(reason).Inc()
			}
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			e.logger.Warn("ws upgrade", "err", err)
			return
		}
		nc := &wsConnAdapter{ws: ws}
		if !e.tryReserveConn() {
			e.logger.Warn("max connections reached; rejecting WS", "remote", r.RemoteAddr)
			e.rejectConnAtCap(nc)
			return
		}
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer e.releaseConn()
			c := newConn(e, nc)
			c.run(ctx)
		}()
	}
	mux.HandleFunc("/", handler)
	mux.HandleFunc("/mqtt", handler)
	srv := &http.Server{Handler: mux}
	e.listenersMu.Lock()
	e.wsServer = srv
	e.listenersMu.Unlock()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && !isClosedNetErr(err) {
		e.logger.Error("ws serve", "err", err)
	}
}

func (e *Engine) shutdownGracefully() {
	e.shutdownOnce.Do(func() {
		close(e.shutdown)
		if e.iplimitCancel != nil {
			e.iplimitCancel()
		}
		e.listenersMu.RLock()
		tcpLn := e.tcpListener
		wsLn := e.wsListener
		wsSrv := e.wsServer
		e.listenersMu.RUnlock()
		if tcpLn != nil {
			_ = tcpLn.Close()
		}
		if wsSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = wsSrv.Shutdown(ctx)
		} else if wsLn != nil {
			_ = wsLn.Close()
		}

		// Snapshot connections, instruct each to drain.
		e.connsMu.RLock()
		conns := make([]*Conn, 0, len(e.conns))
		for _, c := range e.conns {
			conns = append(conns, c)
		}
		e.connsMu.RUnlock()
		for _, c := range conns {
			c.gracefulClose()
		}

		// Detach this pod from sessions it owned.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if id := e.BrokerID(); id != uuid.Nil {
			_, err := e.pool.Exec(ctx,
				`UPDATE sessions SET connected=false, broker_id=NULL WHERE broker_id=$1`, id)
			if err != nil {
				e.logger.Warn("clear broker on shutdown", "err", err)
			}
		}
	})
}

// Shutdown is exposed for callers that want to trigger drain explicitly.
func (e *Engine) Shutdown() { e.shutdownGracefully() }

func (e *Engine) registerConn(clientID string, c *Conn) (prev *Conn) {
	e.connsMu.Lock()
	defer e.connsMu.Unlock()
	prev = e.conns[clientID]
	e.conns[clientID] = c
	if e.metrics != nil {
		if prev == nil {
			e.metrics.Connections.Inc()
		} else {
			// CONNECT for an already-connected client_id is a takeover.
			e.metrics.TakeoversTotal.Inc()
		}
	}
	return prev
}

func (e *Engine) unregisterConnIfSame(clientID string, c *Conn) {
	e.connsMu.Lock()
	defer e.connsMu.Unlock()
	if cur, ok := e.conns[clientID]; ok && cur == c {
		delete(e.conns, clientID)
		if e.metrics != nil {
			e.metrics.Connections.Dec()
		}
	}
}

// ConnFor returns the local *Conn for a clientID if present.
func (e *Engine) ConnFor(clientID string) (*Conn, bool) {
	e.connsMu.RLock()
	defer e.connsMu.RUnlock()
	c, ok := e.conns[clientID]
	return c, ok
}

func isClosedNetErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed)
}

// checkIPLimits gates a brand-new socket against the per-IP limiter.
// Returns ("", true) when the connection should proceed; otherwise
// returns a Prometheus-friendly reason label and false. The caller is
// expected to close the socket (and bump the metric) on a false return.
//
// Order matters: penalty-box (auth-failure cool-off) is checked before
// the connect-rate bucket, since the box explicitly aims to short-
// circuit the bcrypt path even if the IP is well within its CONNECT
// rate budget.
func (e *Engine) checkIPLimits(addr net.Addr) (reason string, ok bool) {
	if addr == nil {
		return "", true
	}
	return e.checkIPLimitsString(addr.String())
}

func (e *Engine) checkIPLimitsString(remote string) (reason string, ok bool) {
	if e.iplimit == nil {
		return "", true
	}
	if e.iplimit.inPenaltyBox(remote) {
		return "penalty_box", false
	}
	if !e.iplimit.allowConnect(remote) {
		return "rate_limit", false
	}
	return "", true
}

// recordAuthFailureFor ticks the limiter's auth-failure bucket for the
// peer behind addr. Called from the CONNECT auth-reject path so a
// stream of bad-credential CONNECTs from a single IP eventually trips
// the penalty box and stops costing us bcrypt CPU.
func (e *Engine) recordAuthFailureFor(addr net.Addr) {
	if e.iplimit == nil || addr == nil {
		return
	}
	e.iplimit.recordAuthFailure(addr.String())
}

// tryReserveConn atomically reserves a slot if under cap. Returns false when
// MaxConnections is configured and at-cap.
func (e *Engine) tryReserveConn() bool {
	cap := e.maxConnsAtomic.Load()
	if cap <= 0 {
		e.openConns.Add(1)
		return true
	}
	for {
		cur := e.openConns.Load()
		if cur >= cap {
			return false
		}
		if e.openConns.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (e *Engine) releaseConn() { e.openConns.Add(-1) }

// OpenConnections returns the current accepted-but-not-yet-closed socket
// count. Exported for /metrics and tests.
func (e *Engine) OpenConnections() int64 { return e.openConns.Load() }

// rejectConnAtCap writes an MQTT-shaped CONNACK rejection (best effort) and
// closes the socket. We don't know the client's protocol version yet, so we
// emit a v5-style CONNACK with reason 0x9F (Connection Rate Exceeded) — v3.1.1
// clients will see a malformed-CONNACK followed by close, which is the same
// outcome they'd see for any other early-reject.
func (e *Engine) rejectConnAtCap(nc net.Conn) {
	defer nc.Close()
	_ = nc.SetWriteDeadline(time.Now().Add(2 * time.Second))
	// Minimal v5 CONNACK: fixed header (0x20 0x03), session present (0x00),
	// reason code 0x9F, properties length 0x00.
	_, _ = nc.Write([]byte{0x20, 0x03, 0x00, 0x9F, 0x00})
}

// localNotifier is the default — no-op. Production swaps in the
// Postgres-LISTEN-backed notifier from the listener package; tests can
// use NewInProcessNotifier for in-memory fanout.
type localNotifier struct{}

func (l *localNotifier) Notify(_ context.Context, _ []uuid.UUID, _ int64) error { return nil }

// NewInProcessNotifier returns a Notifier that dispatches Deliver in-process
// when the message targets this engine's broker ID. Used by single-Pod tests.
func NewInProcessNotifier(e *Engine) Notifier {
	return &inProcNotifier{e: e}
}

type inProcNotifier struct{ e *Engine }

func (n *inProcNotifier) Notify(ctx context.Context, brokerIDs []uuid.UUID, msgID int64) error {
	self := n.e.BrokerID()
	for _, id := range brokerIDs {
		if id == self {
			return n.e.Deliver(ctx, msgID)
		}
	}
	return nil
}

type noopTakeover struct{}

func (noopTakeover) NotifyTakeover(_ context.Context, _ uuid.UUID, _ string) error { return nil }

type noopQuota struct{}

func (noopQuota) NotifyQuota(_ context.Context, _ uuid.UUID, _ string) error { return nil }
