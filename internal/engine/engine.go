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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bo0tzz/pgmqtt/internal/config"
)

// Notifier emits cross-Pod publish notifications. The default notifier
// (NewLocalNotifier) is single-Pod: it dispatches the deliver step in-process.
// In M5 the listener installs a Postgres-LISTEN-backed implementation.
type Notifier interface {
	// Notify is called after a successful publish. brokerIDs are the unique
	// broker UUIDs that own currently-connected subscribers; messageID is the
	// row id in the messages table. Implementations decide whether to fan out
	// via pg_notify, in-process delivery, or both.
	Notify(ctx context.Context, brokerIDs []uuid.UUID, messageID int64) error
}

// TakeoverNotifier emits a takeover signal so the prior owner of a client_id
// can close its now-stale socket. M4 uses a no-op; M6 wires the real one.
type TakeoverNotifier interface {
	NotifyTakeover(ctx context.Context, brokerID uuid.UUID, clientID string) error
}

// Engine is the per-Pod broker.
type Engine struct {
	cfg     *config.Config
	pool    *pgxpool.Pool
	logger  *slog.Logger

	brokerIDMu sync.RWMutex
	brokerID   uuid.UUID

	notify   Notifier
	takeover TakeoverNotifier

	connsMu sync.RWMutex
	conns   map[string]*Conn // client_id -> *Conn

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
func New(_ context.Context, cfg *config.Config, pool *pgxpool.Pool, logger *slog.Logger) (*Engine, error) {
	return &Engine{
		cfg:            cfg,
		pool:           pool,
		logger:         logger,
		conns:          make(map[string]*Conn),
		shutdown:       make(chan struct{}),
		KeepAliveGrace: 1500 * time.Millisecond,
		notify:         &localNotifier{},
		takeover:       noopTakeover{},
	}, nil
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

	// Block on context, then drain.
	<-ctx.Done()
	e.shutdownGracefully()
	e.wg.Wait()
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
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			c := newConn(e, nc)
			c.run(ctx)
		}()
	}
}

func (e *Engine) serveWS(ctx context.Context, ln net.Listener) {
	defer e.wg.Done()
	upgrader := websocket.Upgrader{
		Subprotocols: []string{"mqtt", "mqttv3.1", "mqttv3.11"},
		CheckOrigin:  func(_ *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			e.logger.Warn("ws upgrade", "err", err)
			return
		}
		nc := &wsConnAdapter{ws: ws}
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
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
	return prev
}

func (e *Engine) unregisterConnIfSame(clientID string, c *Conn) {
	e.connsMu.Lock()
	defer e.connsMu.Unlock()
	if cur, ok := e.conns[clientID]; ok && cur == c {
		delete(e.conns, clientID)
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

// localNotifier is the default — no-op. Wires M5's Postgres-LISTEN-backed
// notifier for cross-Pod fanout, or NewInProcessNotifier for single-Pod tests.
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
