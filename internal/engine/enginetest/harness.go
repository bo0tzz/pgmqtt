// Package enginetest provides shared helpers for engine integration tests.
package enginetest

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochi-mqtt/server/v2/packets"
	"golang.org/x/crypto/bcrypt"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// Harness wraps an Engine running on a random port plus a shared Postgres
// for end-to-end tests.
type Harness struct {
	T         *testing.T
	Pool      *pgxpool.Pool
	URL       string
	Engine    *engine.Engine
	BrokerID  uuid.UUID
	cancel    context.CancelFunc
	doneServe chan struct{}
	tcpAddr   string
}

// NewHarness boots a single-pod broker with InProcessNotifier and a default
// user "test"/"test". Use Connect to obtain TestClient instances.
func NewHarness(t *testing.T) *Harness {
	return NewHarnessWith(t, nil)
}

// NewHarnessWith is NewHarness plus a hook that runs against the *engine.Engine
// before Serve is called. Use it to override engine tunables (e.g.
// KeepAliveMultiplier) for a single test.
func NewHarnessWith(t *testing.T, customise PodSetup) *Harness {
	t.Helper()
	url := dbtest.FreshURL(t)
	pool, err := pgxpoolOpen(t, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	seedUser(t, pool, "test", "test")

	eng, brokerID, addr, cancel, done := startBroker(t, pool, url, customise)
	h := &Harness{
		T:         t,
		Pool:      pool,
		URL:       url,
		Engine:    eng,
		BrokerID:  brokerID,
		cancel:    cancel,
		doneServe: done,
		tcpAddr:   addr,
	}
	t.Cleanup(h.Stop)
	return h
}

// PodSetup customises a per-Pod engine boot.
type PodSetup func(*engine.Engine) (cleanup func())

func startBroker(t *testing.T, pool *pgxpool.Pool, url string, customise PodSetup) (
	*engine.Engine, uuid.UUID, string, context.CancelFunc, chan struct{},
) {
	t.Helper()
	cfg := &config.Config{
		DatabaseURL: url,
		TCPAddr:     "127.0.0.1:0",
		WSAddr:      "",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		logger = slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	eng, err := engine.New(context.Background(), cfg, pool, logger)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	brokerID := uuid.New()
	eng.SetBrokerID(brokerID)
	eng.SetNotifier(engine.NewInProcessNotifier(eng))
	// Tests that don't care about metrics get a no-op-ish registry; tests
	// that assert on counters read via eng.Metrics(). Each harness gets
	// its own registry (metrics.New makes one) so parallel tests don't
	// clash on global-registry collisions.
	eng.SetMetrics(metrics.New())

	var cleanup func()
	if customise != nil {
		cleanup = customise(eng)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = eng.Serve(ctx)
		if cleanup != nil {
			cleanup()
		}
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if eng.TCPAddr() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if eng.TCPAddr() == nil {
		cancel()
		t.Fatal("engine never bound TCP")
	}
	return eng, brokerID, eng.TCPAddr().String(), cancel, done
}

func seedUser(t *testing.T, pool *pgxpool.Pool, user, pass string) {
	t.Helper()
	hash, _ := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.MinCost)
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO users(username, password_hash) VALUES($1, $2) ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash`,
		user, string(hash)); err != nil {
		t.Fatalf("seed user: %v", err)
	}
}

// MultiHarness boots N independent Pods sharing a Postgres database. Each Pod
// has its own engine and (via apply) optionally its own listener.
type MultiHarness struct {
	T    *testing.T
	URL  string
	Pool *pgxpool.Pool
	Pods []*Pod
}

// Pod is one node within a MultiHarness.
type Pod struct {
	Engine    *engine.Engine
	BrokerID  uuid.UUID
	TCPAddr   string
	cancel    context.CancelFunc
	doneServe chan struct{}
}

// Connect opens a TCP client against this Pod.
func (p *Pod) Connect(t *testing.T, clientID string, opts ...func(*packets.Packet)) *TestClient {
	t.Helper()
	addr := p.TCPAddr
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tc := &TestClient{Conn: c, r: mqttwire.NewReader(c)}
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: clientID,
			Username:         []byte("test"),
			UsernameFlag:     true,
			Password:         []byte("test"),
			PasswordFlag:     true,
		},
	}
	for _, o := range opts {
		o(pk)
	}
	if err := mqttwire.Write(c, pk); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	tc.r.ProtocolVersion = pk.ProtocolVersion
	resp, err := tc.r.Read()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if resp.FixedHeader.Type != packets.Connack {
		t.Fatalf("expected CONNACK got %d", resp.FixedHeader.Type)
	}
	if resp.ReasonCode != 0 {
		t.Fatalf("connack reason=%d", resp.ReasonCode)
	}
	return tc
}

// NewMultiHarness boots n Pods. The customise function receives each pod's
// engine and may install a listener (which sets brokerID + notifier from
// the listener).
func NewMultiHarness(t *testing.T, n int, customise PodSetup) *MultiHarness {
	t.Helper()
	url := dbtest.FreshURL(t)
	pool, err := pgxpoolOpen(t, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	seedUser(t, pool, "test", "test")

	mh := &MultiHarness{T: t, URL: url, Pool: pool}
	for i := 0; i < n; i++ {
		eng, brokerID, addr, cancel, done := startBroker(t, pool, url, customise)
		mh.Pods = append(mh.Pods, &Pod{
			Engine:    eng,
			BrokerID:  brokerID,
			TCPAddr:   addr,
			cancel:    cancel,
			doneServe: done,
		})
	}
	t.Cleanup(mh.Stop)
	return mh
}

// Stop cancels all pods.
func (mh *MultiHarness) Stop() {
	for _, p := range mh.Pods {
		p.cancel()
		select {
		case <-p.doneServe:
		case <-time.After(5 * time.Second):
			mh.T.Logf("pod shutdown timed out")
		}
	}
	mh.Pool.Close()
}

// Stop cancels the engine's context and waits for shutdown.
func (h *Harness) Stop() {
	h.cancel()
	select {
	case <-h.doneServe:
	case <-time.After(5 * time.Second):
		h.T.Logf("engine shutdown timed out")
	}
	h.Pool.Close()
}

// TCPAddr returns the bound test port.
func (h *Harness) TCPAddr() string { return h.tcpAddr }

// TestClient is a minimal codec-driven MQTT client used by engine tests.
type TestClient struct {
	Conn   net.Conn
	r      *mqttwire.Reader
	pktid  atomic.Uint32
	closed bool
}

// Connect opens a TCP connection and performs CONNECT (MQTT 5 by default).
// If pv is 0 it defaults to v5.
func (h *Harness) Connect(t *testing.T, clientID string, opts ...func(*packets.Packet)) *TestClient {
	t.Helper()
	c, err := net.DialTimeout("tcp", h.tcpAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tc := &TestClient{Conn: c, r: mqttwire.NewReader(c)}

	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: clientID,
			Username:         []byte("test"),
			UsernameFlag:     true,
			Password:         []byte("test"),
			PasswordFlag:     true,
		},
	}
	for _, o := range opts {
		o(pk)
	}
	if err := mqttwire.Write(c, pk); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	tc.r.ProtocolVersion = pk.ProtocolVersion
	resp, err := tc.r.Read()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if resp.FixedHeader.Type != packets.Connack {
		t.Fatalf("expected CONNACK got %d", resp.FixedHeader.Type)
	}
	if resp.ReasonCode != 0 {
		t.Fatalf("connack reason=%d", resp.ReasonCode)
	}
	return tc
}

func (c *TestClient) NextPacketID() uint16 {
	v := c.pktid.Add(1)
	if v == 0 || v > 0xFFFF {
		c.pktid.Store(1)
		v = 1
	}
	return uint16(v)
}

// Publish sends a PUBLISH and (if QoS>0) waits for the matching ack.
func (c *TestClient) Publish(t *testing.T, topic string, payload []byte, qos byte, retain bool) {
	t.Helper()
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: qos, Retain: retain},
		ProtocolVersion: c.r.ProtocolVersion,
		TopicName:       topic,
		Payload:         payload,
	}
	if qos > 0 {
		pk.PacketID = c.NextPacketID()
	}
	if err := mqttwire.Write(c.Conn, pk); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if qos == 1 {
		ack, err := c.r.Read()
		if err != nil {
			t.Fatalf("read puback: %v", err)
		}
		if ack.FixedHeader.Type != packets.Puback {
			t.Fatalf("expected puback, got %d", ack.FixedHeader.Type)
		}
	} else if qos == 2 {
		rec, err := c.r.Read()
		if err != nil {
			t.Fatalf("read pubrec: %v", err)
		}
		if rec.FixedHeader.Type != packets.Pubrec {
			t.Fatalf("expected pubrec, got %d", rec.FixedHeader.Type)
		}
		if err := mqttwire.Write(c.Conn, &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
			ProtocolVersion: c.r.ProtocolVersion,
			PacketID:        pk.PacketID,
		}); err != nil {
			t.Fatalf("pubrel: %v", err)
		}
		comp, err := c.r.Read()
		if err != nil {
			t.Fatalf("read pubcomp: %v", err)
		}
		if comp.FixedHeader.Type != packets.Pubcomp {
			t.Fatalf("expected pubcomp, got %d", comp.FixedHeader.Type)
		}
	}
}

// Subscribe sends a SUBSCRIBE and waits for SUBACK. Returns the codes.
func (c *TestClient) Subscribe(t *testing.T, filter string, qos byte) []byte {
	t.Helper()
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: c.r.ProtocolVersion,
		PacketID:        c.NextPacketID(),
		Filters:         packets.Subscriptions{{Filter: filter, Qos: qos}},
	}
	if err := mqttwire.Write(c.Conn, pk); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	resp, err := c.r.Read()
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	if resp.FixedHeader.Type != packets.Suback {
		t.Fatalf("expected suback got %d", resp.FixedHeader.Type)
	}
	return resp.ReasonCodes
}

// TryRead returns the next non-PINGRESP packet or nil if the read times
// out. It does NOT fatal — use it when "no packet within timeout" is the
// asserted outcome.
func (c *TestClient) TryRead(timeout time.Duration) *packets.Packet {
	if err := c.Conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil
	}
	defer c.Conn.SetReadDeadline(time.Time{})
	for {
		pk, err := c.r.Read()
		if err != nil {
			return nil
		}
		if pk.FixedHeader.Type == packets.Pingresp {
			continue
		}
		if pk.FixedHeader.Type == packets.Publish && pk.FixedHeader.Qos == 1 {
			_ = mqttwire.Write(c.Conn, &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Puback},
				ProtocolVersion: c.r.ProtocolVersion,
				PacketID:        pk.PacketID,
			})
		}
		return &pk
	}
}

// Read returns the next non-PINGRESP packet or fails on timeout.
func (c *TestClient) Read(t *testing.T, timeout time.Duration) packets.Packet {
	t.Helper()
	if err := c.Conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	defer c.Conn.SetReadDeadline(time.Time{})
	for {
		pk, err := c.r.Read()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if pk.FixedHeader.Type == packets.Pingresp {
			continue
		}
		// Auto-ack QoS>0 publishes for tests that aren't testing acks.
		if pk.FixedHeader.Type == packets.Publish && pk.FixedHeader.Qos == 1 {
			_ = mqttwire.Write(c.Conn, &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Puback},
				ProtocolVersion: c.r.ProtocolVersion,
				PacketID:        pk.PacketID,
			})
		}
		return pk
	}
}

// Disconnect sends a v5 DISCONNECT (with optional property customisation
// via the variadic opts) before closing the socket. Use this when the
// test needs to exercise DISCONNECT property handling — for vanilla
// graceful close call Close.
func (c *TestClient) Disconnect(t *testing.T, opts ...func(*packets.Packet)) {
	t.Helper()
	if c.closed {
		return
	}
	c.closed = true
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Disconnect},
		ProtocolVersion: c.r.ProtocolVersion,
	}
	for _, opt := range opts {
		opt(pk)
	}
	if err := mqttwire.Write(c.Conn, pk); err != nil {
		t.Fatalf("write disconnect: %v", err)
	}
	_ = c.Conn.Close()
}

// Close politely sends DISCONNECT and closes the socket.
func (c *TestClient) Close() {
	if c.closed {
		return
	}
	c.closed = true
	_ = mqttwire.Write(c.Conn, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Disconnect},
		ProtocolVersion: c.r.ProtocolVersion,
	})
	_ = c.Conn.Close()
}

// Kill closes the underlying socket without DISCONNECT — simulates an ungraceful drop.
func (c *TestClient) Kill() {
	c.closed = true
	_ = c.Conn.Close()
}

// NextRaw reads the next packet without auto-acking. Used by tests that need
// to assert on raw inbound packets.
func (c *TestClient) NextRaw() (packets.Packet, error) {
	return c.r.Read()
}

// WritePacket sends a raw packet using the client's negotiated protocol.
func (c *TestClient) WritePacket(pk *packets.Packet) error {
	pk.ProtocolVersion = c.r.ProtocolVersion
	return mqttwire.Write(c.Conn, pk)
}

// pgxpoolOpen is a small wrapper avoiding a circular import on dbtest.
func pgxpoolOpen(t *testing.T, url string) (*pgxpool.Pool, error) {
	t.Helper()
	return pgxpool.New(context.Background(), url)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
