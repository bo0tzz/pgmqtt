package engine_test

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

var _ net.Conn = (*net.TCPConn)(nil)

func openTestPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	return p
}

func TestQoS0PublishSubscribeRoundTrip(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	sub := h.Connect(t, "sub-0")
	pub := h.Connect(t, "pub-0")
	defer sub.Close()
	defer pub.Close()

	codes := sub.Subscribe(t, "house/+/light", 0)
	if len(codes) != 1 || codes[0] > 2 {
		t.Fatalf("suback codes: %v", codes)
	}

	pub.Publish(t, "house/kitchen/light", []byte("on"), 0, false)
	pk := sub.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if pk.TopicName != "house/kitchen/light" || string(pk.Payload) != "on" {
		t.Fatalf("payload mismatch: %s=%q", pk.TopicName, pk.Payload)
	}
}

func TestQoS1PublishSubscribe(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	sub := h.Connect(t, "sub-1")
	pub := h.Connect(t, "pub-1")
	defer sub.Close()
	defer pub.Close()

	sub.Subscribe(t, "a/b", 1)
	pub.Publish(t, "a/b", []byte("hello"), 1, false)

	pk := sub.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish || pk.FixedHeader.Qos != 1 {
		t.Fatalf("got type=%d qos=%d", pk.FixedHeader.Type, pk.FixedHeader.Qos)
	}
	if string(pk.Payload) != "hello" {
		t.Fatalf("payload = %q", pk.Payload)
	}
}

func TestRetainedDelivery(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	pub := h.Connect(t, "pub-r")
	pub.Publish(t, "state/light", []byte("on"), 1, true)
	pub.Close()

	// New subscriber after publish: should still get the retained message.
	sub := h.Connect(t, "sub-r")
	defer sub.Close()
	sub.Subscribe(t, "state/+", 1)

	pk := sub.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("got %d", pk.FixedHeader.Type)
	}
	if !pk.FixedHeader.Retain {
		t.Errorf("expected retain flag set")
	}
	if string(pk.Payload) != "on" || pk.TopicName != "state/light" {
		t.Errorf("got %q/%q", pk.TopicName, pk.Payload)
	}
}

// TestQoS0DeliveriesDoNotAccumulate is the regression guard for the
// May 2026 zigbee2mqtt-blackhole bug: QoS-0 publishes inserted a
// `deliveries` row at fanout (cross-broker routing handoff) but never
// deleted it after the wire write. Over time, a stable QoS-0 subscriber
// hit MaxQueuedDeliveriesPerClient (10000) and `mqtt_publish` silently
// dropped further inserts — with no DISCONNECT, log, or metric — so
// the subscriber stopped receiving every matching message.
//
// The fix deletes the row after a successful conn.write on QoS-0. This
// test publishes far more messages than would have ever drained pre-fix,
// then asserts the subscriber's live deliveries count stays bounded.
func TestQoS0DeliveriesDoNotAccumulate(t *testing.T) {
	t.Parallel()
	const N = 200
	h := enginetest.NewHarness(t)

	sub := h.Connect(t, "qos0-sub")
	defer sub.Close()
	sub.Subscribe(t, "qos0/flood", 0)

	pub := h.Connect(t, "qos0-pub")
	for i := 0; i < N; i++ {
		pub.Publish(t, "qos0/flood", []byte("ping"), 0, false)
	}
	pub.Close()

	// Drain the subscriber side so the wire write — and therefore the
	// delete — has actually run for every message before we count.
	deadline := time.Now().Add(5 * time.Second)
	received := 0
	for received < N && time.Now().Before(deadline) {
		pk := sub.TryRead(500 * time.Millisecond)
		if pk == nil {
			break
		}
		if pk.FixedHeader.Type == packets.Publish {
			received++
		}
	}
	if received < N {
		t.Fatalf("received %d of %d QoS-0 publishes", received, N)
	}

	// Allow a small in-flight slack — the DELETE is best-effort async
	// relative to the read above; a few unfinished ones is fine, an
	// accumulating monotonic count is not.
	deadline = time.Now().Add(2 * time.Second)
	var live int
	for time.Now().Before(deadline) {
		if err := h.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM deliveries WHERE client_id=$1 AND state=0`,
			"qos0-sub").Scan(&live); err != nil {
			t.Fatalf("count deliveries: %v", err)
		}
		if live <= 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if live > 10 {
		t.Fatalf("QoS-0 deliveries accumulated: %d rows live for qos0-sub after %d publishes (want bounded)", live, N)
	}
}

// TestHADiscoveryFlood simulates Home Assistant's startup pattern: a single
// client publishes hundreds of retained PUBLISHes in rapid succession
// (auto-discovery topics + their initial state). We then connect a fresh
// subscriber and verify all retained messages are replayed without loss.
//
// HA's MQTT integration on first connect publishes one retained
// `homeassistant/<component>/<node>/<obj>/config` per discovered entity,
// frequently followed by an initial state retain on a separate topic.
// 250 entities → ~500 retained publishes in a burst is realistic for a
// moderately busy install.
//
// What this test exercises:
//   - The retained-table write path under burst load.
//   - That none of the publishes are dropped silently.
//   - That the subscribe-replay path can flush the full retained set.
//
// What it does NOT test (covered separately):
//   - Per-IP CONNECT rate limiter — burst is one connection, many publishes.
//   - bcrypt cost on auth — covered by iplimit_test.go and connect path.
func TestHADiscoveryFlood(t *testing.T) {
	t.Parallel()
	const N = 500
	h := enginetest.NewHarness(t)

	pub := h.Connect(t, "ha-flood-pub")
	for i := 0; i < N; i++ {
		topic := "homeassistant/sensor/dev_" + itoa(i) + "/config"
		payload := []byte(`{"name":"sensor","state_topic":"sensor/state"}`)
		// QoS 1 here (HA's real discovery uses 0, but we need PUBACK
		// confirmation that the retained UPSERT committed before we
		// disconnect — otherwise the subsequent SUBSCRIBE can race
		// with the publish path and pick up messages via deliver-scan
		// instead of retained-replay, which doesn't set the RETAIN
		// flag. The test under load is the same shape either way.
		pub.Publish(t, topic, payload, 1, true)
	}
	pub.Close()

	sub := h.Connect(t, "ha-flood-sub")
	defer sub.Close()
	sub.Subscribe(t, "homeassistant/#", 0)

	got := 0
	deadline := time.Now().Add(10 * time.Second)
	for got < N && time.Now().Before(deadline) {
		pk := sub.Read(t, 2*time.Second)
		if pk.FixedHeader.Type != packets.Publish {
			t.Fatalf("expected publish, got type=%d after %d msgs", pk.FixedHeader.Type, got)
		}
		if !pk.FixedHeader.Retain {
			t.Errorf("retained replay should set RETAIN flag (msg %d)", got)
		}
		got++
	}
	if got != N {
		t.Fatalf("retained replay got %d, want %d (lost %d)", got, N, N-got)
	}
}

// itoa avoids importing strconv just for this loop.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func TestRetainedClearWithEmpty(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	pub := h.Connect(t, "pub-rc")
	pub.Publish(t, "state/light", []byte("on"), 1, true)
	pub.Publish(t, "state/light", []byte{}, 1, true)
	pub.Close()

	var n int
	if err := h.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM retained WHERE topic='state/light'`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 0 {
		t.Errorf("expected retained cleared, found %d rows", n)
	}
}

func TestSessionResume(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	// v5 clean_start=false + SessionExpiryInterval>0 means resumable.
	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	}
	sub1 := h.Connect(t, "sub-resume", persistent)
	sub1.Subscribe(t, "rx/#", 1)
	sub1.Close()

	pub := h.Connect(t, "pub-resume")
	pub.Publish(t, "rx/foo", []byte("queued"), 1, false)
	pub.Close()

	// Reconnect — should drain the queued QoS-1 message.
	sub2 := h.Connect(t, "sub-resume", persistent)
	defer sub2.Close()
	pk := sub2.Read(t, 2*time.Second)
	if string(pk.Payload) != "queued" {
		t.Fatalf("payload = %q", pk.Payload)
	}
}

func TestNoLocalSuppression(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	c := h.Connect(t, "nolocal")
	defer c.Close()

	// Subscribe with NoLocal=true.
	sub := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		PacketID:    c.NextPacketID(),
		Filters:     packets.Subscriptions{{Filter: "x/y", NoLocal: true}},
	}
	if err := c.WritePacket(sub); err != nil {
		t.Fatalf("sub: %v", err)
	}
	if _, err := c.NextRaw(); err != nil {
		t.Fatalf("suback: %v", err)
	}

	c.Publish(t, "x/y", []byte("p"), 0, false)

	deadline := time.Now().Add(500 * time.Millisecond)
	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	pk, err := c.NextRaw()
	if err == nil && pk.FixedHeader.Type == packets.Publish {
		t.Fatalf("nolocal client should not have received its own publish")
	}
}

// TestWillDelayCancelledByReconnectBeforeFire pins MQTT-3.1.3.2.2:
// when a v5 client with WillDelayInterval > 0 reconnects within the
// delay window, its will MUST NOT fire. takeOwnership clears
// will_fire_at on the new CONNECT, so the janitor's fireDueWills
// query (`WHERE will_fire_at IS NOT NULL`) skips the row.
func TestWillDelayCancelledByReconnectBeforeFire(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	observer := h.Connect(t, "obs-will-cancel")
	defer observer.Close()
	observer.Subscribe(t, "lwt/cancel", 1)

	withWill := func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "lwt/cancel"
		p.Connect.WillPayload = []byte("would-fire-without-cancel")
		p.Connect.WillQos = 1
		// WillDelayInterval is a *Will property*, separate from CONNECT
		// properties. mochi keeps them on Connect.WillProperties; setting
		// p.Properties.WillDelayInterval is silently ignored.
		p.Connect.WillProperties.WillDelayInterval = 30
		// SessionExpiry must exceed WillDelay or the latter is clamped
		// to zero per MQTT-3.1.3-9 (we min(delay, expiry)).
		p.Properties.SessionExpiryInterval = 60
		p.Properties.SessionExpiryIntervalFlag = true
		p.Connect.Clean = false
	}
	willer := h.Connect(t, "will-cancel-1", withWill)
	willer.Kill() // ungraceful — will scheduled with 30s delay

	// Reconnect before the delay elapses (immediately is fine).
	willer2 := h.Connect(t, "will-cancel-1", withWill)
	defer willer2.Close()

	// will MUST NOT fire — short read deadline since we're asserting
	// nothing arrives, not that it arrives slowly.
	if pk := observer.TryRead(500 * time.Millisecond); pk != nil && pk.FixedHeader.Type == packets.Publish {
		t.Fatalf("will fired despite reconnect within delay window: topic=%q payload=%q",
			pk.TopicName, pk.Payload)
	}
}

// TestSessionExpiryDisconnectExtension reproduces a slice of Paho's
// test_session_expiry: connect with SE=N, then DISCONNECT carrying a
// larger SE value should extend the session, NOT be silently dropped.
// Connect with SE=0 then DISCONNECT with SE>0 is the only invalid
// case (per [MQTT-3.14.2.2.2]) — that should still get rejected.
func TestSessionExpiryDisconnectExtension(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	clientID := "se-ext-1"
	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 1
		p.Properties.SessionExpiryIntervalFlag = true
	}
	c1 := h.Connect(t, clientID, persistent)
	c1.Subscribe(t, "se/x", 1)

	// DISCONNECT carrying SE=10 (extending from 1).
	c1.Disconnect(t, func(p *packets.Packet) {
		p.Properties.SessionExpiryInterval = 10
		p.Properties.SessionExpiryIntervalFlag = true
	})

	// handleDisconnect's deferred UPDATE runs on a background context
	// after the broker's read loop unwinds; poll briefly so we don't
	// race the test query against the async commit.
	var expiresAt *time.Time
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := h.Pool.QueryRow(ctx,
			`SELECT session_expires_at FROM sessions WHERE client_id=$1`, clientID).Scan(&expiresAt); err != nil {
			t.Fatalf("query: %v", err)
		}
		if expiresAt != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if expiresAt == nil {
		t.Fatalf("session_expires_at must be set after DISCONNECT with SE>0")
	}
	delta := time.Until(*expiresAt)
	if delta < 8*time.Second || delta > 12*time.Second {
		t.Errorf("session_expires_at delta = %v; want ~10s", delta)
	}
}

// TestSessionExpiryDisconnectCancellation reproduces the cancel slice
// of Paho's test_session_expiry: connect with SE=N>0, DISCONNECT with
// SE=0 cancels the session immediately. The session row MUST be
// deleted on disconnect so a subsequent CONNECT cleanstart=False sees
// sessionPresent=False.
func TestSessionExpiryDisconnectCancellation(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	clientID := "se-cancel-1"
	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 60
		p.Properties.SessionExpiryIntervalFlag = true
	}
	c1 := h.Connect(t, clientID, persistent)
	c1.Subscribe(t, "se/y", 1)
	// DISCONNECT carrying SE=0 cancels the persistent session.
	c1.Disconnect(t, func(p *packets.Packet) {
		p.Properties.SessionExpiryInterval = 0
		p.Properties.SessionExpiryIntervalFlag = true
	})

	// Poll for the asynchronous DELETE to land — same race as
	// TestSessionExpiryDisconnectExtension above.
	deadline := time.Now().Add(2 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if err := h.Pool.QueryRow(ctx,
			`SELECT count(*) FROM sessions WHERE client_id=$1`, clientID).Scan(&n); err != nil {
			t.Fatalf("query: %v", err)
		}
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("session row not deleted after DISCONNECT SE=0; count=%d", n)
}

func TestWillFiresOnUngraceful(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	observer := h.Connect(t, "obs-will")
	defer observer.Close()
	observer.Subscribe(t, "lwt/#", 1)

	withWill := func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "lwt/foo"
		p.Connect.WillPayload = []byte("died")
		p.Connect.WillQos = 1
	}
	willer := h.Connect(t, "will-1", withWill)
	willer.Kill() // ungraceful

	pk := observer.Read(t, 3*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected will publish, got %d", pk.FixedHeader.Type)
	}
	if string(pk.Payload) != "died" || pk.TopicName != "lwt/foo" {
		t.Errorf("got %q/%q", pk.TopicName, pk.Payload)
	}
}

func TestQoS2RoundTrip(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	sub := h.Connect(t, "sub-2")
	pub := h.Connect(t, "pub-2")
	defer sub.Close()
	defer pub.Close()

	sub.Subscribe(t, "q2/+", 2)
	pub.Publish(t, "q2/topic", []byte("exact-once"), 2, false)

	pk := sub.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if pk.FixedHeader.Qos != 2 {
		t.Errorf("delivered qos = %d, want 2", pk.FixedHeader.Qos)
	}
	if string(pk.Payload) != "exact-once" {
		t.Errorf("payload = %q", pk.Payload)
	}
}

func TestMQTTv311PublishSubscribe(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	v311 := func(p *packets.Packet) {
		p.ProtocolVersion = 4
	}
	sub := h.Connect(t, "v311-sub", v311)
	pub := h.Connect(t, "v311-pub", v311)
	defer sub.Close()
	defer pub.Close()

	sub.Subscribe(t, "legacy/+", 1)
	pub.Publish(t, "legacy/topic", []byte("ok"), 1, false)

	pk := sub.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if string(pk.Payload) != "ok" {
		t.Errorf("payload = %q", pk.Payload)
	}
}

func TestGracefulShutdownClearsBrokerID(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	}
	c := h.Connect(t, "shut-1", persistent)
	c.Subscribe(t, "x/y", 1)
	c.Close()

	// Stop engine to simulate graceful shutdown.
	h.Stop()

	pool := openTestPool(t, h.URL)
	defer pool.Close()
	var brokerID *string
	if err := pool.QueryRow(context.Background(),
		`SELECT broker_id::text FROM sessions WHERE client_id='shut-1'`).Scan(&brokerID); err != nil {
		t.Fatalf("query: %v", err)
	}
	if brokerID != nil {
		t.Errorf("expected broker_id cleared after shutdown, got %v", *brokerID)
	}
}

func TestWillSuppressedOnGraceful(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	observer := h.Connect(t, "obs-nowill")
	defer observer.Close()
	observer.Subscribe(t, "lwt/#", 1)

	withWill := func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "lwt/foo"
		p.Connect.WillPayload = []byte("dontfire")
		p.Connect.WillQos = 0
	}
	willer := h.Connect(t, "will-2", withWill)
	willer.Close() // graceful — will MUST be suppressed

	if err := observer.Conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	pk, err := observer.NextRaw()
	if err == nil && pk.FixedHeader.Type == packets.Publish {
		t.Fatalf("graceful disconnect must suppress will, got publish %q", pk.Payload)
	}
}

// TestMaxConnectionsRejects asserts that a Pod at the connection cap rejects
// new sockets with CONNACK reason 0x9F.
func TestMaxConnectionsRejects(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	h.Engine.SetMaxConnectionsForTest(1)

	c1 := h.Connect(t, "ml-1")
	defer c1.Close()

	// Direct TCP dial — write a CONNECT, expect CONNACK 0x9F + close.
	d, err := net.DialTimeout("tcp", h.TCPAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer d.Close()
	if err := d.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	// Read whatever the broker sends and assert CONNACK with 0x9F.
	buf := make([]byte, 5)
	if _, err := io.ReadFull(d, buf); err != nil {
		t.Fatalf("read connack: %v", err)
	}
	// 0x20 = CONNACK fixed header. buf[3] is the reason code.
	if buf[0] != 0x20 || buf[3] != 0x9F {
		t.Fatalf("expected CONNACK 0x9F, got % x", buf)
	}
}

// TestRateLimitDisconnects asserts that exceeding the per-conn inbound rate
// triggers DISCONNECT 0x96.
func TestRateLimitDisconnects(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	h.Engine.SetMaxInboundRateForTest(1) // 1 msg/sec

	c := h.Connect(t, "rl-1")
	defer c.Close()

	// First publish consumes the only token; second publish in the same tick
	// must trip the limit.
	c.Publish(t, "rl/x", []byte("a"), 0, false)
	// Send a raw QoS-0 PUBLISH so we don't wait for an ack.
	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
		TopicName:   "rl/x",
		Payload:     []byte("b"),
	}
	if err := c.WritePacket(pk); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	got, err := c.NextRaw()
	if err != nil {
		t.Fatalf("expected DISCONNECT, got read err: %v", err)
	}
	if got.FixedHeader.Type != packets.Disconnect || got.ReasonCode != 0x96 {
		t.Fatalf("expected DISCONNECT 0x96, got type=%d reason=0x%X", got.FixedHeader.Type, got.ReasonCode)
	}
}

// TestUnsubscribeStopsDelivery verifies that after a client UNSUBSCRIBEs,
// new PUBLISHes to the previously-matched filter are not delivered. Catches
// regressions in the subscriptions-table delete path.
func TestUnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	sub := h.Connect(t, "unsub-1")
	defer sub.Close()
	pub := h.Connect(t, "unsub-pub")
	defer pub.Close()

	sub.Subscribe(t, "us/+", 0)
	pub.Publish(t, "us/before", []byte("before"), 0, false)
	pk := sub.Read(t, 2*time.Second)
	if string(pk.Payload) != "before" {
		t.Fatalf("first message payload = %q, want before", pk.Payload)
	}

	// Unsubscribe.
	unsub := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Unsubscribe, Qos: 1},
		ProtocolVersion: 5,
		PacketID:        sub.NextPacketID(),
		Filters:         packets.Subscriptions{{Filter: "us/+"}},
	}
	if err := sub.WritePacket(unsub); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	ack, err := sub.NextRaw()
	if err != nil {
		t.Fatalf("unsuback: %v", err)
	}
	if ack.FixedHeader.Type != packets.Unsuback {
		t.Fatalf("expected UNSUBACK got %d", ack.FixedHeader.Type)
	}

	pub.Publish(t, "us/after", []byte("after"), 0, false)

	// Subscriber must not receive — set a short read deadline so this
	// doesn't hang on regression.
	if err := sub.Conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	got, err := sub.NextRaw()
	if err == nil && got.FixedHeader.Type == packets.Publish {
		t.Fatalf("got publish after unsubscribe: %q", got.Payload)
	}
}

// TestQoS2OutboundFullDance has the subscriber complete the full
// PUBREC→PUBREL→PUBCOMP handshake so the broker's outbound-QoS-2 state
// machine (handlePubrec + handlePubcomp) actually runs. Without this the
// receiver-side dance is exercised but the publisher-side handlers on the
// broker are never reached by tests.
func TestQoS2OutboundFullDance(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	sub := h.Connect(t, "q2-full-sub")
	defer sub.Close()
	pub := h.Connect(t, "q2-full-pub")
	defer pub.Close()

	sub.Subscribe(t, "q2full/+", 2)
	pub.Publish(t, "q2full/x", []byte("exactly-once"), 2, false)

	pk := sub.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish || pk.FixedHeader.Qos != 2 {
		t.Fatalf("expected QoS-2 PUBLISH, got type=%d qos=%d",
			pk.FixedHeader.Type, pk.FixedHeader.Qos)
	}

	// Subscriber → broker: PUBREC. Broker's handlePubrec moves the
	// delivery to state=2 and sends PUBREL.
	if err := sub.WritePacket(&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Pubrec},
		ProtocolVersion: 5,
		PacketID:        pk.PacketID,
	}); err != nil {
		t.Fatalf("pubrec: %v", err)
	}
	pubrel, err := sub.NextRaw()
	if err != nil {
		t.Fatalf("read pubrel: %v", err)
	}
	if pubrel.FixedHeader.Type != packets.Pubrel {
		t.Fatalf("expected PUBREL got %d", pubrel.FixedHeader.Type)
	}

	// Subscriber → broker: PUBCOMP. Broker's handlePubcomp deletes the
	// delivery row.
	if err := sub.WritePacket(&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Pubcomp},
		ProtocolVersion: 5,
		PacketID:        pubrel.PacketID,
	}); err != nil {
		t.Fatalf("pubcomp: %v", err)
	}

	// Cross-check: no leftover delivery rows for this client.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := h.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM deliveries WHERE client_id='q2-full-sub'`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("delivery row not cleared after PUBCOMP")
}

// TestSlowSubscriberQuotaExceeded forces a subscriber's pending-deliveries
// queue past the cap and asserts the broker DISCONNECTs them with reason
// 0x97 (Quota Exceeded). The QoS-0 drop branch is exercised in the same
// scenario by separately publishing a QoS-0 message past the cap.
func TestSlowSubscriberQuotaExceeded(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	h.Engine.SetMaxQueuedDeliveriesForTest(2)

	sub := h.Connect(t, "slow-sub")
	defer sub.Close()
	sub.Subscribe(t, "bp/#", 1)

	// Pre-fill the deliveries table so the next publish overflows. Insert two
	// dummy messages and two delivery rows pointing at them.
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		var msgID int64
		if err := h.Pool.QueryRow(ctx, `
			INSERT INTO messages(topic, payload, qos, retain) VALUES ('bp/x', $1, 1, false)
			RETURNING id`, []byte("seed")).Scan(&msgID); err != nil {
			t.Fatalf("seed message: %v", err)
		}
		if _, err := h.Pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, state) VALUES ($1, $2, 1, 0)
		`, "slow-sub", msgID); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}

	// Publish QoS-1 on a different conn — fanout sees existing 2 ≥ cap=2,
	// skips insert, returns slow-sub in overflow_clients. Engine writes
	// DISCONNECT 0x97 to the local conn and tears it down.
	pub := h.Connect(t, "pub-bp")
	defer pub.Close()
	pub.Publish(t, "bp/y", []byte("overflow"), 1, false)

	if err := sub.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	pk, err := sub.NextRaw()
	if err != nil {
		t.Fatalf("expected DISCONNECT, got read err: %v", err)
	}
	if pk.FixedHeader.Type != packets.Disconnect {
		t.Fatalf("expected DISCONNECT, got type=%d", pk.FixedHeader.Type)
	}
	if pk.ReasonCode != 0x97 {
		t.Fatalf("expected reason 0x97 (Quota Exceeded), got 0x%X", pk.ReasonCode)
	}
}

// TestPreConnectPacketSizeCapped verifies the broker hard-closes a
// connection whose first framed packet declares a remaining length above
// the codec's pre-CONNECT 1 MiB cap. Allocation must NOT happen for the
// announced size — that's the DoS the cap exists to prevent.
func TestPreConnectPacketSizeCapped(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	d, err := net.DialTimeout("tcp", h.TCPAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer d.Close()

	// CONNECT-shaped fixed header with remaining length = 2 MiB. The
	// broker should reject before reading the body and hard-close.
	frame := []byte{
		0x10,                   // CONNECT, flags 0
		0x80, 0x80, 0x80, 0x01, // remaining length = 2 MiB
	}
	if _, err := d.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Expect the broker to close the socket — read returns EOF (or other
	// network err) without any CONNACK bytes coming back.
	if err := d.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	buf := make([]byte, 4)
	n, err := d.Read(buf)
	if err == nil && n > 0 {
		t.Fatalf("expected hard-close after oversize CONNECT, got % x", buf[:n])
	}
	// io.EOF or any net err is acceptable — the contract is "no CONNACK,
	// socket closed".
}

// TestConnectWithAuthenticationMethodRejected verifies that a v5 CONNECT
// with AuthenticationMethod set is rejected with CONNACK 0x8C (Bad
// authentication method). pgmqtt does not advertise an enhanced-auth
// method on CONNACK, so per MQTT-4.12.0-1 the server may reject
// AuthenticationMethod-bearing CONNECTs; previously we silently
// downgraded to password auth.
func TestConnectWithAuthenticationMethodRejected(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	c, err := net.DialTimeout("tcp", h.TCPAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: "auth-method-client",
			Username:         []byte("test"),
			UsernameFlag:     true,
			Password:         []byte("test"),
			PasswordFlag:     true,
		},
	}
	pk.Properties.AuthenticationMethod = "SCRAM-SHA-256"

	if err := mqttwire.Write(c, pk); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	r := mqttwire.NewReader(c)
	r.ProtocolVersion = mqttwire.ProtocolMQTT5
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	resp, err := r.Read()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if resp.FixedHeader.Type != packets.Connack {
		t.Fatalf("expected CONNACK, got %d", resp.FixedHeader.Type)
	}
	if resp.ReasonCode != 0x8C {
		t.Fatalf("expected CONNACK 0x8C (bad auth method), got 0x%X", resp.ReasonCode)
	}
}

// TestDeliverAndDrainRaceClaimsExactlyOnce is the regression guard for
// BUG-A (v0.1.9 QoS-0 fix had a hole): for QoS-0, the post-write DELETE
// left a window where a concurrent caller (drainSessionQueue on reconnect
// vs. NOTIFY-driven Deliver) saw the same row in state=0 and both wrote
// to the wire. The fix moves the claim to a DELETE-RETURNING BEFORE the
// write so only one caller wins.
//
// We provoke the race directly: insert a single QoS-0 deliveries row for
// a connected subscriber, then fire N parallel Deliver calls against the
// same message id. With the fix, exactly one PUBLISH reaches the
// subscriber and the row is gone. Without the fix, the subscriber sees
// the message multiple times.
func TestDeliverAndDrainRaceClaimsExactlyOnce(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	sub := h.Connect(t, "race-sub")
	defer sub.Close()
	sub.Subscribe(t, "race/topic", 0)

	ctx := context.Background()
	const N = 16

	// Insert one message + one QoS-0 deliveries row directly so we can
	// fire Deliver against a single known message id and assert the
	// at-most-once invariant.
	var msgID int64
	if err := h.Pool.QueryRow(ctx, `
		INSERT INTO messages(topic, payload, qos, retain) VALUES ('race/topic', $1, 0, false)
		RETURNING id`, []byte("racing")).Scan(&msgID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	if _, err := h.Pool.Exec(ctx, `
		INSERT INTO deliveries(client_id, message_id, qos, state) VALUES ($1, $2, 0, 0)
	`, "race-sub", msgID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	// Fire N concurrent Deliver calls. With the CAS-claim fix, exactly
	// one wins the DELETE-RETURNING and writes; the rest see RowsAffected==0
	// and skip.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = h.Engine.Deliver(ctx, msgID)
		}()
	}
	close(start)
	wg.Wait()

	// Count PUBLISHes received by the subscriber within a short window.
	received := 0
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		pk := sub.TryRead(200 * time.Millisecond)
		if pk == nil {
			break
		}
		if pk.FixedHeader.Type == packets.Publish {
			received++
		}
	}
	if received != 1 {
		t.Fatalf("expected exactly one PUBLISH (at-most-once QoS-0 with CAS-claim), got %d", received)
	}

	// And the row should be gone — the winning Deliver DELETE-RETURNINGed it.
	var n int
	if err := h.Pool.QueryRow(ctx,
		`SELECT count(*) FROM deliveries WHERE id IN (SELECT id FROM deliveries WHERE client_id=$1 AND message_id=$2)`,
		"race-sub", msgID).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if n != 0 {
		t.Fatalf("delivery row not deleted after winning claim: %d row(s)", n)
	}
}

// TestPubackBogusPacketIDDoesNotReleaseSlot is the regression guard for
// BUG-B: PUBACK / PUBCOMP for an unknown packet id used to call
// returnInflight() unconditionally, freeing a slot that wasn't actually
// held. A misbehaving v5 client could inflate the outbound flow-control
// window past ReceiveMaximum. Fix: gate the release on RowsAffected > 0.
//
// We can't probe the internal channel from the test package; instead, we
// fill the outbound window to capacity (one queued QoS-1 publish that
// holds a slot), send a PUBACK with a bogus packet id, then send a real
// PUBACK and assert the broker still rejects further sends until the
// real PUBACK arrives — i.e. the bogus PUBACK didn't free anything.
//
// Implementation: set ReceiveMaximum=1 on CONNECT. Pub one QoS-1 message
// to the subscriber. The subscriber gets one PUBLISH; without acking it,
// publish a second QoS-1 to the same subscriber — should not be
// delivered (slot held). Send bogus PUBACK; second publish should still
// not be delivered (bug would deliver it because the bogus PUBACK
// "freed" the slot).
func TestPubackBogusPacketIDDoesNotReleaseSlot(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	sub := h.Connect(t, "puback-bogus-sub", func(p *packets.Packet) {
		p.Properties.ReceiveMaximum = 1
	})
	defer sub.Close()
	sub.Subscribe(t, "bogus/#", 1)

	pub := h.Connect(t, "puback-bogus-pub")
	defer pub.Close()

	// First QoS-1 publish — should land on the subscriber and consume
	// the single outbound slot. We deliberately do NOT PUBACK it.
	pub.Publish(t, "bogus/x", []byte("first"), 1, false)
	first, err := sub.NextRaw()
	if err != nil {
		t.Fatalf("read first PUBLISH: %v", err)
	}
	if first.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected first PUBLISH, got type=%d", first.FixedHeader.Type)
	}

	// Second QoS-1 publish — slot is held, so the broker must queue it
	// and not deliver until PUBACK for the first arrives.
	pub.Publish(t, "bogus/y", []byte("second"), 1, false)

	// Send a bogus PUBACK (packet id = 0xBEEF, unknown). Pre-fix this
	// would release the inflight slot and cause "second" to be delivered.
	if err := mqttwire.Write(sub.Conn, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Puback},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		PacketID:        0xBEEF,
	}); err != nil {
		t.Fatalf("write bogus PUBACK: %v", err)
	}

	// Give the broker time to (mis)react. The second publish should NOT
	// have been delivered because the bogus PUBACK didn't actually free
	// a slot. TryRead with a short timeout — if we read a PUBLISH here
	// the fix has regressed.
	if pk := sub.TryRead(500 * time.Millisecond); pk != nil && pk.FixedHeader.Type == packets.Publish {
		t.Fatalf("bogus PUBACK released a slot it did not hold: second PUBLISH delivered prematurely")
	}

	// Sanity: real PUBACK for the first should free the slot and allow
	// the second to flow. (This both proves the broker is still alive
	// and that we measured the right thing.)
	if err := mqttwire.Write(sub.Conn, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Puback},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		PacketID:        first.PacketID,
	}); err != nil {
		t.Fatalf("write real PUBACK: %v", err)
	}
	second := sub.TryRead(2 * time.Second)
	if second == nil || second.FixedHeader.Type != packets.Publish {
		t.Fatalf("real PUBACK didn't free the slot; second PUBLISH never arrived")
	}
}

// TestQoS2DupRetransmitDoesNotDoubleCountInbound is the regression guard
// for BUG-C: a QoS-2 PUBLISH with DUP=1 used to unconditionally Add(1)
// against the inbound flow-control counter. When the publisher's
// in-flight count was already at ReceiveMaximum, the retransmit would
// trip "Receive Maximum exceeded" and earn DISCONNECT 0x93 — even though
// [MQTT-3.3.4-9] says dup retransmits MUST NOT double-count.
//
// We set serverReceiveMaximum=1, send one QoS-2 PUBLISH (slot held
// through PUBREL/PUBCOMP), then retransmit the same PUBLISH with DUP=1.
// Pre-fix the broker DISCONNECTs us. Post-fix it re-sends PUBREC.
func TestQoS2DupRetransmitDoesNotDoubleCountInbound(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	h.Engine.SetReceiveMaxV5ForTest(1)

	pub := h.Connect(t, "qos2-dup-pub")
	defer pub.Close()

	// First QoS-2 PUBLISH — broker increments inbound counter to 1
	// (== ReceiveMaximum), inserts inbound_qos2 dedup row, replies PUBREC.
	// We deliberately do NOT send PUBREL, so the slot stays held.
	pid := pub.NextPacketID()
	if err := mqttwire.Write(pub.Conn, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 2},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		TopicName:       "qos2dup/x",
		Payload:         []byte("first"),
		PacketID:        pid,
	}); err != nil {
		t.Fatalf("write first PUBLISH: %v", err)
	}
	rec, err := pub.NextRaw()
	if err != nil {
		t.Fatalf("read first PUBREC: %v", err)
	}
	if rec.FixedHeader.Type != packets.Pubrec {
		t.Fatalf("expected PUBREC, got type=%d", rec.FixedHeader.Type)
	}

	// Retransmit with DUP=1, same packet id. Pre-fix the broker increments
	// the inbound counter to 2 (> ReceiveMaximum=1) and DISCONNECTs us
	// with 0x93. Post-fix the broker sees the dedup row, skips the
	// increment, and publishCore returns ErrQoS2Duplicate so we get a
	// repeated PUBREC.
	if err := mqttwire.Write(pub.Conn, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 2, Dup: true},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		TopicName:       "qos2dup/x",
		Payload:         []byte("first"),
		PacketID:        pid,
	}); err != nil {
		t.Fatalf("write dup PUBLISH: %v", err)
	}
	pk, err := pub.NextRaw()
	if err != nil {
		t.Fatalf("read response to DUP=1: %v", err)
	}
	if pk.FixedHeader.Type == packets.Disconnect {
		t.Fatalf("dup retransmit was double-counted: got DISCONNECT 0x%X", pk.ReasonCode)
	}
	if pk.FixedHeader.Type != packets.Pubrec {
		t.Fatalf("expected PUBREC for dup, got type=%d", pk.FixedHeader.Type)
	}
}

// TestQoS1OverflowEmitsMetric is the regression guard for OBS-1: the
// QoS≥1 over-cap drop path (mqtt_publish skipped the INSERT because the
// subscriber's deliveries queue is at MaxQueuedDeliveriesPerClient) used
// to have no counter. Fix: publishCore Inc's pgmqtt_deliveries_dropped_total
// with reason="overflow" for each over-cap subscriber.
//
// We seed the session + subscription + cap-filling deliveries directly via
// SQL so no live conn is involved on the subscriber side — that avoids
// the drain-loop racing the assertion and lets us measure the counter
// deterministically.
func TestQoS1OverflowEmitsMetric(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	h.Engine.SetMaxQueuedDeliveriesForTest(2)

	ctx := context.Background()
	const target = "overflow-sub"

	// Seed a disconnected session + subscription so mqtt_publish has
	// something to match. broker_id=NULL is fine — the overflow path
	// fires regardless of which Pod "owns" the session.
	if _, err := h.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, connected, protocol_version, clean_start)
		VALUES ($1, false, 5, false)
	`, target); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := h.Pool.Exec(ctx, `
		INSERT INTO subscriptions(client_id, topic_filter, qos)
		VALUES ($1, 'ovf/#', 1)
	`, target); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}
	// Fill the deliveries table to cap.
	for i := 0; i < 2; i++ {
		var msgID int64
		if err := h.Pool.QueryRow(ctx, `
			INSERT INTO messages(topic, payload, qos, retain) VALUES ('ovf/x', $1, 1, false)
			RETURNING id`, []byte("seed")).Scan(&msgID); err != nil {
			t.Fatalf("seed message: %v", err)
		}
		if _, err := h.Pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, state) VALUES ($1, $2, 1, 0)
		`, target, msgID); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}

	metricsBefore := testutil.ToFloat64(
		h.Engine.Metrics().DeliveriesDroppedTotal.WithLabelValues("overflow"),
	)

	// Publish QoS-1 from a separate client. mqtt_publish sees existing
	// 2 ≥ cap=2 for `target`, skips the insert, returns `target` in
	// overflow_clients, and publishCore Inc's the metric.
	pub := h.Connect(t, "overflow-pub")
	defer pub.Close()
	pub.Publish(t, "ovf/y", []byte("over-cap"), 1, false)

	// Poll briefly — Inc runs synchronously inside publishCore before
	// publish.go writes PUBACK, so the counter is already bumped by the
	// time pub.Publish returns. The small sleep covers goroutine slack.
	deadline := time.Now().Add(2 * time.Second)
	var got float64
	for time.Now().Before(deadline) {
		got = testutil.ToFloat64(
			h.Engine.Metrics().DeliveriesDroppedTotal.WithLabelValues("overflow"),
		)
		if got > metricsBefore {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got <= metricsBefore {
		t.Fatalf("pgmqtt_deliveries_dropped_total{reason=overflow} did not increment: before=%v after=%v", metricsBefore, got)
	}
}
