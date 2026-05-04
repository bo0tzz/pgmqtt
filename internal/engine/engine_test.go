package engine_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochi-mqtt/server/v2/packets"

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
