package engine_test

// MQTT protocol conformance regression tests. These pin spec corners that
// audit work in v0.1.x surfaced as gaps.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// TestDisconnectReason04FiresWill: per [MQTT-3.14.4-3], a v5 client that
// sends DISCONNECT with reason code 0x04 ("Disconnect with Will Message")
// MUST cause the broker to publish that client's Will. Previous behaviour
// unconditionally cleared willTopic on graceful DISCONNECT.
func TestDisconnectReason04FiresWill(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	observer := h.Connect(t, "obs-will-04")
	defer observer.Close()
	observer.Subscribe(t, "lwt/04", 1)

	withWill := func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "lwt/04"
		p.Connect.WillPayload = []byte("fire-via-0x04")
		p.Connect.WillQos = 1
	}
	willer := h.Connect(t, "willer-04", withWill)
	// Graceful DISCONNECT with reason 0x04 — spec says: still publish the will.
	willer.Disconnect(t, func(p *packets.Packet) {
		p.ReasonCode = 0x04
	})

	pk := observer.Read(t, 3*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected will publish after DISCONNECT 0x04, got type=%d", pk.FixedHeader.Type)
	}
	if pk.TopicName != "lwt/04" || string(pk.Payload) != "fire-via-0x04" {
		t.Errorf("unexpected will: topic=%q payload=%q", pk.TopicName, pk.Payload)
	}
}

// TestDisconnectReason00SuppressesWill is a complementary control: the
// default graceful close (reason 0x00) still suppresses the will.
func TestDisconnectReason00SuppressesWill(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	observer := h.Connect(t, "obs-will-00")
	defer observer.Close()
	observer.Subscribe(t, "lwt/00", 1)

	withWill := func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "lwt/00"
		p.Connect.WillPayload = []byte("should-not-fire")
		p.Connect.WillQos = 1
	}
	willer := h.Connect(t, "willer-00", withWill)
	willer.Disconnect(t, func(p *packets.Packet) { p.ReasonCode = 0x00 })

	if pk := observer.TryRead(500 * time.Millisecond); pk != nil && pk.FixedHeader.Type == packets.Publish {
		t.Fatalf("DISCONNECT 0x00 must suppress will; got publish payload=%q", pk.Payload)
	}
}

// TestConnectMaxPacketSizeZeroRejected: per [MQTT-3.1.2-25], a CONNECT
// carrying MaximumPacketSize=0 is a Protocol Error. The broker MUST NOT
// accept the connection — accepting one would disable our outbound
// size cap entirely (c.write treats c.maxPacketSize==0 as "unlimited").
func TestConnectMaxPacketSizeZeroRejected(t *testing.T) {
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
			ClientIdentifier: "mps-zero",
			Username:         []byte("test"),
			UsernameFlag:     true,
			Password:         []byte("test"),
			PasswordFlag:     true,
		},
	}
	// mochi's encoder strips MaximumPacketSize when the value is 0, so we
	// build the CONNECT with a sentinel value and then patch the four MPS
	// bytes in the wire image before sending. Verifies the broker rejects
	// the present-and-zero case (not just the absent case).
	if err := writeConnectWithMPS(c, pk, 0); err != nil {
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
	// Per spec, 0x95 (Packet too large) or 0x82 (Protocol Error) both
	// communicate the violation. The broker uses 0x95.
	if resp.ReasonCode != 0x82 && resp.ReasonCode != 0x95 {
		t.Fatalf("expected CONNACK 0x82 or 0x95, got 0x%X", resp.ReasonCode)
	}
}

// TestWillDelayClampedWhenSessionExpiryAbsent: per [MQTT-3.1.2.11.2],
// when SessionExpiryInterval is absent the spec default is 0.
// WillDelayInterval MUST be clamped to min(WillDelay, SessionExpiry) —
// so without SessionExpiry the effective delay is 0 and the will fires
// immediately. Previously the broker only clamped when sessionExpiry
// != nil, letting a 60s WillDelay fire well after the session had
// ended.
func TestWillDelayClampedWhenSessionExpiryAbsent(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	observer := h.Connect(t, "obs-clamp")
	defer observer.Close()
	observer.Subscribe(t, "lwt/clamp", 1)

	withWill := func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "lwt/clamp"
		p.Connect.WillPayload = []byte("clamped-to-zero")
		p.Connect.WillQos = 1
		// 60s WillDelay, but NO SessionExpiryInterval (spec default 0).
		// Expected: clamp to 0, will fires immediately on ungraceful
		// disconnect.
		p.Connect.WillProperties.WillDelayInterval = 60
	}
	willer := h.Connect(t, "willer-clamp", withWill)
	willer.Kill() // ungraceful

	pk := observer.Read(t, 2*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected immediate will publish, got type=%d", pk.FixedHeader.Type)
	}
	if string(pk.Payload) != "clamped-to-zero" {
		t.Errorf("payload = %q", pk.Payload)
	}
}

// TestRetainedExpiredNotReplayed: per [MQTT-3.3.2.3.3], a retained
// message that has expired MUST NOT be delivered to new subscribers,
// even before the janitor sweep (5s default) deletes the row.
// dispatchRetainedForFilter now filters expired rows in the SELECT.
func TestRetainedExpiredNotReplayed(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	pub := h.Connect(t, "ret-exp-pub")
	defer pub.Close()

	// Publish a retained message with 1s MessageExpiryInterval.
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: true},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		TopicName:       "ret/exp",
		Payload:         []byte("expired"),
		PacketID:        1,
	}
	pk.Properties.MessageExpiryInterval = 1
	if err := mqttwire.Write(pub.Conn, pk); err != nil {
		t.Fatalf("write publish: %v", err)
	}
	pub.Read(t, 2*time.Second) // PUBACK

	// Wait for the retained row to expire but still exist in PG (janitor
	// sweep is 5s; we wait 2s).
	time.Sleep(2 * time.Second)

	var rowExpired bool
	if err := h.Pool.QueryRow(ctx,
		`SELECT expires_at IS NOT NULL AND expires_at <= now() FROM retained WHERE topic=$1`,
		"ret/exp").Scan(&rowExpired); err != nil {
		t.Fatalf("query retained: %v", err)
	}
	if !rowExpired {
		t.Fatalf("expected retained row to still exist but be expired")
	}

	sub := h.Connect(t, "ret-exp-sub")
	defer sub.Close()
	sub.Subscribe(t, "ret/exp", 1)

	if got := sub.TryRead(500 * time.Millisecond); got != nil && got.FixedHeader.Type == packets.Publish {
		t.Fatalf("expired retained must not be replayed; got payload=%q", got.Payload)
	}
}

// TestRetainedReplayDecrementsMEI: per [MQTT-3.3.2.3.3], a subscriber
// joining N seconds after a retained PUBLISH with MessageExpiryInterval=M
// receives M-N, not M. The replay path must rewrite the property,
// mirroring deliver.go's deliverOneTracked logic.
func TestRetainedReplayDecrementsMEI(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	pub := h.Connect(t, "ret-mei-pub")
	defer pub.Close()

	const initialMEI uint32 = 60
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0, Retain: true},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		TopicName:       "ret/mei",
		Payload:         []byte("rewrite-me"),
	}
	pk.Properties.MessageExpiryInterval = initialMEI
	if err := mqttwire.Write(pub.Conn, pk); err != nil {
		t.Fatalf("write publish: %v", err)
	}

	// Sleep ~2s so remaining MEI is materially below initial.
	time.Sleep(2 * time.Second)

	sub := h.Connect(t, "ret-mei-sub")
	defer sub.Close()
	sub.Subscribe(t, "ret/mei", 0)

	got := sub.Read(t, 2*time.Second)
	if got.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected retained publish, got type=%d", got.FixedHeader.Type)
	}
	if got.Properties.MessageExpiryInterval == 0 {
		t.Fatalf("expected MEI to be set, got 0")
	}
	if got.Properties.MessageExpiryInterval >= initialMEI {
		t.Errorf("MEI not decremented: got %d, want < %d",
			got.Properties.MessageExpiryInterval, initialMEI)
	}
}

// TestDisconnectInvalidSEIncreaseSends82: a CONNECT with SessionExpiry=0
// (or absent) followed by DISCONNECT carrying SessionExpiry>0 is a
// Protocol Error per [MQTT-3.14.2.2.2]. Server MUST send DISCONNECT
// 0x82 before tearing the conn down — previously the broker silently
// dropped the update with no signal to the client.
func TestDisconnectInvalidSEIncreaseSends82(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	c, err := net.DialTimeout("tcp", h.TCPAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// CONNECT with no SessionExpiry property (defaults to 0).
	connect := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: "se-bad-1",
			Username:         []byte("test"),
			UsernameFlag:     true,
			Password:         []byte("test"),
			PasswordFlag:     true,
		},
	}
	if err := mqttwire.Write(c, connect); err != nil {
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
	if resp.FixedHeader.Type != packets.Connack || resp.ReasonCode != 0 {
		t.Fatalf("connack: type=%d code=0x%X", resp.FixedHeader.Type, resp.ReasonCode)
	}

	// DISCONNECT with SE=10 — invalid increase from 0.
	disc := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Disconnect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
	}
	disc.Properties.SessionExpiryInterval = 10
	disc.Properties.SessionExpiryIntervalFlag = true
	if err := mqttwire.Write(c, disc); err != nil {
		t.Fatalf("write disconnect: %v", err)
	}

	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	got, err := r.Read()
	if err != nil {
		t.Fatalf("expected DISCONNECT 0x82, got read err: %v", err)
	}
	if got.FixedHeader.Type != packets.Disconnect {
		t.Fatalf("expected DISCONNECT, got type=%d", got.FixedHeader.Type)
	}
	if got.ReasonCode != 0x82 {
		t.Errorf("expected reason 0x82, got 0x%X", got.ReasonCode)
	}
}

// TestPublishWithSubscriptionIdentifierRejected: [MQTT-3.3.4-6] forbids
// clients from including SubscriptionIdentifier in PUBLISH. The server
// MUST disconnect with reason 0x82 (Protocol Error) instead of
// persisting and forwarding the client-supplied SubID.
func TestPublishWithSubscriptionIdentifierRejected(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	c := h.Connect(t, "pub-with-subid")
	defer c.Close()

	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		TopicName:       "pub/subid",
		Payload:         []byte("nope"),
	}
	pk.Properties.SubscriptionIdentifier = []int{42}
	if err := mqttwire.Write(c.Conn, pk); err != nil {
		t.Fatalf("write publish: %v", err)
	}

	if err := c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	got, err := c.NextRaw()
	if err != nil {
		t.Fatalf("expected DISCONNECT 0x82, got read err: %v", err)
	}
	if got.FixedHeader.Type != packets.Disconnect {
		t.Fatalf("expected DISCONNECT, got type=%d", got.FixedHeader.Type)
	}
	if got.ReasonCode != 0x82 {
		t.Errorf("expected reason 0x82, got 0x%X", got.ReasonCode)
	}
}

// TestSubscribeEmptyFiltersRejected: [MQTT-3.8.3-2] requires SUBSCRIBE
// to carry at least one Topic Filter. The server MUST treat a
// zero-filter SUBSCRIBE as Protocol Error (0x82). Previously the
// broker emitted an empty SUBACK and proceeded as if successful.
func TestSubscribeEmptyFiltersRejected(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	c := h.Connect(t, "sub-empty")
	defer c.Close()

	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		PacketID:        1,
		Filters:         packets.Subscriptions{},
	}
	if err := mqttwire.Write(c.Conn, pk); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	if err := c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	got, err := c.NextRaw()
	if err != nil {
		t.Fatalf("expected DISCONNECT 0x82, got read err: %v", err)
	}
	if got.FixedHeader.Type != packets.Disconnect {
		t.Fatalf("expected DISCONNECT, got type=%d", got.FixedHeader.Type)
	}
	if got.ReasonCode != 0x82 {
		t.Errorf("expected reason 0x82, got 0x%X", got.ReasonCode)
	}
}

// TestMqttRetainedExpiresAtIsStable: migration 0004 created
// mqtt_retained_expires_at with IMMUTABLE volatility, but the body
// calls now() — so the function depends on transaction time and the
// IMMUTABLE contract is a lie. The planner is free to fold IMMUTABLE
// function calls at plan time, which could materialise the wrong
// expiry timestamp. Migration 0016 redeclares it STABLE.
func TestMqttRetainedExpiresAtIsStable(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	var volatility string
	// provolatile is char(1); cast to text so pgx scans into string.
	if err := h.Pool.QueryRow(ctx,
		`SELECT provolatile::text FROM pg_proc WHERE proname='mqtt_retained_expires_at'`).
		Scan(&volatility); err != nil {
		t.Fatalf("query volatility: %v", err)
	}
	// 's' = STABLE, 'i' = IMMUTABLE, 'v' = VOLATILE.
	if volatility != "s" {
		t.Errorf("mqtt_retained_expires_at volatility = %q, want 's' (STABLE)", volatility)
	}
}

// writeConnectWithMPS encodes a CONNECT with MaximumPacketSize=1 (so
// mochi's encoder emits the property), then patches the 4-byte value in
// place. Used to construct a "MaximumPacketSize present, value 0"
// CONNECT, which mochi's encoder cannot produce directly.
func writeConnectWithMPS(w net.Conn, pk *packets.Packet, mps uint32) error {
	pk.Properties.MaximumPacketSize = 1
	buf, err := mqttwire.Encode(pk)
	if err != nil {
		return err
	}
	for i := 0; i+5 <= len(buf); i++ {
		// PropMaximumPacketSize is 0x27 in v5 properties; the sentinel
		// value 0x00 0x00 0x00 0x01 anchors a safe match against the
		// rest of the byte stream.
		if buf[i] == 0x27 && buf[i+1] == 0 && buf[i+2] == 0 && buf[i+3] == 0 && buf[i+4] == 1 {
			buf[i+1] = byte(mps >> 24)
			buf[i+2] = byte(mps >> 16)
			buf[i+3] = byte(mps >> 8)
			buf[i+4] = byte(mps)
			_, err := w.Write(buf)
			return err
		}
	}
	_, err = w.Write(buf)
	return err
}
