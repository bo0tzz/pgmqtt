package engine_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
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

	// Persistent session subscribes then disconnects.
	withClean := func(p *packets.Packet) { p.Connect.Clean = false }
	sub1 := h.Connect(t, "sub-resume", withClean)
	sub1.Subscribe(t, "rx/#", 1)
	sub1.Close()

	pub := h.Connect(t, "pub-resume")
	pub.Publish(t, "rx/foo", []byte("queued"), 1, false)
	pub.Close()

	// Reconnect — should drain the queued QoS-1 message.
	sub2 := h.Connect(t, "sub-resume", withClean)
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

	clean := func(p *packets.Packet) { p.Connect.Clean = false }
	c := h.Connect(t, "shut-1", clean)
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
