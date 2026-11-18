package engine_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
)

func v311(p *packets.Packet) { p.ProtocolVersion = 4 }

// TestV311KeepaliveIsNotOverridden: the server keepalive policy can only
// be applied to a client that can be told about it, which is v5 (Server
// Keep Alive in CONNACK). A v3.1.1 client asking for no keepalive, or a
// longer one than the policy, used to be cut off on the server's
// schedule while pinging on its own, over and over.
func TestV311KeepaliveIsNotOverridden(t *testing.T) {
	t.Parallel()
	// 60s (the default applied to keepalive=0) x 0.005 = 300ms.
	h := enginetest.NewHarnessWith(t, func(e *engine.Engine) func() {
		e.KeepAliveMultiplier = 0.005
		return nil
	})

	none := h.Connect(t, "v311-no-keepalive", v311, func(p *packets.Packet) { p.Connect.Keepalive = 0 })
	defer none.Close()
	long := h.Connect(t, "v311-long-keepalive", v311, func(p *packets.Packet) { p.Connect.Keepalive = 600 })
	defer long.Close()
	time.Sleep(900 * time.Millisecond)

	for name, c := range map[string]*enginetest.TestClient{"keepalive 0": none, "keepalive 600": long} {
		if err := c.WritePacket(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}}); err != nil {
			t.Errorf("v3.1.1 client with %s was disconnected: %v", name, err)
			continue
		}
		_ = c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if pk, err := c.NextRaw(); err != nil || pk.FixedHeader.Type != packets.Pingresp {
			t.Errorf("v3.1.1 client with %s was disconnected on the server's keepalive: %v", name, err)
		}
	}
}

// TestConnectAcceptsPresentButEmptyFields: an empty will payload and an
// empty password are both legal (the spec asks for the fields to be
// present). The codec library rejects them, and the broker then closed
// the connection with no CONNACK, so the client retried forever.
func TestConnectAcceptsPresentButEmptyFields(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	c := h.Connect(t, "empty-will-payload", func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = "state/empty"
		p.Connect.WillPayload = nil
		p.Connect.WillRetain = true
	})
	c.Close()

	rc, pk := h.ConnectExpectReject(t, "empty-password", func(p *packets.Packet) {
		p.Connect.Password = nil
		p.Connect.PasswordFlag = true
	})
	defer rc.Close()
	if pk == nil || pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0x86 {
		t.Errorf("empty password: want CONNACK 0x86 (bad credentials), got %+v", pk)
	}
}

// TestMaxPacketSizeZeroMeansNoServerCap: PGMQTT_MAX_PACKET_SIZE=0 is
// documented as "no cap". It left the reader on its pre-CONNECT limit
// of 1 MiB.
func TestMaxPacketSizeZeroMeansNoServerCap(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarnessWith(t, func(e *engine.Engine) func() {
		e.SetMaxPacketSizeForTest(0)
		return nil
	})
	pub := h.Connect(t, "big-pub")
	defer pub.Close()
	pub.Publish(t, "big/one", make([]byte, 2<<20), 1, false)
}

// TestFullBrokerClosesWithoutConnack: being at the connection cap is
// temporary, so the refusal must be one clients retry. A CONNACK with a
// failure code is treated as final by MQTT.js and others.
func TestFullBrokerClosesWithoutConnack(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarnessWith(t, func(e *engine.Engine) func() {
		e.SetMaxConnectionsForTest(1)
		return nil
	})
	first := h.Connect(t, "only-one")
	defer first.Close()

	c, err := net.DialTimeout("tcp", h.TCPAddr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	n, err := c.Read(buf)
	if n != 0 || err == nil {
		t.Fatalf("full broker sent % x (err=%v); want the connection closed with nothing written", buf[:n], err)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("full broker left the connection open")
	}
}

// TestInboundQoS2CountSurvivesReconnect: QoS 2 publishes a client sent
// and has not yet released still count against the server's Receive
// Maximum after it reconnects. The per-connection counter restarted at
// zero, the PUBRELs took it negative, and the next PUBLISH was refused
// with 0x93 (Receive Maximum exceeded).
func TestInboundQoS2CountSurvivesReconnect(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	read := func(c *enginetest.TestClient) *packets.Packet {
		_ = c.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		pk, err := c.NextRaw()
		if err != nil {
			return nil
		}
		return &pk
	}
	pub := h.Connect(t, "q2-pub", persistent)
	for pid := uint16(1); pid <= 2; pid++ {
		if err := pub.WritePacket(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2},
			TopicName:   "q2/x", Payload: []byte("x"), PacketID: pid,
		}); err != nil {
			t.Fatal(err)
		}
		if rec := read(pub); rec == nil || rec.FixedHeader.Type != packets.Pubrec {
			t.Fatalf("no PUBREC for %d: %+v", pid, rec)
		}
	}
	pub.Kill()
	time.Sleep(300 * time.Millisecond)

	pub = h.Connect(t, "q2-pub", persistent)
	defer pub.Close()
	for pid := uint16(1); pid <= 2; pid++ {
		if err := pub.WritePacket(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
			PacketID:    pid,
		}); err != nil {
			t.Fatal(err)
		}
		if comp := read(pub); comp == nil || comp.FixedHeader.Type != packets.Pubcomp {
			t.Fatalf("no PUBCOMP for %d: %+v", pid, comp)
		}
	}
	if err := pub.WritePacket(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "q2/y", Payload: []byte("y"), PacketID: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if resp := read(pub); resp == nil || resp.FixedHeader.Type != packets.Puback {
		t.Fatalf("publish after releasing resumed QoS 2 messages: want PUBACK, got %+v", resp)
	}
}

// TestLargeMessageExpiryIntervalIsAccepted: Message Expiry Interval is a
// uint32; the SQL cast it to a 32-bit signed int and the publish failed
// for the top half of the range.
func TestLargeMessageExpiryIntervalIsAccepted(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	sub := h.Connect(t, "mei-sub")
	defer sub.Close()
	sub.Subscribe(t, "mei/#", 1)
	pub := h.Connect(t, "mei-pub")
	defer pub.Close()

	for i, retain := range []bool{false, true} {
		if err := pub.WritePacket(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: retain},
			TopicName:   "mei/x", Payload: []byte("far future"), PacketID: uint16(i + 1),
			Properties: packets.Properties{MessageExpiryInterval: 0xFFFFFFFF},
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		ack := pub.TryRead(3 * time.Second)
		if ack == nil || ack.FixedHeader.Type != packets.Puback || ack.ReasonCode >= 0x80 {
			t.Fatalf("publish with retain=%v and the maximum expiry interval: %+v", retain, ack)
		}
		if pk := nextPublish(sub, 3*time.Second); pk == nil {
			t.Fatalf("message with the maximum expiry interval was not delivered (retain=%v)", retain)
		}
	}
}

// TestQueueCapIsPerQoSClass: queued QoS 0 rows waiting for a connected
// client's sender used to count towards the cap that only QoS>0 is
// subject to, so a burst of QoS 0 could get the next QoS 1 message
// dropped and the client disconnected; and QoS 0 itself had no bound.
func TestQueueCapIsPerQoSClass(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarnessWith(t, func(e *engine.Engine) func() {
		e.SetMaxQueuedDeliveriesForTest(3)
		return nil
	})
	ctx := context.Background()
	queued := func(clientID string, qos int) int {
		var n int
		if err := h.Pool.QueryRow(ctx,
			`SELECT count(*) FROM deliveries WHERE client_id=$1 AND qos=$2`, clientID, qos).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}
	// Rows as they are between a publish and the sender getting to them.
	seedQoS0 := func(clientID string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := h.Pool.Exec(ctx, `
				WITH m AS (INSERT INTO messages(topic, payload, qos, retain)
				           VALUES ('cap/seed', 'x', 0, false) RETURNING id)
				INSERT INTO deliveries(client_id, message_id, qos, state) SELECT $1, id, 0, 0 FROM m`,
				clientID); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
	}

	pub := h.Connect(t, "cap-pub")
	defer pub.Close()

	// A full QoS 0 backlog does not stand in the way of QoS 1.
	mixed := h.Connect(t, "cap-mixed", persistent)
	mixed.Subscribe(t, "cap/q1", 1)
	mixed.Close()
	time.Sleep(200 * time.Millisecond)
	seedQoS0("cap-mixed", 3)
	pub.Publish(t, "cap/q1", []byte("must queue"), 1, false)
	if n := queued("cap-mixed", 1); n != 1 {
		t.Errorf("QoS 1 delivery dropped because of queued QoS 0 rows (%d queued)", n)
	}

	// QoS 0 has a bound of its own, and hitting it disconnects the client.
	slow := h.Connect(t, "cap-slow")
	defer slow.Close()
	slow.Subscribe(t, "cap/q0", 0)
	stopReading(t, slow)
	seedQoS0("cap-slow", 3)
	pub.Publish(t, "cap/q0", []byte("one too many"), 0, false)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := h.Engine.ConnFor("cap-slow"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client with a full QoS 0 queue was not disconnected (%d rows queued)", queued("cap-slow", 0))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEmptyPropertiesAreNotStored: a message with no properties used to
// be stored with a few hundred bytes of zero-valued ones.
func TestEmptyPropertiesAreNotStored(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	pub := h.Connect(t, "noprops-pub", v311)
	defer pub.Close()
	pub.Publish(t, "noprops/x", []byte("plain"), 1, false)
	var stored *string
	if err := h.Pool.QueryRow(context.Background(),
		`SELECT properties::text FROM messages WHERE topic='noprops/x'`).Scan(&stored); err != nil {
		t.Fatalf("read message: %v", err)
	}
	if stored != nil {
		t.Errorf("message with no properties stored %d bytes of them", len(*stored))
	}
}
