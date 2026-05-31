package engine_test

// MQTT protocol conformance regression tests. These pin spec corners that
// audit work in v0.1.x surfaced as gaps.

import (
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
