package mqtt

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
)

// TestPreConnectPacketSizeCapped asserts the codec rejects a framed
// remaining-length above 1 MiB before any body allocation, when the Reader
// has not had its post-CONNECT cap raised.
func TestPreConnectPacketSizeCapped(t *testing.T) {
	t.Parallel()
	// Synthesise a CONNECT-shaped fixed header with remaining length =
	// 2 MiB. We use the variable-byte-integer encoding for the length:
	// 2 MiB = 0x200000 → bytes 0x80 0x80 0x80 0x01.
	wire := []byte{
		0x10,                   // CONNECT, flags 0
		0x80, 0x80, 0x80, 0x01, // remaining length = 2,097,152 (2 MiB)
	}
	r := NewReader(bytes.NewReader(wire))
	_, err := r.Read()
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("expected ErrPacketTooLarge, got %v", err)
	}
}

// TestPostConnectCapRaised verifies SetMaxPacketSize lifts the pre-CONNECT
// cap. Set 4 MiB and a 2 MiB remaining length must NOT trip ErrPacketTooLarge
// at the cap check (it'll subsequently fail trying to read the body, but the
// allocation gate is the surface we're testing).
func TestPostConnectCapRaised(t *testing.T) {
	t.Parallel()
	wire := []byte{
		0x30,                   // PUBLISH, flags 0
		0x80, 0x80, 0x80, 0x01, // remaining length = 2 MiB
	}
	r := NewReader(bytes.NewReader(wire))
	r.SetMaxPacketSize(4 * 1024 * 1024)
	_, err := r.Read()
	// We expect a body-read failure (EOF/UnexpectedEOF) — NOT
	// ErrPacketTooLarge, because the cap was raised above the announced
	// remaining length.
	if errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("got ErrPacketTooLarge after cap raise; expected body read err")
	}
	if err == nil {
		t.Fatalf("expected read error reading body of empty stream")
	}
}

func TestPingReqRoundTrip(t *testing.T) {
	t.Parallel()
	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
	}
	wire, err := Encode(pk)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	r := NewReader(bytes.NewReader(wire))
	got, err := r.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.FixedHeader.Type != packets.Pingreq {
		t.Errorf("type = %d, want %d", got.FixedHeader.Type, packets.Pingreq)
	}
	if _, err := r.Read(); err != io.EOF {
		t.Errorf("expected EOF after single packet, got %v", err)
	}
}

func TestConnectThenPublishV5(t *testing.T) {
	t.Parallel()

	conn := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: ProtocolMQTT5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: "client-1",
		},
	}
	connWire, err := Encode(conn)
	if err != nil {
		t.Fatalf("encode connect: %v", err)
	}

	pub := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
		ProtocolVersion: ProtocolMQTT5,
		TopicName:       "house/light",
		Payload:         []byte("on"),
		PacketID:        42,
	}
	pubWire, err := Encode(pub)
	if err != nil {
		t.Fatalf("encode publish: %v", err)
	}

	stream := append([]byte(nil), connWire...)
	stream = append(stream, pubWire...)

	r := NewReader(bytes.NewReader(stream))
	gotConn, err := r.Read()
	if err != nil {
		t.Fatalf("read connect: %v", err)
	}
	if gotConn.FixedHeader.Type != packets.Connect {
		t.Fatalf("expected connect, got %d", gotConn.FixedHeader.Type)
	}
	if gotConn.ProtocolVersion != ProtocolMQTT5 {
		t.Errorf("protocol = %d, want 5", gotConn.ProtocolVersion)
	}

	gotPub, err := r.Read()
	if err != nil {
		t.Fatalf("read publish: %v", err)
	}
	if gotPub.TopicName != "house/light" {
		t.Errorf("topic = %q", gotPub.TopicName)
	}
	if string(gotPub.Payload) != "on" {
		t.Errorf("payload = %q", gotPub.Payload)
	}
	if gotPub.PacketID != 42 {
		t.Errorf("packet id = %d", gotPub.PacketID)
	}
}
