package mqtt

import (
	"bytes"
	"io"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
)

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
