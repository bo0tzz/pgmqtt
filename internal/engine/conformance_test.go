package engine_test

// MQTT protocol conformance regression tests. These pin spec corners that
// audit work in v0.1.x surfaced as gaps.

import (
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
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
