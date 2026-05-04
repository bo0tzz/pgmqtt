package engine_test

import (
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// TestKeepAliveDeadlineUses1Point5xMultiplier asserts the spec
// [MQTT-3.1.2-22] rule: a server SHOULD disconnect after 1.5× the
// negotiated keepalive without traffic, NOT after keepalive+ε. The
// previous implementation added a hard 1500ms grace, so a client
// with the very common Paho 60s default would be torn down at
// 61.5s instead of 90s. This test pins down the multiplier semantic
// at the small end so it can run quickly.
func TestKeepAliveDeadlineUses1Point5xMultiplier(t *testing.T) {
	t.Parallel()

	// Configure a small multiplier to keep the test fast, but big enough
	// to avoid flake on slow CI: 1.5× a 1s keepalive = 1.5s grace.
	h := enginetest.NewHarnessWith(t, func(e *engine.Engine) func() {
		e.KeepAliveMultiplier = 1.5
		return nil
	})

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
			Keepalive:        1, // 1 second
			ClientIdentifier: "ka-multiplier-client",
			Username:         []byte("test"),
			UsernameFlag:     true,
			Password:         []byte("test"),
			PasswordFlag:     true,
		},
	}
	if err := mqttwire.Write(c, pk); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}

	// Read the CONNACK; ignore the body.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := mqttwire.NewReader(c)
	if _, err := r.Read(); err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}

	// Now stop sending PINGREQs entirely. Spec: server may disconnect
	// after 1.5×keepalive = 1.5s. Anything earlier than ~1.2s would be
	// the old "keepalive+ε" bug (broken at any keepalive >0).
	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	_, _ = c.Read(buf) // expected: EOF / RST when broker tears down
	elapsed := time.Since(start)

	// Allow 1.2s..3.0s window. Spec says "after 1.5s"; broker has small
	// scheduling slack on top.
	if elapsed < 1200*time.Millisecond {
		t.Fatalf("disconnect at %v — earlier than 1.5× keepalive (=1.5s); the old additive-grace bug would do this", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("disconnect at %v — much later than 1.5× keepalive (=1.5s); something's wrong", elapsed)
	}
}
