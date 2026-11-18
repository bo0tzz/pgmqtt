package engine_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
)

func persistent(p *packets.Packet) {
	p.Connect.Clean = false
	p.Properties.SessionExpiryInterval = 3600
	p.Properties.SessionExpiryIntervalFlag = true
}

func puback(t *testing.T, c *enginetest.TestClient, pk *packets.Packet) {
	t.Helper()
	if err := c.WritePacket(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback},
		PacketID:    pk.PacketID,
	}); err != nil {
		t.Fatalf("puback: %v", err)
	}
}

// nextPublish reads until a PUBLISH arrives or the timeout passes. Like
// TestClient.TryRead, it acknowledges QoS 1 on the caller's behalf.
func nextPublish(c *enginetest.TestClient, timeout time.Duration) *packets.Packet {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pk := c.TryRead(time.Until(deadline))
		if pk == nil {
			return nil
		}
		if pk.FixedHeader.Type == packets.Publish {
			return pk
		}
	}
	return nil
}

// nextUnacked is nextPublish without the acknowledgement, for tests about
// what is in flight.
func nextUnacked(c *enginetest.TestClient, timeout time.Duration) *packets.Packet {
	_ = c.Conn.SetReadDeadline(time.Now().Add(timeout))
	defer c.Conn.SetReadDeadline(time.Time{})
	for {
		pk, err := c.NextRaw()
		if err != nil {
			return nil
		}
		if pk.FixedHeader.Type == packets.Publish {
			return &pk
		}
	}
}

// stopReading makes c a client whose socket fills up: a tiny receive
// buffer, and nothing ever read from it.
func stopReading(t *testing.T, c *enginetest.TestClient) {
	t.Helper()
	if err := c.Conn.(*net.TCPConn).SetReadBuffer(4096); err != nil {
		t.Fatalf("shrink read buffer: %v", err)
	}
}

// TestStalledSubscriberHoldsUpNobodyElse: one client that stops reading
// used to stall delivery to every other client on its broker, about ten
// seconds per message addressed to it, because all deliveries were
// written from one goroutine and a failed write did not close anything.
func TestStalledSubscriberHoldsUpNobodyElse(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	stalled := h.Connect(t, "stalled")
	defer stalled.Close()
	stalled.Subscribe(t, "big/#", 0)
	stopReading(t, stalled)

	healthy := h.Connect(t, "healthy")
	defer healthy.Close()
	healthy.Subscribe(t, "big/#", 0)

	pub := h.Connect(t, "pub")
	defer pub.Close()
	const n = 24
	payload := make([]byte, 512*1024)
	start := time.Now()
	for i := 0; i < n; i++ {
		pub.Publish(t, "big/x", payload, 0, false)
	}
	for i := 0; i < n; i++ {
		if pk := nextPublish(healthy, 5*time.Second); pk == nil {
			t.Fatalf("healthy subscriber got %d of %d messages in %s while another client was not reading",
				i, n, time.Since(start).Round(time.Millisecond))
		}
	}
}

// TestClientThatStopsReadingIsDisconnected: a write that cannot complete
// closes the connection, and that is an ordinary abnormal end for the
// session (its will is published).
func TestClientThatStopsReadingIsDisconnected(t *testing.T) {
	t.Cleanup(engine.SetWriteTimeoutForTest(300 * time.Millisecond))
	mh := enginetest.NewMultiHarness(t, 1, func(e *engine.Engine) func() {
		e.SetNotifier(engine.NewInProcessNotifier(e))
		return nil
	})
	pod := mh.Pods[0]

	stalled := pod.Connect(t, "stalled", withRetainedWill("stalled/state"), withExpiry(3600))
	defer stalled.Close()
	stalled.Subscribe(t, "big/#", 0)
	stopReading(t, stalled)

	pub := pod.Connect(t, "pub")
	defer pub.Close()
	payload := make([]byte, 512*1024)
	for i := 0; i < 24; i++ {
		pub.Publish(t, "big/x", payload, 0, false)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		s := snapshotSession(t, mh, "stalled")
		if !s.connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client that stopped reading is still connected: %+v", s)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, ok := pod.Engine.ConnFor("stalled"); ok {
		t.Errorf("broker still holds a Conn for the disconnected client")
	}
	if got := retainedPayload(t, mh, "stalled/state"); got != "offline" {
		t.Errorf("will of the disconnected client not published: %q", got)
	}
}

// TestDisconnectNeverWaitsOnTheClient: closing a connection from outside
// its own goroutines (a quota kick from another client's publish, from
// the listener, or the shutdown loop) must return at once even when the
// client is not reading.
func TestDisconnectNeverWaitsOnTheClient(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	stalled := h.Connect(t, "stalled")
	defer stalled.Close()
	stalled.Subscribe(t, "big/#", 0)
	stopReading(t, stalled)
	pub := h.Connect(t, "pub")
	defer pub.Close()
	payload := make([]byte, 512*1024)
	for i := 0; i < 24; i++ {
		pub.Publish(t, "big/x", payload, 0, false)
	}
	time.Sleep(300 * time.Millisecond) // let the sender run into the full socket

	conn, ok := h.Engine.ConnFor("stalled")
	if !ok {
		t.Fatal("no Conn for the stalled client")
	}
	start := time.Now()
	conn.Disconnect(0x97)
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("Disconnect blocked for %s on a client that was not reading", d)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := h.Engine.ConnFor("stalled"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection still open 5s after Disconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestResumedBacklogPrecedesLiveMessages: a session that resumes gets
// what was queued while it was away before anything published after it
// came back. Live deliveries used to be written straight to any
// registered connection, overtaking the backlog still being drained.
func TestResumedBacklogPrecedesLiveMessages(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	const backlog = 300

	sub := h.Connect(t, "ord-sub", persistent)
	sub.Subscribe(t, "ord/#", 1)
	sub.Close()
	time.Sleep(200 * time.Millisecond)

	pub := h.Connect(t, "ord-pub")
	defer pub.Close()
	for i := 0; i < backlog; i++ {
		pub.Publish(t, "ord/state", []byte(fmt.Sprintf("backlog-%04d", i)), 1, false)
	}
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; !stop.Load() && i < 500; i++ {
			pub.Publish(t, "ord/state", []byte(fmt.Sprintf("live-%04d", i)), 1, false)
		}
	}()
	defer func() { stop.Store(true); <-done }()

	sub2 := h.Connect(t, "ord-sub", persistent)
	defer sub2.Close()
	next, lives := 0, 0
	for next < backlog {
		pk := nextPublish(sub2, 5*time.Second)
		if pk == nil {
			t.Fatalf("got %d of %d backlog messages", next, backlog)
		}
		p := string(pk.Payload)
		if strings.HasPrefix(p, "live-") {
			lives++
			continue
		}
		if want := fmt.Sprintf("backlog-%04d", next); p != want {
			t.Fatalf("backlog out of order: got %q, want %q", p, want)
		}
		next++
	}
	if lives > 0 {
		t.Errorf("%d live messages were delivered before the backlog had finished", lives)
	}
}

// TestNothingPrecedesConnack: the first packet a client receives is its
// CONNACK [MQTT-3.2.0-1], including when it reconnects under a client id
// that deliveries are streaming to.
func TestNothingPrecedesConnack(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	sub := h.Connect(t, "ck-sub", persistent)
	sub.Subscribe(t, "ck/#", 0)
	pub := h.Connect(t, "ck-pub")
	defer pub.Close()
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			pub.Publish(t, "ck/x", []byte("tick"), 0, false)
		}
	}()
	defer func() { stop.Store(true); <-done }()

	// Connect itself fails the test if the first packet is not CONNACK.
	for i := 0; i < 60; i++ {
		next := h.Connect(t, "ck-sub", persistent)
		sub.Close()
		sub = next
	}
	sub.Close()
}

// TestReceiveMaximumHoldsOnRetainedReplayAndResume: the client's Receive
// Maximum bounds what is in flight to it however the message got there.
// Retained replay ignored it, and a resumed session's in-flight messages
// held no slots, so their acknowledgements widened the window.
func TestReceiveMaximumHoldsOnRetainedReplayAndResume(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	const window = 2

	pub := h.Connect(t, "rm-pub")
	defer pub.Close()
	for i := 0; i < 5; i++ {
		pub.Publish(t, fmt.Sprintf("rm/%d", i), []byte("retained"), 1, true)
	}

	withWindow := func(p *packets.Packet) {
		persistent(p)
		p.Properties.ReceiveMaximum = window
	}
	sub := h.Connect(t, "rm-sub", withWindow)
	sub.Subscribe(t, "rm/#", 1)
	var unacked []*packets.Packet
	for {
		pk := nextUnacked(sub, 700*time.Millisecond)
		if pk == nil {
			break
		}
		if !pk.FixedHeader.Retain {
			t.Errorf("retained replay arrived without RETAIN: %s", pk.TopicName)
		}
		unacked = append(unacked, pk)
	}
	if len(unacked) != window {
		t.Fatalf("retained replay: %d messages in flight with Receive Maximum %d", len(unacked), window)
	}

	// Drop the connection with the window full and come back.
	sub.Kill()
	time.Sleep(200 * time.Millisecond)
	sub = h.Connect(t, "rm-sub", withWindow)
	defer sub.Close()
	var resent []*packets.Packet
	for {
		pk := nextUnacked(sub, 700*time.Millisecond)
		if pk == nil {
			break
		}
		resent = append(resent, pk)
	}
	if len(resent) != window {
		t.Fatalf("after resume: %d messages in flight with Receive Maximum %d", len(resent), window)
	}
	for _, pk := range resent {
		if !pk.FixedHeader.Dup {
			t.Errorf("resent in-flight message without DUP: %s", pk.TopicName)
		}
	}

	// Acknowledging the resumed messages must not widen the window.
	seen := window
	for _, pk := range resent {
		puback(t, sub, pk)
	}
	for seen < 5 {
		var wave []*packets.Packet
		for {
			pk := nextUnacked(sub, 500*time.Millisecond)
			if pk == nil {
				break
			}
			wave = append(wave, pk)
		}
		if len(wave) == 0 {
			break
		}
		if len(wave) > window {
			t.Fatalf("%d messages in flight with Receive Maximum %d after acknowledging resumed ones", len(wave), window)
		}
		seen += len(wave)
		for _, pk := range wave {
			puback(t, sub, pk)
		}
	}
	if seen != 5 {
		t.Errorf("received %d of 5 retained messages in total", seen)
	}
}

// TestOversizeMessageIsDroppedNotStuck: a message larger than the
// client's Maximum Packet Size is discarded for that client
// [MQTT-3.1.2-25]. The size check for QoS>0 never ran, so such a message
// was marked in flight, could not be written, and then blocked everything
// behind it on every resume; and retained replay stopped at the first one.
func TestOversizeMessageIsDroppedNotStuck(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	big := make([]byte, 2000)

	small := func(p *packets.Packet) {
		persistent(p)
		p.Properties.MaximumPacketSize = 300
	}
	pub := h.Connect(t, "os-pub")
	defer pub.Close()
	pub.Publish(t, "os/retained/big", big, 0, true)
	pub.Publish(t, "os/retained/a", []byte("a"), 0, true)
	pub.Publish(t, "os/retained/b", []byte("b"), 0, true)
	// QoS 0 is not acknowledged: wait until the broker has stored all
	// three, so the subscription below is not racing their publication.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		var n int
		if err := h.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM retained WHERE topic LIKE 'os/retained/%'`).Scan(&n); err != nil {
			t.Fatalf("count retained: %v", err)
		}
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 3 retained messages stored", n)
		}
	}

	sub := h.Connect(t, "os-sub", small)
	defer sub.Close()
	sub.Subscribe(t, "os/retained/#", 0)
	got := map[string]bool{}
	for len(got) < 2 {
		pk := nextPublish(sub, 2*time.Second)
		if pk == nil {
			break
		}
		got[pk.TopicName] = true
	}
	if !got["os/retained/a"] || !got["os/retained/b"] || got["os/retained/big"] {
		t.Errorf("retained replay past an oversize message: got %v", got)
	}

	sub.Subscribe(t, "os/live", 1)
	pub.Publish(t, "os/live", big, 1, false)
	pub.Publish(t, "os/live", []byte("fits"), 1, false)
	pk := nextPublish(sub, 3*time.Second)
	if pk == nil || string(pk.Payload) != "fits" {
		t.Fatalf("message behind an oversize one was not delivered: %+v", pk)
	}
	time.Sleep(200 * time.Millisecond)
	var left int
	if err := h.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM deliveries WHERE client_id='os-sub'`).Scan(&left); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if left != 0 {
		t.Errorf("%d delivery rows left behind for the oversize message", left)
	}
}

// TestTopicAliasAnnouncedByItsFirstPublish: the PUBLISH that introduces
// an alias carries the topic name with it. The alias used to be
// registered before the send was certain, so a message that then had to
// wait for a Receive Maximum slot went out later with the alias alone.
func TestTopicAliasAnnouncedByItsFirstPublish(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	aliasing := func(p *packets.Packet) {
		persistent(p)
		p.Properties.ReceiveMaximum = 1
		p.Properties.TopicAliasMaximum = 8
		p.Properties.TopicAliasFlag = true
	}
	sub := h.Connect(t, "al-sub", aliasing)
	sub.Subscribe(t, "al/#", 1)
	sub.Close()
	time.Sleep(200 * time.Millisecond)

	// Both queued before the client is back, so the sender meets them
	// together with room for only the first.
	pub := h.Connect(t, "al-pub")
	defer pub.Close()
	pub.Publish(t, "al/x", []byte("1"), 1, false)
	pub.Publish(t, "al/y", []byte("2"), 1, false)

	sub = h.Connect(t, "al-sub", aliasing)
	defer sub.Close()
	first := nextUnacked(sub, 3*time.Second)
	if first == nil || first.TopicName != "al/x" {
		t.Fatalf("first publish: %+v", first)
	}
	if extra := nextUnacked(sub, 300*time.Millisecond); extra != nil {
		t.Fatalf("second message sent with the window full: %+v", extra)
	}
	puback(t, sub, first)
	second := nextUnacked(sub, 3*time.Second)
	if second == nil {
		t.Fatal("second publish never arrived")
	}
	if second.TopicName != "al/y" {
		t.Errorf("first PUBLISH for al/y carried topic %q with alias %d: the alias was never announced",
			second.TopicName, second.Properties.TopicAlias)
	}
}

// TestQueuedQoS0IsSentWhenSenderWakes: a QoS 0 delivery whose NOTIFY was
// missed (the listener was reconnecting) is picked up the next time the
// connection's sender is woken. The wake-up used to look for QoS>0 only,
// so these rows sat until the session's next resume.
func TestQueuedQoS0IsSentWhenSenderWakes(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	sub := h.Connect(t, "q0-sub")
	defer sub.Close()
	sub.Subscribe(t, "q0/#", 0)

	var msgID int64
	if err := h.Pool.QueryRow(ctx, `
		INSERT INTO messages(topic, payload, qos, retain) VALUES ('q0/x', 'missed', 0, false)
		RETURNING id`).Scan(&msgID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	if _, err := h.Pool.Exec(ctx,
		`INSERT INTO deliveries(client_id, message_id, qos, state) VALUES ('q0-sub', $1, 0, 0)`, msgID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	h.Engine.KickAllDrains()
	pk := nextPublish(sub, 3*time.Second)
	if pk == nil || string(pk.Payload) != "missed" {
		t.Fatalf("queued QoS 0 delivery not sent on wake-up: %+v", pk)
	}
}
