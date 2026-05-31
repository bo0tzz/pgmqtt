package listener_test

import (
	"context"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// TestPostReconnectDrainsQueuedDeliveries pins BUG-Y: cross-broker
// NOTIFYs that fire while pod A's listener is mid-reconnect land on a
// deaf channel. The publish writes a `deliveries` row (state=0) but
// nothing wakes pod A's per-conn drain loop, so a QoS-1 subscriber
// that doesn't publish/ack frequently has its queued row sit
// indefinitely. The fix kicks every local Conn's drain loop after a
// successful reconnect.
//
// Setup: 2 pods, QoS-1 subscriber on pod A. Wedge pod A's listener into
// a sustained reconnect loop by swapping its URL to an unreachable
// address and forcing a wait error. Publish from pod B while pod A's
// channel is dead. Restore the URL and assert the queued message
// reaches the subscriber — orders of magnitude faster than waiting
// for any other drain trigger, because the passive subscriber emits
// no PUBACK that would otherwise wake the loop.
func TestPostReconnectDrainsQueuedDeliveries(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	listeners := make([]*listener.Listener, len(mh.Pods))
	for i, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		listeners[i] = l
		t.Cleanup(l.Stop)
		mtx := metrics.New()
		l.SetMetrics(mtx)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	// Passive QoS-1 subscriber on pod 0. It only ever reads from the
	// socket — no PUBLISH, no PUBACK after the initial sanity message,
	// so nothing else can wake the drain loop.
	sub := mh.Pods[0].Connect(t, "drain-sub")
	defer sub.Close()
	sub.Subscribe(t, "drain/#", 1)

	// Publisher on pod 1.
	pub := mh.Pods[1].Connect(t, "drain-pub")
	defer pub.Close()

	// Sanity: cross-broker publish works pre-disruption.
	pub.Publish(t, "drain/pre", []byte("pre"), 1, false)
	pk := sub.Read(t, 10*time.Second)
	if pk.FixedHeader.Type != packets.Publish || string(pk.Payload) != "pre" {
		t.Fatalf("pre-kill: type=%d payload=%q", pk.FixedHeader.Type, pk.Payload)
	}
	// Ack the sanity PUBLISH so its inflight slot is freed before we
	// trigger the disruption. (sub.Read consumes the packet but
	// doesn't auto-PUBACK for QoS-1; emit one explicitly.)
	if err := sub.WritePacket(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback},
		PacketID:    pk.PacketID,
	}); err != nil {
		t.Fatalf("write puback: %v", err)
	}

	// Wedge pod 0's listener into a sustained reconnect loop by
	// pointing it at an unreachable URL, then forcing a wait error.
	// Every reconnect dial will fail (connection refused on :1) so
	// pod 0's listener is reliably DEAD for the publish below. Pod 1's
	// listener is irrelevant — publish.go's `pg_notify` runs INSIDE
	// the publish tx via pool conns, not via the publisher's listener
	// conn. The NOTIFY is targeted at pod 0's `pgmqtt_<broker_id>`
	// channel, and pod 0 is the one that won't be LISTENing.
	goodURL := mh.URL
	listeners[0].SetURLForTest("postgres://nobody:nobody@127.0.0.1:1/nodb?sslmode=disable")
	if _, err := mh.Pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid)
		  FROM pg_stat_activity
		 WHERE application_name = 'pgmqttd-listener'
		   AND datname = current_database()
		   AND pid <> pg_backend_pid()
	`); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}

	// Wait until pod 0's listener has failed at least once (so we
	// know it's truly in the backoff loop, not racing pre-disrupt).
	// We can read pg_stat_activity instead of holding pod 0's
	// metrics handle — once pod 1 reconnects (n==1 listener backend)
	// we know pod 0 is the one still down. Pod 0 hits 127.0.0.1:1 →
	// refused → backoff.
	dialDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(dialDeadline) {
		var n int
		if err := mh.Pool.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			 WHERE application_name = 'pgmqttd-listener'
			   AND datname = current_database()`).Scan(&n); err == nil {
			if n == 1 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Publish while pod 0's listener is firmly stuck in the failed-
	// reconnect loop. The publish-tx pg_notify fires, pod 0's channel
	// is dead, the delivery row sits at state=0 with nothing to wake
	// the per-conn drain loop.
	pub.Publish(t, "drain/during", []byte("during"), 1, false)

	// Now point pod 0's listener back at the real URL. Once it dials
	// successfully, the run-loop's post-reconnect path must invoke
	// engine.KickAllDrains, which wakes our passive subscriber's drain
	// loop and delivers the queued row.
	listeners[0].SetURLForTest(goodURL)

	// Wait for the message. Pre-fix this hangs forever (or until
	// some other PUBACK-driven kick comes in — which won't happen for
	// a passive subscriber). Post-fix the post-reconnect drain kick
	// pulls the queued row within the next backoff cycle (≤ a few s).
	if err := sub.Conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	defer sub.Conn.SetReadDeadline(time.Time{})
	got, err := sub.NextRaw()
	if err != nil {
		t.Fatalf("post-reconnect drain never delivered queued PUBLISH: %v", err)
	}
	if got.FixedHeader.Type != packets.Publish || string(got.Payload) != "during" {
		t.Fatalf("unexpected packet after reconnect: type=%d payload=%q", got.FixedHeader.Type, got.Payload)
	}
}
