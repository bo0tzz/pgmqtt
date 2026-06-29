package listener_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/listener"
)

// TestCNPGFailoverStormSelfHeals pins end-to-end recovery for the
// 2026-06-29 production incident: CNPG primary failover killed every
// broker's listener PG session simultaneously (SQLSTATE 57P01),
// releasing every per-broker advisory lock at once. The first broker
// whose listener reconnected ran find_dead_brokers, saw every peer's
// lock available, and ran handleDeadBroker against still-live peers —
// rewriting their sessions rows to broker_id=NULL, connected=false.
// The peers' TCP clients stayed connected; only the DB lied.
//
// Pre-fix bug: a subsequent cross-broker publish ran mqtt_publish.
// The broker_ids fan-out filtered on m.connected, so the reaped
// sub's row was dropped from the NOTIFY targets. The deliveries row
// was inserted (QoS-1 gate passes), but no broker was notified —
// silent loss until something else woke the drain loop.
//
// Fix in cfdc073: migration 0018 drops the m.connected filter from
// the routing fan-out; listener reconnect calls AttestOwnedSessions
// to rewrite the truth (broker_id, connected) for every in-memory
// Conn, gated by session_token so legitimate takeovers aren't
// stomped.
//
// This test stages the full chain: stand up two brokers with
// listeners, plant a sub on pod 0, simulate the spurious reap of
// pod 0 (the SQL effect of handleDeadBroker firing against a
// still-live peer), force pod 0's listener through a reconnect
// cycle, and verify that the truth is restored AND a brand-new
// cross-broker publish reaches the subscriber. Pre-fix this test
// hangs forever on the post-storm read.
func TestCNPGFailoverStormSelfHeals(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	listeners := make([]*listener.Listener, len(mh.Pods))
	for i, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener[%d]: %v", i, err)
		}
		listeners[i] = l
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	// Persistent QoS-1 subscriber on pod 0. Persistent session so the
	// sessions row sticks around for us to manipulate; QoS-1 so the
	// post-storm publish exercises the full delivery-row path that the
	// bug silently dropped on the floor.
	const subID = "cnpg-sub"
	sub := mh.Pods[0].Connect(t, subID, func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	})
	defer sub.Close()
	sub.Subscribe(t, "storm/+", 1)

	pub := mh.Pods[1].Connect(t, "cnpg-pub")
	defer pub.Close()

	// Sanity: cross-broker delivery works before the storm.
	pub.Publish(t, "storm/sanity", []byte("sanity"), 1, false)
	pk := sub.Read(t, 10*time.Second)
	if pk.FixedHeader.Type != packets.Publish || string(pk.Payload) != "sanity" {
		t.Fatalf("sanity: type=%d payload=%q", pk.FixedHeader.Type, pk.Payload)
	}
	if err := sub.WritePacket(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback},
		PacketID:    pk.PacketID,
	}); err != nil {
		t.Fatalf("puback sanity: %v", err)
	}

	// Capture the truth pre-storm.
	var origBroker uuid.UUID
	var origToken uuid.UUID
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT broker_id, session_token FROM sessions WHERE client_id=$1`,
		subID).Scan(&origBroker, &origToken); err != nil {
		t.Fatalf("read pre-storm: %v", err)
	}
	if origBroker != mh.Pods[0].BrokerID {
		t.Fatalf("pre-storm: broker_id=%s want %s", origBroker, mh.Pods[0].BrokerID)
	}

	// Simulate the spurious peer reap. handleDeadBroker on a peer would
	// have done exactly this UPDATE against the sub's row during the
	// brief window where every broker's advisory lock was free. We
	// don't drive janitor.handleDeadBroker through the real path here
	// because timing it deterministically against listener reconnect
	// requires racing the lock against the test goroutine — and that's
	// not what we're pinning. We're pinning the recovery path: given
	// reaped rows, does the broker self-heal and does routing still
	// work?
	if _, err := mh.Pool.Exec(context.Background(), `
		UPDATE sessions SET broker_id=NULL, connected=false, last_seen=now()
		 WHERE client_id=$1
	`, subID); err != nil {
		t.Fatalf("simulate reap: %v", err)
	}

	// Self-check: the row really is reaped now.
	var reapedBroker *uuid.UUID
	var reapedConnected bool
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT broker_id, connected FROM sessions WHERE client_id=$1`,
		subID).Scan(&reapedBroker, &reapedConnected); err != nil {
		t.Fatalf("read post-reap: %v", err)
	}
	if reapedBroker != nil || reapedConnected {
		t.Fatalf("self-check: row not reaped (broker=%v connected=%v)",
			reapedBroker, reapedConnected)
	}

	// Force pod 0's listener through a reconnect cycle. The CNPG event
	// killed the listener PG session; the wait loop will see the wait
	// error and run the reconnect path, which calls
	// AttestOwnedSessions. We don't need to point pod 0 at a bad URL —
	// just terminate the backend; the listener reconnects to the same
	// good URL and the attest fires on success.
	if _, err := mh.Pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid)
		  FROM pg_stat_activity
		 WHERE application_name = 'pgmqttd-listener'
		   AND datname = current_database()
		   AND pid <> pg_backend_pid()
	`); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}

	// Wait for the attest UPDATE to land. It fires on the first
	// successful reconnect, which dials immediately (no leading sleep)
	// so this is sub-second in practice; give it generous headroom.
	healDeadline := time.Now().Add(15 * time.Second)
	var healedBroker uuid.UUID
	var healedConnected bool
	var healedToken uuid.UUID
	for time.Now().Before(healDeadline) {
		err := mh.Pool.QueryRow(context.Background(),
			`SELECT COALESCE(broker_id, '00000000-0000-0000-0000-000000000000'::uuid),
			        connected, session_token
			   FROM sessions WHERE client_id=$1`,
			subID).Scan(&healedBroker, &healedConnected, &healedToken)
		if err == nil && healedBroker == mh.Pods[0].BrokerID && healedConnected {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if healedBroker != mh.Pods[0].BrokerID {
		t.Fatalf("attest never restored broker_id: want %s, got %s",
			mh.Pods[0].BrokerID, healedBroker)
	}
	if !healedConnected {
		t.Fatalf("attest never restored connected=true")
	}
	if healedToken != origToken {
		t.Fatalf("attest rotated session_token: want %s, got %s",
			origToken, healedToken)
	}

	// The real end-to-end assertion: a NEW cross-broker publish, run
	// strictly after the storm, must reach the sub. Pre-fix this hangs
	// because mqtt_publish's broker_ids fan-out filtered on m.connected
	// — even after pod 0's listener was healthy again, peer pubs to the
	// sub's topic would write a delivery row but not NOTIFY anyone if
	// the row's connected flag had decayed. Post-fix (0018 + attest),
	// routing is broker_id-only and attest restored both fields.
	pub.Publish(t, "storm/post", []byte("after-storm"), 1, false)

	if err := sub.Conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	defer sub.Conn.SetReadDeadline(time.Time{})
	got, err := sub.NextRaw()
	if err != nil {
		t.Fatalf("post-storm publish never delivered: %v", err)
	}
	if got.FixedHeader.Type != packets.Publish || string(got.Payload) != "after-storm" {
		t.Fatalf("post-storm: type=%d payload=%q", got.FixedHeader.Type, got.Payload)
	}
}
