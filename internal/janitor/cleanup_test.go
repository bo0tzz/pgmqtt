package janitor_test

import (
	"context"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
)

func count(t *testing.T, h *enginetest.Harness, query string, args ...any) int {
	t.Helper()
	var n int
	if err := h.Pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestOrphanSweepKeepsQueueOfPersistentV5Session: a v5 session persists
// by its Session Expiry Interval, not by Clean Start, which only says
// whether to discard what was there before. The sweep treated
// clean_start=true as "nothing here is worth keeping" and deleted the
// queued messages of sessions with an hour left to run.
func TestOrphanSweepKeepsQueueOfPersistentV5Session(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	sub := h.Connect(t, "v5-persistent", func(p *packets.Packet) {
		p.Connect.Clean = true
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	})
	sub.Subscribe(t, "keep/#", 1)
	sub.Disconnect(t)
	time.Sleep(200 * time.Millisecond)

	pub := h.Connect(t, "keep-pub")
	defer pub.Close()
	pub.Publish(t, "keep/x", []byte("queued"), 1, false)
	if n := count(t, h, `SELECT count(*) FROM deliveries WHERE client_id='v5-persistent'`); n != 1 {
		t.Fatalf("setup: %d deliveries queued, want 1", n)
	}
	if _, err := h.Pool.Exec(ctx, `
		UPDATE sessions SET last_seen = now() - interval '1 day' WHERE client_id='v5-persistent'`); err != nil {
		t.Fatalf("backdate session: %v", err)
	}
	if _, err := h.Pool.Exec(ctx, `
		UPDATE deliveries SET created_at = now() - interval '1 day' WHERE client_id='v5-persistent'`); err != nil {
		t.Fatalf("backdate delivery: %v", err)
	}
	// A delivery whose session really is gone, for contrast.
	if _, err := h.Pool.Exec(ctx, `ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_client_id_fkey`); err != nil {
		t.Fatalf("drop fk: %v", err)
	}
	if _, err := h.Pool.Exec(ctx, `
		INSERT INTO deliveries(client_id, message_id, qos, state, created_at)
		SELECT 'gone', message_id, 1, 0, now() - interval '1 day' FROM deliveries LIMIT 1`); err != nil {
		t.Fatalf("seed orphan delivery: %v", err)
	}

	jt := janitor.New(h.Pool, h.Engine, warnLogger())
	jt.SetOrphanGrace(time.Minute)
	if err := jt.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if n := count(t, h, `SELECT count(*) FROM deliveries WHERE client_id='v5-persistent'`); n != 1 {
		t.Errorf("orphan sweep deleted the queue of a session that still exists (%d left)", n)
	}
	if n := count(t, h, `SELECT count(*) FROM deliveries WHERE client_id='gone'`); n != 0 {
		t.Errorf("orphan sweep left %d deliveries for a session that is gone", n)
	}
}

// TestExpiredQueuedDeliveriesAreRemoved: a message whose expiry passes
// while it waits for a client that is away is deleted for that client.
// Nothing used to do it (the drain queries skipped expired rows, which
// made the delete behind them unreachable), so the rows stayed, counted
// against the client's queue cap, and got it kicked on every reconnect.
func TestExpiredQueuedDeliveriesAreRemoved(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	}
	sub := h.Connect(t, "exp-sub", persistent)
	sub.Subscribe(t, "exp/#", 1)
	sub.Disconnect(t)
	time.Sleep(200 * time.Millisecond)

	pub := h.Connect(t, "exp-pub")
	defer pub.Close()
	if err := pub.WritePacket(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "exp/short", Payload: []byte("short-lived"), PacketID: 1,
		Properties: packets.Properties{MessageExpiryInterval: 1},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if pk := pub.TryRead(3 * time.Second); pk == nil || pk.FixedHeader.Type != packets.Puback {
		t.Fatalf("no PUBACK for the expiring publish: %+v", pk)
	}
	pub.Publish(t, "exp/long", []byte("keeps"), 1, false)
	if n := count(t, h, `SELECT count(*) FROM deliveries WHERE client_id='exp-sub'`); n != 2 {
		t.Fatalf("setup: %d deliveries queued, want 2", n)
	}
	time.Sleep(1200 * time.Millisecond)

	jt := janitor.New(h.Pool, h.Engine, warnLogger())
	if err := jt.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if n := count(t, h, `SELECT count(*) FROM deliveries WHERE client_id='exp-sub'`); n != 1 {
		t.Errorf("%d deliveries left after the janitor ran, want only the unexpired one", n)
	}

	sub = h.Connect(t, "exp-sub", persistent)
	defer sub.Close()
	pk := sub.TryRead(3 * time.Second)
	if pk == nil || string(pk.Payload) != "keeps" {
		t.Fatalf("resumed session did not get the unexpired message: %+v", pk)
	}
}

// TestRepairSessionsLeftByOlderBrokers: the three kinds of row that
// brokers before 0.3.0 left behind on shutdown and on reap, each stuck
// for good. See Janitor.repairSessions.
func TestRepairSessionsLeftByOlderBrokers(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	seed := func(clientID string, expiry any, will bool) {
		t.Helper()
		var topic, payload any
		if will {
			topic, payload = "repair/"+clientID, []byte("offline")
		}
		if _, err := h.Pool.Exec(ctx, `
			INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
			    expiry_interval, will_topic, will_payload, will_qos, will_retain)
			VALUES ($1, NULL, false, 5, false, $2, $3, $4, CASE WHEN $3::text IS NULL THEN NULL ELSE 1 END,
			        CASE WHEN $3::text IS NULL THEN NULL ELSE true END)
		`, clientID, expiry, topic, payload); err != nil {
			t.Fatalf("seed %s: %v", clientID, err)
		}
	}
	seed("stranded-will", 3600, true)
	seed("no-expiry-time", 5, false)
	seed("should-have-ended", 0, false)
	seed("ended-with-will", 0, true)
	seed("fine-forever", nil, false)

	jt := janitor.New(h.Pool, h.Engine, warnLogger())
	jt.SetJobIntervals(map[string]time.Duration{
		janitor.JobRepairSessions: 0,
		janitor.JobFireDueWills:   0,
	})
	for i := 0; i < 3; i++ {
		if err := jt.Tick(ctx); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}

	retained := func(clientID string) bool {
		return count(t, h, `SELECT count(*) FROM retained WHERE topic=$1`, "repair/"+clientID) == 1
	}
	exists := func(clientID string) bool {
		return count(t, h, `SELECT count(*) FROM sessions WHERE client_id=$1`, clientID) == 1
	}
	if !retained("stranded-will") {
		t.Errorf("a will stranded with no fire time was not published")
	}
	if n := count(t, h, `SELECT count(*) FROM sessions WHERE client_id='stranded-will'
		AND will_topic IS NULL AND session_expires_at > now() + interval '50 minutes'`); n != 1 {
		t.Errorf("stranded-will: want the will cleared and an expiry about an hour out")
	}
	if n := count(t, h, `SELECT count(*) FROM sessions WHERE client_id='no-expiry-time'
		AND session_expires_at BETWEEN now() AND now() + interval '6 seconds'`); n != 1 {
		t.Errorf("a session with a 5s expiry interval and no expiry time was not given one")
	}
	if exists("should-have-ended") {
		t.Errorf("a session that should have ended with its connection is still there")
	}
	if !retained("ended-with-will") || exists("ended-with-will") {
		t.Errorf("ended session with a will: published=%v still exists=%v, want published and gone",
			retained("ended-with-will"), exists("ended-with-will"))
	}
	if !exists("fine-forever") {
		t.Errorf("a persistent session with nothing wrong was deleted")
	}
}

// TestExpiredSessionPublishesItsWillBeforeGoing: an expired session that
// still holds a will (publishing it failed, or an older broker scheduled
// it past the expiry) is not deleted with the will unpublished.
func TestExpiredSessionPublishesItsWillBeforeGoing(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	if _, err := h.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, connected, protocol_version, clean_start, expiry_interval,
		    will_topic, will_payload, will_qos, will_retain, will_delay,
		    will_fire_at, session_expires_at)
		VALUES ('expired-with-will', false, 5, false, 5, 'expired/state', 'offline', 1, true, 600,
		        now() + interval '10 minutes', now() - interval '1 second')
	`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	jt := janitor.New(h.Pool, h.Engine, warnLogger())
	for i := 0; i < 2; i++ {
		if err := jt.Tick(ctx); err != nil {
			t.Fatalf("tick: %v", err)
		}
		if i == 0 {
			time.Sleep(1100 * time.Millisecond) // fire_due_wills and expire_sessions run once a second
		}
	}
	if n := count(t, h, `SELECT count(*) FROM retained WHERE topic='expired/state'`); n != 1 {
		t.Errorf("session expired without its will being published")
	}
	if n := count(t, h, `SELECT count(*) FROM sessions WHERE client_id='expired-with-will'`); n != 0 {
		t.Errorf("expired session still present after its will went out")
	}
}
