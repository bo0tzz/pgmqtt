package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
)

type sessionSnapshot struct {
	exists    bool
	connected bool
	brokerID  *uuid.UUID
	willTopic *string
	fireIn    *float64 // seconds until will_fire_at
	expiresIn *float64 // seconds until session_expires_at
}

func snapshotSession(t *testing.T, mh *enginetest.MultiHarness, clientID string) sessionSnapshot {
	t.Helper()
	var s sessionSnapshot
	err := mh.Pool.QueryRow(context.Background(), `
		SELECT connected, broker_id, will_topic,
		       extract(epoch FROM will_fire_at - now())::float8,
		       extract(epoch FROM session_expires_at - now())::float8
		  FROM sessions WHERE client_id=$1
	`, clientID).Scan(&s.connected, &s.brokerID, &s.willTopic, &s.fireIn, &s.expiresIn)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return s
		}
		t.Fatalf("read session %q: %v", clientID, err)
	}
	s.exists = true
	return s
}

func countRows(t *testing.T, mh *enginetest.MultiHarness, query string, args ...any) int {
	t.Helper()
	var n int
	if err := mh.Pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func retainedPayload(t *testing.T, mh *enginetest.MultiHarness, topic string) string {
	t.Helper()
	var payload []byte
	err := mh.Pool.QueryRow(context.Background(),
		`SELECT payload FROM retained WHERE topic=$1`, topic).Scan(&payload)
	if err != nil {
		return ""
	}
	return string(payload)
}

func near(got *float64, want, slack float64) bool {
	return got != nil && *got > want-slack && *got < want+slack
}

func withRetainedWill(topic string) func(*packets.Packet) {
	return func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = topic
		p.Connect.WillPayload = []byte("offline")
		p.Connect.WillQos = 1
		p.Connect.WillRetain = true
	}
}

func withExpiry(seconds uint32) func(*packets.Packet) {
	return func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = seconds
		p.Properties.SessionExpiryIntervalFlag = true
	}
}

// TestBrokerShutdownEndsItsSessions: a broker that is told to stop closes
// its clients' connections, and for the sessions that is the same event
// as any other close. Previously none of it happened: wills were neither
// published nor scheduled (and could never be afterwards), sessions that
// should have ended with their connection stayed, and sessions with an
// expiry never got an expiry time.
func TestBrokerShutdownEndsItsSessions(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	pod := mh.Pods[0]

	willNow := pod.Connect(t, "sd-will-now", withRetainedWill("sd/now"), withExpiry(3600))
	defer willNow.Close()
	willLater := pod.Connect(t, "sd-will-later", withRetainedWill("sd/later"), withExpiry(3600),
		func(p *packets.Packet) { p.Connect.WillProperties.WillDelayInterval = 60 })
	defer willLater.Close()
	ephemeral := pod.Connect(t, "sd-ephemeral", withRetainedWill("sd/ephemeral"))
	defer ephemeral.Close()
	ephemeral.Subscribe(t, "sd/topic", 1)
	v3 := pod.Connect(t, "sd-v3-persistent", func(p *packets.Packet) {
		p.ProtocolVersion = 4
		p.Connect.Clean = false
	})
	defer v3.Close()

	pod.Shutdown(t)

	if got := retainedPayload(t, mh, "sd/now"); got != "offline" {
		t.Errorf("will with no delay: retained payload %q, want it published at shutdown", got)
	}
	if s := snapshotSession(t, mh, "sd-will-now"); !s.exists || s.connected || s.brokerID != nil ||
		s.willTopic != nil || !near(s.expiresIn, 3600, 30) {
		t.Errorf("session with a published will: %+v", s)
	}

	if got := retainedPayload(t, mh, "sd/later"); got != "" {
		t.Errorf("will with a 60s delay was published at shutdown")
	}
	if s := snapshotSession(t, mh, "sd-will-later"); !s.exists || s.connected || s.brokerID != nil ||
		s.willTopic == nil || !near(s.fireIn, 60, 10) {
		t.Errorf("session with a delayed will: want it scheduled ~60s out, got %+v", s)
	}

	// Session Expiry 0: the session ends with the connection, which is
	// also when its will is due.
	if s := snapshotSession(t, mh, "sd-ephemeral"); s.exists {
		t.Errorf("session with no expiry interval survived its connection: %+v", s)
	}
	if n := countRows(t, mh, `SELECT count(*) FROM subscriptions WHERE client_id='sd-ephemeral'`); n != 0 {
		t.Errorf("ended session left %d subscriptions", n)
	}
	if got := retainedPayload(t, mh, "sd/ephemeral"); got != "offline" {
		t.Errorf("will of a session that ended: retained payload %q", got)
	}

	if s := snapshotSession(t, mh, "sd-v3-persistent"); !s.exists || s.connected || s.brokerID != nil || s.expiresIn != nil {
		t.Errorf("v3.1.1 persistent session: want kept, disconnected, no expiry; got %+v", s)
	}
}

// TestServerClosedConnectionEndsSession: the session-end work used to be
// skipped whenever the server was the one to close the socket (quota
// kick, a failed drain), leaving a row that claimed a live connection.
func TestServerClosedConnectionEndsSession(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	c := pod.Connect(t, "kicked", withRetainedWill("kick/state"), withExpiry(3600))
	defer c.Close()
	conn, ok := pod.Engine.ConnFor("kicked")
	if !ok {
		t.Fatal("no Conn for the client")
	}
	conn.Shutdown()

	deadline := time.Now().Add(5 * time.Second)
	for {
		s := snapshotSession(t, mh, "kicked")
		if s.exists && !s.connected && s.brokerID == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session still claims a connection after the server closed it: %+v", s)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := retainedPayload(t, mh, "kick/state"); got != "offline" {
		t.Errorf("will not published after a server-side close: retained payload %q", got)
	}
}

// TestOrphanedSessionIsEnded: a row can be left naming this broker with
// no connection behind it (the end of the connection could not be
// written at the time). Nothing used to notice.
func TestOrphanedSessionIsEnded(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	ctx := context.Background()

	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    expiry_interval, will_topic, will_payload, will_qos, will_retain, last_seen)
		VALUES ('orphan', $1, true, 5, false, 3600, 'orphan/state', 'offline', 1, true,
		        now() - interval '1 hour')
	`, pod.BrokerID); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	// A row just as old, but with a live connection behind it.
	live := pod.Connect(t, "not-orphan", withRetainedWill("live/state"), withExpiry(3600))
	defer live.Close()
	// And one that is mid-CONNECT as far as the sweep can tell: recent,
	// no Conn registered yet.
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    will_topic, will_payload, will_qos, will_retain)
		VALUES ('connecting', $1, true, 5, false, 'connecting/state', 'offline', 1, true)
	`, pod.BrokerID); err != nil {
		t.Fatalf("seed connecting: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx,
		`UPDATE sessions SET last_seen = now() - interval '1 hour' WHERE client_id='not-orphan'`); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	if err := pod.Engine.EndOrphanedSessionsForTest(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if s := snapshotSession(t, mh, "orphan"); s.connected || s.brokerID != nil || s.willTopic != nil ||
		!near(s.expiresIn, 3600, 30) {
		t.Errorf("orphan not ended: %+v", s)
	}
	if got := retainedPayload(t, mh, "orphan/state"); got != "offline" {
		t.Errorf("orphan's will not published: %q", got)
	}
	if s := snapshotSession(t, mh, "not-orphan"); !s.connected || s.brokerID == nil || s.willTopic == nil {
		t.Errorf("sweep ended a session with a live connection: %+v", s)
	}
	if s := snapshotSession(t, mh, "connecting"); !s.connected || s.brokerID == nil {
		t.Errorf("sweep ended a session that may still be connecting: %+v", s)
	}
	if got := retainedPayload(t, mh, "live/state") + retainedPayload(t, mh, "connecting/state"); got != "" {
		t.Errorf("sweep published a will it should not have: %q", got)
	}
}

// failPublishesTo makes every publish to topic fail inside Postgres, and
// returns a func that lets them through again.
func failPublishesTo(t *testing.T, mh *enginetest.MultiHarness, topic string) (heal func()) {
	t.Helper()
	ctx := context.Background()
	if _, err := mh.Pool.Exec(ctx, `
		CREATE FUNCTION test_fail_topic() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.topic = TG_ARGV[0] THEN RAISE EXCEPTION 'injected publish failure'; END IF;
			RETURN NEW;
		END $$`); err != nil {
		t.Fatalf("create function: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx,
		`CREATE TRIGGER test_fail_topic BEFORE INSERT ON messages
		 FOR EACH ROW EXECUTE FUNCTION test_fail_topic('`+topic+`')`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	return func() {
		t.Helper()
		if _, err := mh.Pool.Exec(ctx, `DROP TRIGGER test_fail_topic ON messages`); err != nil {
			t.Fatalf("drop trigger: %v", err)
		}
	}
}

// TestFailedWillPublishKeepsTheWill: a will that could not be published
// must still be there to publish later. It used to be cleared regardless,
// on every path, and the session was marked ended around it.
func TestFailedWillPublishKeepsTheWill(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	ctx := context.Background()
	heal := failPublishesTo(t, mh, "fail/state")

	// On disconnect.
	c := pod.Connect(t, "fail-now", withRetainedWill("fail/state"), withExpiry(3600))
	c.Kill()
	// Due, from an earlier disconnect.
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, connected, protocol_version, clean_start, expiry_interval,
		    will_topic, will_payload, will_qos, will_retain, will_delay, will_fire_at)
		VALUES ('fail-due', false, 5, false, 3600, 'fail/state', 'offline', 1, true, 1,
		        now() - interval '1 second')
	`); err != nil {
		t.Fatalf("seed due will: %v", err)
	}

	// Give the disconnect time to be handled, then try the due will.
	time.Sleep(500 * time.Millisecond)
	if _, err := pod.Engine.FireDueWills(ctx); err != nil {
		t.Fatalf("FireDueWills: %v", err)
	}

	if s := snapshotSession(t, mh, "fail-now"); !s.connected || s.brokerID == nil || s.willTopic == nil {
		t.Errorf("after a failed will publish the session was ended anyway: %+v", s)
	}
	if s := snapshotSession(t, mh, "fail-due"); s.willTopic == nil || s.fireIn == nil {
		t.Errorf("a due will that failed to publish was cleared: %+v", s)
	}
	if got := retainedPayload(t, mh, "fail/state"); got != "" {
		t.Fatalf("publish was supposed to fail, but the will is retained: %q", got)
	}

	heal()
	if _, err := mh.Pool.Exec(ctx,
		`UPDATE sessions SET last_seen = now() - interval '1 hour' WHERE client_id='fail-now'`); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if err := pod.Engine.EndOrphanedSessionsForTest(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if got := retainedPayload(t, mh, "fail/state"); got != "offline" {
		t.Errorf("will not published once publishing worked again: %q", got)
	}
	if s := snapshotSession(t, mh, "fail-now"); s.connected || s.willTopic != nil {
		t.Errorf("session not ended on retry: %+v", s)
	}
	if _, err := mh.Pool.Exec(ctx, `DELETE FROM retained WHERE topic='fail/state'`); err != nil {
		t.Fatalf("clear retained: %v", err)
	}
	fired, err := pod.Engine.FireDueWills(ctx)
	if err != nil || fired != 1 {
		t.Fatalf("FireDueWills on retry: fired=%d err=%v", fired, err)
	}
	if got := retainedPayload(t, mh, "fail/state"); got != "offline" {
		t.Errorf("due will not published on retry: %q", got)
	}
}

// TestDueWillNotFiredForReconnectedClient: once a client is back, a will
// scheduled from its previous connection must not go out, and the will
// columns on its row now belong to the new connection.
func TestDueWillNotFiredForReconnectedClient(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	ctx := context.Background()

	c := pod.Connect(t, "back-again", withRetainedWill("back/state"), withExpiry(3600))
	defer c.Close()
	// The state a row is in between a reconnect taking ownership and the
	// old schedule being cleared, had those been separate steps.
	if _, err := mh.Pool.Exec(ctx,
		`UPDATE sessions SET will_fire_at = now() - interval '1 second' WHERE client_id='back-again'`); err != nil {
		t.Fatalf("seed stale schedule: %v", err)
	}
	fired, err := pod.Engine.FireDueWills(ctx)
	if err != nil {
		t.Fatalf("FireDueWills: %v", err)
	}
	if fired != 0 || retainedPayload(t, mh, "back/state") != "" {
		t.Errorf("published the will of a connected client (fired=%d)", fired)
	}
}

// TestWillTopicIsValidatedAtConnect: a will topic is a topic name, and is
// held to the same rules as the topic of a PUBLISH.
func TestWillTopicIsValidatedAtConnect(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)

	rc, pk := h.ConnectExpectReject(t, "bad-will", withRetainedWill("w/#"))
	defer rc.Close()
	if pk == nil || pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0x90 {
		t.Fatalf("want CONNACK 0x90 (Topic Name invalid), got %+v", pk)
	}
	var n int
	if err := h.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM sessions WHERE client_id='bad-will'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("a rejected CONNECT left a session row")
	}
}
