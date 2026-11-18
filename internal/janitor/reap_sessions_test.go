package janitor_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
)

// TestReapAppliesSessionLifetime: when a broker dies, each of its
// sessions has lost its connection, and what the client asked for at
// CONNECT applies exactly as it would on any other disconnect. The reap
// used to clear ownership and nothing else: sessions that should have
// ended with their connection stayed for good, sessions with an expiry
// never got an expiry time, and a Will Delay was honoured even when the
// session (and so the wait) ended at once.
func TestReapAppliesSessionLifetime(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	startListener(t, mh)
	ctx := context.Background()
	dead := uuid.New()

	seed := func(clientID string, protocol int, clean bool, expiry any, willDelay any) {
		t.Helper()
		if _, err := mh.Pool.Exec(ctx, `
			INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
			    expiry_interval, will_topic, will_payload, will_qos, will_retain, will_delay)
			VALUES ($1, $2, true, $3, $4, $5, 'reap/' || $1, 'offline', 1, true, $6)
		`, clientID, dead, protocol, clean, expiry, willDelay); err != nil {
			t.Fatalf("seed %s: %v", clientID, err)
		}
		if _, err := mh.Pool.Exec(ctx,
			`INSERT INTO subscriptions(client_id, topic_filter, qos) VALUES ($1, 'some/topic', 1)`,
			clientID); err != nil {
			t.Fatalf("seed subscription for %s: %v", clientID, err)
		}
	}
	seed("v3-clean", 4, true, 0, nil)
	seed("v5-expiry-0-delayed-will", 5, true, 0, 60)
	seed("v5-expiry-5", 5, false, 5, nil)
	seed("v3-persistent", 4, false, nil, nil)
	seed("v5-never", 5, false, 2147483647, nil)

	jt := janitor.New(mh.Pool, mh.Pods[0].Engine, warnLogger())
	jt.SetDeadBrokerGrace(0)
	if err := jt.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}

	type row struct {
		exists    bool
		expiresIn *float64
		subs      int
		will      string
	}
	read := func(clientID string) row {
		t.Helper()
		var r row
		err := mh.Pool.QueryRow(ctx, `
			SELECT extract(epoch FROM session_expires_at - now())::float8
			  FROM sessions WHERE client_id=$1`, clientID).Scan(&r.expiresIn)
		r.exists = err == nil
		if err := mh.Pool.QueryRow(ctx,
			`SELECT count(*) FROM subscriptions WHERE client_id=$1`, clientID).Scan(&r.subs); err != nil {
			t.Fatalf("count subscriptions: %v", err)
		}
		var payload []byte
		if err := mh.Pool.QueryRow(ctx,
			`SELECT payload FROM retained WHERE topic='reap/' || $1`, clientID).Scan(&payload); err == nil {
			r.will = string(payload)
		}
		return r
	}

	for _, id := range []string{"v3-clean", "v5-expiry-0-delayed-will"} {
		if r := read(id); r.exists || r.subs != 0 {
			t.Errorf("%s: session should have ended with its connection: %+v", id, r)
		}
	}
	// The session ended, so its will is due however long the Will Delay.
	if r := read("v5-expiry-0-delayed-will"); r.will != "offline" {
		t.Errorf("will of an ended session waited for its Will Delay: %+v", r)
	}
	if r := read("v5-expiry-5"); !r.exists || r.subs != 1 || r.expiresIn == nil || *r.expiresIn < 1 || *r.expiresIn > 6 {
		t.Errorf("v5-expiry-5: want kept with an expiry about 5s out: %+v", r)
	}
	for _, id := range []string{"v3-persistent", "v5-never"} {
		if r := read(id); !r.exists || r.subs != 1 || r.expiresIn != nil {
			t.Errorf("%s: want kept with no expiry: %+v", id, r)
		}
	}
	for _, id := range []string{"v3-clean", "v5-expiry-5", "v3-persistent", "v5-never"} {
		if r := read(id); r.will != "offline" {
			t.Errorf("%s: will not published by the reap: %+v", id, r)
		}
	}
}
