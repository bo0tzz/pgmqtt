package janitor_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/listener"
)

// TestDeadBrokerGraceExceedsReacquireDeadline pins the ordering the whole
// scheme rests on: a broker gives up (and exits) strictly before any peer
// would reap it.
func TestDeadBrokerGraceExceedsReacquireDeadline(t *testing.T) {
	if janitor.DeadBrokerGrace <= listener.LockReacquireDeadline {
		t.Fatalf("DeadBrokerGrace (%s) must exceed listener.LockReacquireDeadline (%s)",
			janitor.DeadBrokerGrace, listener.LockReacquireDeadline)
	}
}

type sessionState struct {
	brokerID  *uuid.UUID
	connected bool
	willTopic *string
}

func readSession(t *testing.T, mh *enginetest.MultiHarness, clientID string) sessionState {
	t.Helper()
	var s sessionState
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT broker_id, connected, will_topic FROM sessions WHERE client_id=$1`,
		clientID).Scan(&s.brokerID, &s.connected, &s.willTopic); err != nil {
		t.Fatalf("read session %q: %v", clientID, err)
	}
	return s
}

func seedSessionOwnedBy(t *testing.T, mh *enginetest.MultiHarness, clientID string, broker uuid.UUID) {
	t.Helper()
	if _, err := mh.Pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    will_topic, will_payload, will_qos, will_retain)
		VALUES ($1, $2, true, 5, false, 'lwt/' || $1, 'died', 1, true)
	`, clientID, broker); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

// startListener gives pod 0 a real listener, so its own advisory lock is
// held the way it is on a healthy broker.
func startListener(t *testing.T, mh *enginetest.MultiHarness) *listener.Listener {
	t.Helper()
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.BrokerID = l.BrokerID()
	return l
}

// holdBrokerLock takes broker's advisory lock on a dedicated connection,
// standing in for that broker's listener. The returned func drops it and
// waits for Postgres to have actually let go.
func holdBrokerLock(t *testing.T, mh *enginetest.MultiHarness, broker uuid.UUID) (release func()) {
	t.Helper()
	ctx := context.Background()
	holder, err := pgx.Connect(ctx, mh.URL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = holder.Close(ctx) })
	if _, err := holder.Exec(ctx,
		`SELECT pg_advisory_lock(hashtextextended($1, 0))`, "pgmqtt:broker:"+broker.String()); err != nil {
		t.Fatalf("hold lock: %v", err)
	}
	pid := holder.PgConn().PID()
	return func() {
		t.Helper()
		if err := holder.Close(ctx); err != nil {
			t.Fatalf("release lock: %v", err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			var alive int
			if err := mh.Pool.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&alive); err != nil {
				t.Fatalf("poll holder backend: %v", err)
			}
			if alive == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("lock-holding backend never exited")
			}
		}
	}
}

// fakeClockJanitor returns a janitor for pod 0 that probes for dead
// brokers on every Tick and reads time from the returned pointer. tick
// advances the clock, runs one Tick and returns clientID's session.
func fakeClockJanitor(t *testing.T, mh *enginetest.MultiHarness, clientID string) (jt *janitor.Janitor, tick func(advance time.Duration) sessionState) {
	now := time.Now()
	jt = janitor.New(mh.Pool, mh.Pods[0].Engine, warnLogger())
	jt.SetJobIntervals(map[string]time.Duration{janitor.JobFindDeadBrokers: 0})
	jt.SetNowForTest(func() time.Time { return now })
	return jt, func(advance time.Duration) sessionState {
		t.Helper()
		now = now.Add(advance)
		if err := jt.Tick(context.Background()); err != nil {
			t.Fatalf("tick: %v", err)
		}
		return readSession(t, mh, clientID)
	}
}

func (s sessionState) owned() bool { return s.brokerID != nil && s.connected && s.willTopic != nil }

// TestJanitorNeverReapsOwnBroker covers the self-reap half of the
// 2026-11-17 incident. The harness engine has a broker ID but no listener,
// so its advisory lock is free: exactly what a running broker looks like
// while its listener reconnects and its pool is already back. Grace is
// zero so nothing but the identity check can save the session.
func TestJanitorNeverReapsOwnBroker(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	const clientID = "self-willer"
	const willTopic = "lwt/self"
	c := pod.Connect(t, clientID, func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = willTopic
		p.Connect.WillPayload = []byte("offline")
		p.Connect.WillQos = 1
		p.Connect.WillRetain = true
	})
	defer c.Close()

	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetDeadBrokerGrace(0)
	if err := jt.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	// Tick stops earlier, at "my own lock is not held". Go straight at
	// the reap as well so the identity check is exercised on its own.
	claimed, err := jt.HandleDeadBrokerForTest(context.Background(), pod.BrokerID)
	if err != nil {
		t.Fatalf("handleDeadBroker(self): %v", err)
	}
	if claimed {
		t.Errorf("janitor claimed its own broker as dead")
	}

	if s := readSession(t, mh, clientID); !s.owned() || *s.brokerID != pod.BrokerID {
		t.Errorf("janitor reaped its own broker's session: %+v", s)
	}
	var retained int
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM retained WHERE topic=$1`, willTopic).Scan(&retained); err != nil {
		t.Fatalf("read retained: %v", err)
	}
	if retained != 0 {
		t.Errorf("janitor published the will of its own connected client")
	}
}

// TestJanitorJudgesNobodyWithoutOwnLock: while this broker's own lock is
// missing, Postgres has (as far as it can tell) just dropped everyone's,
// so a peer's free lock is not evidence. Grace is zero so the own-lock
// check is the only thing in the way.
func TestJanitorJudgesNobodyWithoutOwnLock(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	peer := uuid.New()
	seedSessionOwnedBy(t, mh, "nolock-ghost", peer)

	jt, tick := fakeClockJanitor(t, mh, "nolock-ghost")
	jt.SetDeadBrokerGrace(0)

	if s := tick(0); !s.owned() {
		t.Fatalf("reaped a peer while our own lock was not held: %+v", s)
	}
	holdBrokerLock(t, mh, mh.Pods[0].BrokerID)
	if s := tick(0); s.brokerID != nil {
		t.Fatalf("peer not reaped once our own lock was held: %+v", s)
	}
}

// TestJanitorReapsPeerOnlyAfterGrace: a free lock starts a clock; the
// broker is reaped once the lock has stayed free for the full grace.
func TestJanitorReapsPeerOnlyAfterGrace(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	startListener(t, mh)
	peer := uuid.New()
	seedSessionOwnedBy(t, mh, "grace-ghost", peer)

	_, tick := fakeClockJanitor(t, mh, "grace-ghost")
	if s := tick(0); !s.owned() {
		t.Fatalf("reaped on first sight of a free lock: %+v", s)
	}
	if s := tick(janitor.DeadBrokerGrace - time.Second); !s.owned() {
		t.Fatalf("reaped inside the grace: %+v", s)
	}
	if s := tick(time.Second); s.brokerID != nil || s.connected {
		t.Fatalf("not reaped after the grace: %+v", s)
	}
}

// TestJanitorGraceRestartsWhenLockReheld: the grace measures one unbroken
// stretch. A broker that got its lock back and loses it again later is
// owed a full grace from the second loss, not the remainder of the first.
func TestJanitorGraceRestartsWhenLockReheld(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	startListener(t, mh)
	peer := uuid.New()
	seedSessionOwnedBy(t, mh, "flap-ghost", peer)

	_, tick := fakeClockJanitor(t, mh, "flap-ghost")
	if s := tick(0); !s.owned() {
		t.Fatalf("reaped on first sight of a free lock: %+v", s)
	}

	// The peer's listener comes back...
	release := holdBrokerLock(t, mh, peer)
	if s := tick(10 * time.Second); !s.owned() {
		t.Fatalf("reaped while the lock was held: %+v", s)
	}
	// ...and drops again, with the first clock already past the grace.
	release()
	if s := tick(janitor.DeadBrokerGrace); !s.owned() {
		t.Fatalf("reaped on the stale clock from before the lock was re-held: %+v", s)
	}
	if s := tick(janitor.DeadBrokerGrace - time.Second); !s.owned() {
		t.Fatalf("reaped inside the second grace: %+v", s)
	}
	if s := tick(time.Second); s.brokerID != nil {
		t.Fatalf("not reaped after a full grace from the second loss: %+v", s)
	}
}

// TestJanitorGraceRestartsWhenOwnLockInterrupted: the janitor samples a
// peer's lock every few seconds, so "free at every sample" cannot rule out
// the peer having held it in between. What can: our own lock sharing the
// peer's fate. If ours was lost at any point since the clock started,
// Postgres may have dropped and restored everyone's, and the clock is
// void. Covers both ways of finding out: the lock is missing right now,
// and the listener reports it was lost and re-taken between two ticks.
func TestJanitorGraceRestartsWhenOwnLockInterrupted(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	peer := uuid.New()
	seedSessionOwnedBy(t, mh, "bounce-ghost", peer)

	var epoch atomic.Uint64
	jt, tick := fakeClockJanitor(t, mh, "bounce-ghost")
	jt.SetLockEpoch(epoch.Load)

	release := holdBrokerLock(t, mh, mh.Pods[0].BrokerID)
	if s := tick(0); !s.owned() {
		t.Fatalf("reaped on first sight of a free lock: %+v", s)
	}

	// Our lock is missing at the tick where the peer's grace would end.
	release()
	if s := tick(janitor.DeadBrokerGrace); !s.owned() {
		t.Fatalf("reaped a peer while our own lock was not held: %+v", s)
	}
	// It is back: the peer gets a full grace from here.
	holdBrokerLock(t, mh, mh.Pods[0].BrokerID)
	if s := tick(time.Second); !s.owned() {
		t.Fatalf("reaped on a clock from before our own lock was lost: %+v", s)
	}

	// Our lock was lost and re-taken entirely between two ticks.
	epoch.Add(1)
	if s := tick(janitor.DeadBrokerGrace); !s.owned() {
		t.Fatalf("reaped on a clock spanning a loss of our own lock: %+v", s)
	}
	if s := tick(janitor.DeadBrokerGrace - time.Second); !s.owned() {
		t.Fatalf("reaped inside the grace: %+v", s)
	}
	if s := tick(time.Second); s.brokerID != nil {
		t.Fatalf("not reaped after a full grace under an unbroken own lock: %+v", s)
	}
}
