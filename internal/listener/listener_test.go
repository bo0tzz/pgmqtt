package listener_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"
	dto "github.com/prometheus/client_model/go"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

func newPodLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestCrossPodFanout(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	for _, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	subClient := mh.Pods[0].Connect(t, "sub-cross")
	pubClient := mh.Pods[1].Connect(t, "pub-cross")
	defer subClient.Close()
	defer pubClient.Close()

	subClient.Subscribe(t, "x/#", 1)

	pubClient.Publish(t, "x/y", []byte("hi"), 1, false)

	pk := subClient.Read(t, 10*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if string(pk.Payload) != "hi" {
		t.Errorf("payload = %q", pk.Payload)
	}
}

func TestSessionMigratesOnPodLoss(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	listeners := make(map[int]*listener.Listener)
	for i, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		listeners[i] = l
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	// Persistent v5 client on pod 0 with a subscription, then disconnect cleanly.
	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	}
	c1 := mh.Pods[0].Connect(t, "migrant", persistent)
	c1.Subscribe(t, "mig/#", 1)
	c1.Close()

	// "Kill" pod 0 by stopping its listener — releases its advisory lock.
	// Any session rows still pointing at broker 0 would now be reclaimable.
	listeners[0].Stop()

	// Reconnect same client_id on pod 1 — it should succeed (the dead pod's
	// takeover NOTIFY is unobserved but irrelevant) and the subscription must
	// still be intact.
	c2 := mh.Pods[1].Connect(t, "migrant", persistent)
	defer c2.Close()

	pub := mh.Pods[1].Connect(t, "mig-pub")
	defer pub.Close()
	pub.Publish(t, "mig/x", []byte("alive"), 1, false)

	pk := c2.Read(t, 10*time.Second)
	if pk.FixedHeader.Type != packets.Publish || string(pk.Payload) != "alive" {
		t.Fatalf("post-migration delivery failed: type=%d payload=%q", pk.FixedHeader.Type, pk.Payload)
	}
}

// TestListenerReconnectsOnTransientError simulates a transient PG-side
// failure by killing the listener's backend via pg_terminate_backend mid-
// WaitForNotification, then asserts:
//
//   - the listener_restarts_total{reason="wait_error"} counter increments,
//   - the listener resumes serving NOTIFYs (a publish on another conn
//     reaches a subscriber pinned to the listener's pod).
//
// Catches the bug: pre-fix, the listener's run() loop returned on any
// non-EOF wait error so subsequent cross-broker publishes silently
// dropped without a metric or restart.
func TestListenerReconnectsOnTransientError(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	mtxs := make([]*metrics.Metrics, len(mh.Pods))
	for i, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		t.Cleanup(l.Stop)
		mtxs[i] = metrics.New()
		l.SetMetrics(mtxs[i])
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	// Subscriber lives on pod 0 — its delivery requires pod 0's listener
	// to receive the cross-broker NOTIFY emitted by pod 1's publishCore.
	sub := mh.Pods[0].Connect(t, "rcn-sub")
	defer sub.Close()
	sub.Subscribe(t, "rcn/#", 1)

	pub := mh.Pods[1].Connect(t, "rcn-pub")
	defer pub.Close()

	// Sanity: cross-broker publish works pre-disruption.
	pub.Publish(t, "rcn/pre", []byte("pre"), 1, false)
	pk := sub.Read(t, 10*time.Second)
	if pk.FixedHeader.Type != packets.Publish || string(pk.Payload) != "pre" {
		t.Fatalf("pre-kill: got type=%d payload=%q", pk.FixedHeader.Type, pk.Payload)
	}

	// Kill the listener backends for THIS test's database. Tests run in
	// parallel against the same testcontainer Postgres instance (each test
	// gets a fresh DB inside it); we must filter by datname so we don't
	// nuke sibling tests' listener backends and blow past their reconnect
	// retry budget. pg_listening_channels() reports only the current
	// conn's channels so we can't filter per-pid by listen channel from
	// the pool side; broad-terminate inside our own database is fine —
	// the other pod's listener will also reconnect, no-op for the
	// assertion below.
	if _, err := mh.Pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid)
		  FROM pg_stat_activity
		 WHERE application_name = 'pgmqttd-listener'
		   AND datname = current_database()
		   AND pid <> pg_backend_pid()
	`); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}

	// Wait for the listener to observe its conn dying and successfully
	// reconnect. Reconnect-initial-backoff is 1 s so we give it generous
	// headroom. Poll the metric.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var pb dto.Metric
		if err := mtxs[0].ListenerRestartsTotal.WithLabelValues("wait_error").Write(&pb); err == nil {
			if pb.GetCounter().GetValue() >= 1 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	var pb dto.Metric
	if err := mtxs[0].ListenerRestartsTotal.WithLabelValues("wait_error").Write(&pb); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if got := pb.GetCounter().GetValue(); got < 1 {
		t.Fatalf("listener_restarts_total{wait_error}: got %g, want >=1", got)
	}

	// Wait until the listener has actually re-registered LISTEN. The
	// initial backoff is 1 s and dialAndRegister is a few ms, so 2 s is a
	// safe upper bound. We poll pg_stat_activity for a fresh backend with
	// our application_name; a successful reconnect appears as soon as
	// pgx finishes the LISTEN sequence on the new conn.
	deadline = time.Now().Add(8 * time.Second)
	wantBackends := len(mh.Pods)
	for time.Now().Before(deadline) {
		var n int
		if err := mh.Pool.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			 WHERE application_name = 'pgmqttd-listener'
			   AND datname = current_database()
			   AND state = 'idle'`).Scan(&n); err == nil && n >= wantBackends {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Post-reconnect: the cross-broker publish path must work again. Use a
	// retry loop on the first publish to absorb the small window between
	// "new backend visible in pg_stat_activity" and "PG NOTIFY routing
	// table updated to include the new LISTEN registration". Each attempt
	// publishes a fresh topic so a stuck deliver from a previous attempt
	// can't accidentally satisfy the assertion.
	pub2 := mh.Pods[1].Connect(t, "rcn-pub2")
	defer pub2.Close()
	const attempts = 10
	gotPost := false
	for i := 0; i < attempts; i++ {
		topic := fmt.Sprintf("rcn/post/%d", i)
		pub2.Publish(t, topic, []byte("post"), 1, false)
		if err := sub.Conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			t.Fatalf("set deadline: %v", err)
		}
		next, err := sub.NextRaw()
		_ = sub.Conn.SetReadDeadline(time.Time{})
		if err == nil && next.FixedHeader.Type == packets.Publish && string(next.Payload) == "post" {
			gotPost = true
			break
		}
	}
	if !gotPost {
		t.Fatalf("post-reconnect publish never reached subscriber after %d attempts", attempts)
	}
}

// TestStaleTakeoverNotifyDoesNotKillFreshConn pins the token-aware
// takeover semantics. A stale takeover NOTIFY (carrying the session_token
// of a Conn that no longer exists) must NOT shut down a freshly-
// reconnected Conn that owns a different token. Without the guard, a
// reconnect storm could see PodA → PodB → PodA play out, with PodB's
// earlier-queued notify arriving at PodA after the second reconnect and
// killing the legitimate fresh Conn.
func TestStaleTakeoverNotifyDoesNotKillFreshConn(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, newPodLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	// Connect a client; current token is whatever takeOwnership rotated
	// to (call it T_fresh).
	tc := pod.Connect(t, "stale-takeover-victim")
	defer tc.Close()
	conn, ok := pod.Engine.ConnFor("stale-takeover-victim")
	if !ok {
		t.Fatalf("conn not registered")
	}
	freshToken := conn.SessionToken()

	// Emit a stale takeover notify with a random (unrelated) prevToken
	// — this represents the PodA→PodB→PodA race where PodB's notify
	// (carrying the now-superseded token) arrives late.
	staleToken := uuid.New()
	if staleToken == freshToken {
		staleToken = uuid.New() // astronomically unlikely; just guard
	}
	if _, err := mh.Pool.Exec(context.Background(),
		`SELECT pg_notify($1, $2)`,
		"pgmqtt_takeover_"+l.BrokerID().String(),
		staleToken.String()+"stale-takeover-victim"); err != nil {
		t.Fatalf("emit stale notify: %v", err)
	}

	// Give the listener a moment to process it. The fresh Conn must
	// still be alive.
	time.Sleep(150 * time.Millisecond)
	if err := tc.Conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := tc.NextRaw(); err == nil {
		t.Fatalf("fresh conn was unexpectedly closed by a stale takeover notify")
	}
	// Now confirm a matching takeover (with the fresh token) DOES close
	// it — otherwise the test wouldn't be testing the right thing.
	if _, err := mh.Pool.Exec(context.Background(),
		`SELECT pg_notify($1, $2)`,
		"pgmqtt_takeover_"+l.BrokerID().String(),
		freshToken.String()+"stale-takeover-victim"); err != nil {
		t.Fatalf("emit matching notify: %v", err)
	}
	if err := tc.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := tc.NextRaw(); err == nil {
		t.Fatalf("matching takeover failed to close conn")
	}
}

// TestTakeoverDoesNotClobberNewSession exercises BUG-1: a takeover-driven
// Shutdown of a stale Conn (handleDisconnect on the prior owner) must NOT
// wipe the broker_id of the new owner that has since taken over the
// sessions row. The persist-path UPDATE (broker_id=NULL, ...) is token-
// scoped exactly like the clean-session DELETE.
//
// Scenario: persistent client (no-clean, expiry>0) connects to pod 0;
// reconnects on pod 1. Pod 0's listener fires the takeover Shutdown of
// the now-stale Conn → handleDisconnect runs the persist-path UPDATE.
// If the UPDATE isn't token-scoped, it clears pod 1's broker_id back
// to NULL and the ownership sweeper kicks the healthy reconnect later.
// With the guard, pod 1's row survives unchanged.
func TestTakeoverDoesNotClobberNewSession(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	for _, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	}

	const clientID = "takeover-clobber"

	// First connect to pod 0.
	c1 := mh.Pods[0].Connect(t, clientID, persistent)
	defer c1.Close()

	// Reconnect same client_id on pod 1.
	c2 := mh.Pods[1].Connect(t, clientID, persistent)
	defer c2.Close()

	// Wait for the takeover NOTIFY to fire on pod 0's listener and
	// the stale Conn's handleDisconnect to complete its UPDATE. The
	// listener is async over pg_notify so we poll the sessions row.
	wantBroker := mh.Pods[1].BrokerID
	deadline := time.Now().Add(5 * time.Second)
	var (
		observedBroker *uuid.UUID
		observedConn   bool
	)
	for time.Now().Before(deadline) {
		var b *uuid.UUID
		var connected bool
		if err := mh.Pool.QueryRow(context.Background(),
			`SELECT broker_id, connected FROM sessions WHERE client_id=$1`,
			clientID).Scan(&b, &connected); err != nil {
			t.Fatalf("session lookup: %v", err)
		}
		observedBroker = b
		observedConn = connected
		// Give the stale handleDisconnect a window to clobber, then
		// re-read once more and assert.
		time.Sleep(50 * time.Millisecond)
		if b != nil && *b == wantBroker && connected {
			time.Sleep(300 * time.Millisecond)
			if err := mh.Pool.QueryRow(context.Background(),
				`SELECT broker_id, connected FROM sessions WHERE client_id=$1`,
				clientID).Scan(&b, &connected); err != nil {
				t.Fatalf("session re-read: %v", err)
			}
			observedBroker = b
			observedConn = connected
			break
		}
	}
	if observedBroker == nil {
		t.Fatalf("sessions.broker_id NULL after takeover — stale handleDisconnect clobbered the new owner")
	}
	if *observedBroker != wantBroker {
		t.Fatalf("sessions.broker_id=%s, want pod1=%s", observedBroker, wantBroker)
	}
	if !observedConn {
		t.Fatalf("sessions.connected=false after takeover — stale handleDisconnect clobbered the new owner")
	}
}

func TestTakeoverClosesPriorPodSocket(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	for _, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	first := mh.Pods[0].Connect(t, "client-A")
	defer first.Close()

	// Same client_id reconnects to a different pod.
	second := mh.Pods[1].Connect(t, "client-A")
	defer second.Close()

	// First connection should be closed by the takeover signal.
	if err := first.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	_, err := first.NextRaw()
	if err == nil {
		t.Fatalf("expected first conn to be closed; read succeeded")
	}
}
