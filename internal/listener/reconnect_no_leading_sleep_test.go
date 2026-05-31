package listener_test

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// readListenerRestarts returns the current value of
// listener_restarts_total{reason=reason}; 0 if the counter has never
// been observed for that label. Tests gate on this to detect when the
// listener has entered a particular branch.
func readListenerRestarts(t *testing.T, mtx *metrics.Metrics, reason string) float64 {
	t.Helper()
	var pb dto.Metric
	if err := mtx.ListenerRestartsTotal.WithLabelValues(reason).Write(&pb); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return pb.GetCounter().GetValue()
}

// TestReconnectDoesNotSleepBeforeFirstAttempt pins BUG-X: the old
// reconnect path slept reconnectInitialBackoff BEFORE the first attempt,
// giving peer Pods' dead-broker reapers a 1-16 s window to grab the
// (now-unheld) per-broker advisory lock and kick every owning client.
// The fix runs the first attempt immediately and only sleeps after a
// FAILED attempt.
//
// We measure the time between "listener observes its conn dying" and
// "listener has re-registered LISTEN" (visible as a fresh pgmqttd-
// listener backend in pg_stat_activity). Pre-fix this is ~1 s; post-fix
// it's the dial+LISTEN round-trip, which is reliably <750 ms against a
// local testcontainer Postgres.
func TestReconnectDoesNotSleepBeforeFirstAttempt(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, newPodLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	mtx := metrics.New()
	l.SetMetrics(mtx)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	// Wait until the listener backend is visible.
	deadline := time.Now().Add(5 * time.Second)
	var origPID int32
	for time.Now().Before(deadline) {
		err := mh.Pool.QueryRow(context.Background(), `
			SELECT pid FROM pg_stat_activity
			 WHERE application_name = 'pgmqttd-listener'
			   AND datname = current_database()
			   AND state = 'idle'
			 LIMIT 1`).Scan(&origPID)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if origPID == 0 {
		t.Fatalf("listener backend never appeared")
	}

	// Kill the listener's backend. The listener observes EOF on
	// WaitForNotification and enters reconnect.
	terminateStart := time.Now()
	if _, err := mh.Pool.Exec(context.Background(),
		`SELECT pg_terminate_backend($1)`, origPID); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}

	// Poll until a FRESH (different pid) listener backend appears.
	// Pre-fix this would take >=1 s (the leading sleep). Post-fix it's
	// the dial round-trip, comfortably under 750 ms against a local
	// testcontainer.
	const budget = 750 * time.Millisecond
	pollDeadline := terminateStart.Add(budget)
	var (
		freshPID  int32
		reconnect time.Duration
	)
	for time.Now().Before(pollDeadline) {
		err := mh.Pool.QueryRow(context.Background(), `
			SELECT pid FROM pg_stat_activity
			 WHERE application_name = 'pgmqttd-listener'
			   AND datname = current_database()
			   AND pid <> $1
			   AND state = 'idle'
			 LIMIT 1`, origPID).Scan(&freshPID)
		if err == nil && freshPID != 0 {
			reconnect = time.Since(terminateStart)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if freshPID == 0 {
		t.Fatalf("listener did not reconnect within %s (BUG-X regression: leading sleep reintroduced)", budget)
	}
	t.Logf("reconnect observed in %s (budget %s)", reconnect, budget)

	// Sanity: a wait_error was actually counted.
	if got := readListenerRestarts(t, mtx, "wait_error"); got < 1 {
		t.Fatalf("listener_restarts_total{wait_error}=%g, want >=1", got)
	}
}
