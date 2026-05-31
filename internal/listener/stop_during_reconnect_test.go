package listener_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// TestListenerStopDuringReconnectIsClean pins the "MEDIUM listener
// osExit on ctx-cancel" finding: a Stop() that races the reconnect
// loop (so reconnect returns false because ctx was cancelled, not
// because it burned through retries) must NOT call osExit. The old
// code's run loop returned to "exhausted retries" unconditionally on
// reconnect=false, which would crash the pod on a clean shutdown.
//
// We point the listener at an unreachable URL via SetURLForTest before
// forcing a wait error, so every reconnect dial fails. With a 1 s
// initial backoff and 5 attempts the loop would burn ~31 s and then
// call osExit; calling Stop() during the first backoff must short-
// circuit that with a clean return.
func TestListenerStopDuringReconnectIsClean(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, newPodLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	mtx := metrics.New()
	l.SetMetrics(mtx)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	// Stub osExit so a "would crash" path is observable, not fatal.
	var osExitCalls atomic.Int32
	restore := listener.SetOSExitForTest(func(code int) {
		osExitCalls.Add(1)
	})
	defer restore()

	// Swap to an unreachable URL — port 1 on loopback. Every dial
	// fails fast (connection refused), so the reconnect loop spends
	// its time in the backoff sleep, which is the exact window
	// Stop() must short-circuit.
	l.SetURLForTest("postgres://nobody:nobody@127.0.0.1:1/nodb?sslmode=disable")

	// Force a wait error so the listener enters reconnect against
	// the unreachable URL.
	if _, err := mh.Pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid)
		  FROM pg_stat_activity
		 WHERE application_name = 'pgmqttd-listener'
		   AND datname = current_database()
		   AND pid <> pg_backend_pid()
	`); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}

	// Wait until wait_error ticks — that proves we're in reconnect.
	waitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitDeadline) {
		if readListenerRestarts(t, mtx, "wait_error") >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := readListenerRestarts(t, mtx, "wait_error"); got < 1 {
		t.Fatalf("listener never entered reconnect: wait_error=%g", got)
	}

	// Allow at least one failed reconnect attempt so we're in the
	// backoff sleep, not the brief dial window. 300 ms is plenty —
	// dialing localhost:1 errors in microseconds (connection refused).
	time.Sleep(300 * time.Millisecond)

	// Now Stop() during the backoff sleep. It must return promptly
	// and must NOT call osExit.
	stopStart := time.Now()
	done := make(chan struct{})
	go func() {
		l.Stop()
		close(done)
	}()
	select {
	case <-done:
		stopDuration := time.Since(stopStart)
		t.Logf("Stop() returned in %s", stopDuration)
		// Hard cap: Stop must beat a single backoff worth of waiting.
		if stopDuration > listener.ReconnectInitialBackoffForTest+500*time.Millisecond {
			t.Errorf("Stop() took %s (>= initial backoff); ctx cancel didn't short-circuit reconnect", stopDuration)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Stop() never returned — graceful shutdown is broken")
	}

	// The critical assertion: osExit must NOT have been called.
	if n := osExitCalls.Load(); n != 0 {
		t.Fatalf("osExit called %d times during Stop()-induced reconnect cancellation; want 0", n)
	}

	// Sanity: we should see a ctx_cancel restart counted (the fix's
	// new branch) — this proves we reached the new clean-return arm
	// rather than e.g. the old osExit path being silently no-op'd.
	if got := readListenerRestarts(t, mtx, "ctx_cancel"); got < 1 {
		t.Errorf("listener_restarts_total{ctx_cancel}=%g, want >=1 — new clean-return arm wasn't taken", got)
	}
}
