package operator

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func metricValue(t *testing.T, m prometheus.Metric) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatalf("read metric: %v", err)
	}
	if out.Counter != nil {
		return out.Counter.GetValue()
	}
	return out.Gauge.GetValue()
}

// syncBuffer lets the supervisor goroutine log while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testSupervisor returns a supervisor with millisecond timings, its log
// output, and a function that runs it and reports when run returned.
func testSupervisor(t *testing.T) (*supervisor, *syncBuffer, func(ctx context.Context) <-chan struct{}) {
	t.Helper()
	logs := &syncBuffer{}
	s := &supervisor{
		logger:          slog.New(slog.NewTextHandler(logs, nil)),
		crdServed:       func(context.Context) (bool, error) { return true, nil },
		crdPollInterval: time.Millisecond,
		minBackoff:      time.Millisecond,
		maxBackoff:      4 * time.Millisecond,
		stableAfter:     time.Hour,
	}
	start := func(ctx context.Context) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.run(ctx)
		}()
		return done
	}
	return s, logs, start
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func mustStop(t *testing.T, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not return after its context was cancelled")
	}
}

// TestSupervisorRestartsManagerThatStops is the case from production: the
// manager returns "leader election lost" after an API server blip. It used
// to be started once per process, so that pod never reconciled again.
func TestSupervisorRestartsManagerThatStops(t *testing.T) {
	s, logs, start := testSupervisor(t)
	var runs atomic.Int32
	s.runManager = func(ctx context.Context) error {
		if runs.Add(1) <= 2 {
			return errors.New("manager: leader election lost")
		}
		<-ctx.Done()
		return nil
	}
	restartsBefore := metricValue(t, managerRestartsTotal)

	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	waitFor(t, "the third manager to be started", func() bool { return runs.Load() == 3 })
	waitFor(t, "the running gauge", func() bool { return metricValue(t, managerRunning) == 1 })

	select {
	case <-done:
		t.Fatal("supervisor returned while its context was live")
	default:
	}
	if got := metricValue(t, managerRestartsTotal) - restartsBefore; got != 2 {
		t.Errorf("restart counter moved by %v, want 2", got)
	}
	if out := logs.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "leader election lost") {
		t.Errorf("manager exit should be logged at ERROR with its error, got:\n%s", out)
	}

	mustStop(t, cancel, done)
	if runs.Load() != 3 {
		t.Errorf("manager started %d times, want 3: shutdown must not count as a failure", runs.Load())
	}
	if got := metricValue(t, managerRestartsTotal) - restartsBefore; got != 2 {
		t.Errorf("restart counter moved by %v after shutdown, want 2", got)
	}
	if got := metricValue(t, managerRunning); got != 0 {
		t.Errorf("running gauge = %v after shutdown, want 0", got)
	}
}

// TestSupervisorRetriesWhenDiscoveryFails: one failed discovery call at
// pod start used to be read as "CRD not installed" and disabled the
// reconciler for the life of the pod.
func TestSupervisorRetriesWhenDiscoveryFails(t *testing.T) {
	s, logs, start := testSupervisor(t)
	var probes atomic.Int32
	s.crdServed = func(context.Context) (bool, error) {
		if probes.Add(1) <= 3 {
			return false, errors.New("dial tcp 10.96.0.1:443: connect: connection refused")
		}
		return true, nil
	}
	started := make(chan struct{})
	s.runManager = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	}
	errorsBefore := metricValue(t, discoveryErrorsTotal)

	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("manager never started after discovery recovered")
	}
	mustStop(t, cancel, done)

	if got := metricValue(t, discoveryErrorsTotal) - errorsBefore; got != 3 {
		t.Errorf("discovery error counter moved by %v, want 3", got)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "connection refused") {
		t.Errorf("a discovery failure should be logged at WARN with the error, got:\n%s", out)
	}
	if strings.Contains(out, "not installed") {
		t.Errorf("a discovery failure was reported as a missing CRD:\n%s", out)
	}
}

// TestSupervisorPicksUpCRDInstalledLater covers crds.install=false with
// the CRD applied after the broker Pods are up.
func TestSupervisorPicksUpCRDInstalledLater(t *testing.T) {
	s, logs, start := testSupervisor(t)
	var probes atomic.Int32
	s.crdServed = func(context.Context) (bool, error) {
		return probes.Add(1) > 5, nil
	}
	started := make(chan struct{})
	s.runManager = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("manager never started after the CRD appeared")
	}
	mustStop(t, cancel, done)

	out := logs.String()
	if n := strings.Count(out, "is not installed"); n != 1 {
		t.Errorf("the missing CRD should be logged once, not on every poll; logged %d times:\n%s", n, out)
	}
	if !strings.Contains(out, "is now installed") {
		t.Errorf("the CRD appearing should be logged, got:\n%s", out)
	}
}

// TestSupervisorLooksForTheCRDAgainAfterAManagerStops: a manager whose CRD
// was removed under it stops (its caches cannot sync). The supervisor must
// go back to waiting rather than start managers in a loop.
func TestSupervisorLooksForTheCRDAgainAfterAManagerStops(t *testing.T) {
	s, _, start := testSupervisor(t)
	var crdPresent atomic.Bool
	crdPresent.Store(true)
	var probes atomic.Int32
	s.crdServed = func(context.Context) (bool, error) {
		probes.Add(1)
		return crdPresent.Load(), nil
	}
	var runs atomic.Int32
	s.runManager = func(ctx context.Context) error {
		runs.Add(1)
		crdPresent.Store(false)
		return errors.New("manager: failed to wait for pgmqtt-user caches to sync")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	waitFor(t, "the supervisor to poll for the CRD again", func() bool { return probes.Load() >= 10 })
	mustStop(t, cancel, done)

	if runs.Load() != 1 {
		t.Errorf("manager started %d times with the CRD gone, want 1", runs.Load())
	}
}

// TestSupervisorSurvivesPanics: a panic while discovering or running a
// manager is logged with its stack and handled like any other stop.
func TestSupervisorSurvivesPanics(t *testing.T) {
	s, logs, start := testSupervisor(t)
	var probes, runs atomic.Int32
	s.crdServed = func(context.Context) (bool, error) {
		if probes.Add(1) == 1 {
			panic("discovery bug")
		}
		return true, nil
	}
	s.runManager = func(ctx context.Context) error {
		if runs.Add(1) == 1 {
			panic("manager bug")
		}
		<-ctx.Done()
		return nil
	}
	restartsBefore := metricValue(t, managerRestartsTotal)

	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	waitFor(t, "a manager to run after both panics", func() bool { return runs.Load() == 2 })
	mustStop(t, cancel, done)

	out := logs.String()
	for _, want := range []string{"discovery bug", "manager bug", "stack="} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
	if got := metricValue(t, managerRestartsTotal) - restartsBefore; got != 1 {
		t.Errorf("restart counter moved by %v, want 1", got)
	}
}

func TestSupervisorReturnsWhileWaiting(t *testing.T) {
	for name, probe := range map[string]func(context.Context) (bool, error){
		"for the CRD": func(context.Context) (bool, error) { return false, nil },
		"on errors":   func(context.Context) (bool, error) { return false, errors.New("boom") },
	} {
		t.Run(name, func(t *testing.T) {
			s, _, start := testSupervisor(t)
			s.crdServed = probe
			s.crdPollInterval, s.minBackoff, s.maxBackoff = time.Hour, time.Hour, time.Hour
			s.runManager = func(context.Context) error {
				t.Error("manager started without a CRD")
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := start(ctx)
			time.Sleep(10 * time.Millisecond)
			mustStop(t, cancel, done)
		})
	}
}

func TestBackoff(t *testing.T) {
	b := backoff{min: time.Second, max: 8 * time.Second}
	for i, base := range []time.Duration{1, 2, 4, 8, 8, 8} {
		base *= time.Second
		if got := b.next(); got < base || got > base+base/5 {
			t.Errorf("step %d: %v, want between %v and %v", i, got, base, base+base/5)
		}
	}
	b.reset()
	if got := b.next(); got > time.Second+time.Second/5 {
		t.Errorf("after reset: %v, want about 1s", got)
	}
}

// TestSupervisorBackoffResetsAfterAStableRun: a manager that ran for a
// long time and then lost its lease should be restarted promptly, not
// after whatever delay earlier failures had built up.
func TestSupervisorBackoffResetsAfterAStableRun(t *testing.T) {
	s, logs, start := testSupervisor(t)
	s.minBackoff, s.maxBackoff = time.Millisecond, time.Hour
	s.stableAfter = 20 * time.Millisecond
	var runs atomic.Int32
	s.runManager = func(ctx context.Context) error {
		switch runs.Add(1) {
		case 1, 2, 3, 4:
			return errors.New("fails at once")
		case 5:
			time.Sleep(40 * time.Millisecond)
			return errors.New("leader election lost")
		}
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	// Four quick failures put the delay at 16ms; without the reset the
	// sixth start would wait 32ms or more.
	waitFor(t, "the restart after the stable run", func() bool { return runs.Load() == 6 })
	mustStop(t, cancel, done)

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	last := lines[len(lines)-1]
	_, rest, _ := strings.Cut(last, "restart_in=")
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		t.Fatalf("no restart_in in the last log line: %s", last)
	}
	delay, err := time.ParseDuration(fields[0])
	if err != nil {
		t.Fatalf("restart_in in %q: %v", last, err)
	}
	if delay > 2*time.Millisecond {
		t.Errorf("restart after a stable run waited %v, want the 1ms minimum", delay)
	}
}
