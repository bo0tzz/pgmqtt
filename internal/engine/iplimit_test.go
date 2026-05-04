package engine

import (
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock returns a Now() function reading from an *atomic.Int64 of unix
// nanos, so tests can advance virtual time without sleeping. Each test
// constructs its own clock to avoid state leakage.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(start.UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time     { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func newTestLimiter(t *testing.T, connectsPerSec, authFailuresPerMin int) (*ipLimiter, *fakeClock) {
	t.Helper()
	clk := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	// Build via the no-GC constructor so tests can mutate nowFunc /
	// gcIdle without racing the production sweep goroutine. Tests drive
	// gcOnce directly when they need eviction.
	l := newIPLimiterNoGC(connectsPerSec, authFailuresPerMin)
	l.nowFunc = clk.Now
	l.gcIdle = 100 * time.Millisecond
	l.gcInterval = 10 * time.Millisecond
	return l, clk
}

func TestIPLimiter_ConnectRate(t *testing.T) {
	l, clk := newTestLimiter(t, 5, 0)
	const ip = "203.0.113.1:54321"
	for i := 0; i < 5; i++ {
		if !l.allowConnect(ip) {
			t.Fatalf("connect %d: want allow, got reject", i+1)
		}
	}
	if l.allowConnect(ip) {
		t.Fatalf("connect 6: want reject, got allow")
	}
	// Refill a full second's worth of tokens; should be back to 5 burst.
	clk.Advance(1 * time.Second)
	for i := 0; i < 5; i++ {
		if !l.allowConnect(ip) {
			t.Fatalf("post-refill connect %d: want allow, got reject", i+1)
		}
	}
	if l.allowConnect(ip) {
		t.Fatalf("post-refill connect 6: want reject, got allow")
	}
}

func TestIPLimiter_AuthFailurePenaltyBox(t *testing.T) {
	l, clk := newTestLimiter(t, 0, 30)
	const ip = "198.51.100.7:443"
	if l.inPenaltyBox(ip) {
		t.Fatalf("fresh IP should not be in penalty box")
	}
	for i := 0; i < 30; i++ {
		l.recordAuthFailure(ip)
		if l.inPenaltyBox(ip) {
			t.Fatalf("auth failure %d: penalty box tripped early", i+1)
		}
	}
	// 31st failure exhausts the burst → enter the box.
	l.recordAuthFailure(ip)
	if !l.inPenaltyBox(ip) {
		t.Fatalf("auth failure 31: want penalty-box, got allow")
	}
	// Mid-box: still in box.
	clk.Advance(30 * time.Second)
	if !l.inPenaltyBox(ip) {
		t.Fatalf("mid-box: want still in box, got out")
	}
	// After full penalty duration: out.
	clk.Advance(31 * time.Second)
	if l.inPenaltyBox(ip) {
		t.Fatalf("post-penalty: want out, got still in box")
	}
}

func TestIPLimiter_DisabledRateAlwaysAllows(t *testing.T) {
	l, _ := newTestLimiter(t, 0, 0)
	const ip = "10.0.0.1:1883"
	for i := 0; i < 1000; i++ {
		if !l.allowConnect(ip) {
			t.Fatalf("connect %d: want allow with limiter disabled, got reject", i+1)
		}
	}
	// recordAuthFailure on disabled limiter should never put IP in box.
	for i := 0; i < 100; i++ {
		l.recordAuthFailure(ip)
	}
	if l.inPenaltyBox(ip) {
		t.Fatalf("disabled limiter should never put IP in penalty box")
	}
}

func TestIPLimiter_GCReapsColdEntries(t *testing.T) {
	l, clk := newTestLimiter(t, 5, 30)
	const ip1 = "192.0.2.1:1883"
	const ip2 = "192.0.2.2:1883"
	l.allowConnect(ip1)
	l.allowConnect(ip2)
	if got := l.len(); got != 2 {
		t.Fatalf("post-touch len: want 2, got %d", got)
	}
	// Advance past gcIdle so both entries qualify for reap.
	clk.Advance(200 * time.Millisecond)
	l.gcOnce()
	if got := l.len(); got != 0 {
		t.Fatalf("post-gc len: want 0, got %d", got)
	}
	// Re-touch one IP, advance, and confirm only the cold one is reaped.
	l.allowConnect(ip1)
	clk.Advance(50 * time.Millisecond)
	l.allowConnect(ip2) // ip2 is fresh
	clk.Advance(60 * time.Millisecond)
	// ip1 last touched 110ms ago (>gcIdle 100ms); ip2 60ms ago.
	l.gcOnce()
	if got := l.len(); got != 1 {
		t.Fatalf("partial-gc len: want 1, got %d", got)
	}
}

func TestIPLimiter_PenaltyBoxedEntryNotReaped(t *testing.T) {
	l, clk := newTestLimiter(t, 0, 1) // tiny burst → next failure trips box.
	const ip = "192.0.2.10:443"
	l.recordAuthFailure(ip) // consumes the only token
	l.recordAuthFailure(ip) // trips the box
	if !l.inPenaltyBox(ip) {
		t.Fatalf("expected penalty box after exhausting burst")
	}
	// Push past gcIdle but stay inside penaltyDuration. GC should NOT reap
	// the entry — that would let the IP escape its cool-off.
	clk.Advance(200 * time.Millisecond)
	l.gcOnce()
	if got := l.len(); got != 1 {
		t.Fatalf("in-box entry should not be GC'd: len want 1, got %d", got)
	}
	if !l.inPenaltyBox(ip) {
		t.Fatalf("entry preserved but penalty-box state lost")
	}
}

func TestIPLimiter_IPv4MappedV6Normalized(t *testing.T) {
	l, _ := newTestLimiter(t, 1, 0)
	v4 := "203.0.113.42:1883"
	v4in6 := "[::ffff:203.0.113.42]:1883"
	// Burst=1, so first call from each form should both share one bucket.
	if !l.allowConnect(v4) {
		t.Fatalf("v4 first call: want allow")
	}
	if l.allowConnect(v4in6) {
		t.Fatalf("v4-in-v6 second call: want reject (shared bucket)")
	}
}

func TestIPLimiter_NilOrUnparseable(t *testing.T) {
	l, _ := newTestLimiter(t, 1, 1)
	// Nil ipLimiter: all calls must be no-ops.
	var nilL *ipLimiter
	if !nilL.allowConnect("anything") {
		t.Fatalf("nil limiter should allow")
	}
	if nilL.inPenaltyBox("anything") {
		t.Fatalf("nil limiter should report not-in-box")
	}
	nilL.recordAuthFailure("anything") // must not panic
	// Unparseable IP returns true ("allow") so we don't deny legitimate
	// callers when the transport surfaces an unexpected RemoteAddr shape.
	if !l.allowConnect("not-a-valid-addr") {
		t.Fatalf("unparseable IP: want allow, got reject")
	}
}
