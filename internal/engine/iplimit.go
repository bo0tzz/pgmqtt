package engine

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ipLimiter mitigates bcrypt-CPU DoS by metering CONNECTs and auth-failures
// per source IP. It exposes two token buckets per IP plus a transient
// "penalty box" that drops further CONNECTs from an IP that has exhausted
// its auth-failure budget — so the bcrypt comparison is skipped entirely.
//
// Why two buckets:
//
//   * connect — caps the raw rate at which a single source IP can open
//     sockets. Bursts up to `connectsPerSec` per second; overflow is
//     dropped with a hard close (no CONNACK), since CONNACK-rejecting
//     fans the flames (the attacker just immediately retries).
//
//   * auth-failure — caps the rate at which a single IP can submit
//     credentials that fail the bcrypt check. When this bucket is
//     exhausted, the IP enters penaltyDuration of "no more bcrypts"
//     where every CONNECT is dropped pre-bcrypt, so the bcrypt CPU
//     pressure tied to that IP collapses to ~0 for the cool-off.
//
// Storage is a sync.Map keyed on netip.Addr (comparable, no allocations
// per lookup). Cold entries (idle > gcIdle) are reaped by a background
// sweep so a long-running broker can't accumulate per-IP entries
// indefinitely. The sweep goroutine exits on the engine context.
//
// 0 disables the limiter — both allowConnect and inPenaltyBox become
// no-ops (true / false respectively).
type ipLimiter struct {
	connectsPerSec     int
	authFailuresPerMin int

	entries sync.Map // map[netip.Addr]*ipEntry

	// Configurable for tests; production uses the constants below.
	penaltyDuration time.Duration
	gcIdle          time.Duration
	gcInterval      time.Duration

	// nowFunc is overridable for tests so penalty-box / GC timing can be
	// asserted without sleeping. Defaults to time.Now.
	nowFunc func() time.Time
}

type ipEntry struct {
	connect  *rate.Limiter
	auth     *rate.Limiter
	mu       sync.Mutex
	until    time.Time // penalty-box expiry; zero = not in box
	lastSeen time.Time
}

const (
	defaultPenaltyDuration = 60 * time.Second
	defaultGCIdle          = 5 * time.Minute
	defaultGCInterval      = 1 * time.Minute
)

// newIPLimiter returns a limiter with the supplied per-IP rates. When both
// rates are 0, the limiter is fully disabled — allowConnect always returns
// true, recordAuthFailure is a no-op, and no GC goroutine is started.
//
// connectsPerSec doubles as the burst capacity of the connect bucket — a
// freshly-seen IP can issue connectsPerSec CONNECTs back-to-back before
// the rate kicks in. authFailuresPerMin similarly doubles as the
// auth-failure burst — N failures back-to-back before the IP enters the
// penalty box.
//
// The caller passes a context that, when cancelled, stops the GC sweep.
func newIPLimiter(ctx context.Context, connectsPerSec, authFailuresPerMin int) *ipLimiter {
	l := &ipLimiter{
		connectsPerSec:     connectsPerSec,
		authFailuresPerMin: authFailuresPerMin,
		penaltyDuration:    defaultPenaltyDuration,
		gcIdle:             defaultGCIdle,
		gcInterval:         defaultGCInterval,
		nowFunc:            time.Now,
	}
	if l.disabled() {
		return l
	}
	go l.gcLoop(ctx)
	return l
}

func (l *ipLimiter) disabled() bool {
	return l.connectsPerSec <= 0 && l.authFailuresPerMin <= 0
}

// allowConnect returns false when the IP has exceeded the configured
// CONNECTs-per-second budget. Caller is expected to have already
// short-circuited via inPenaltyBox; this method handles the per-second
// connect bucket only.
//
// Empty / unparseable IP strings are treated as "allow" — the engine's
// caller already only invokes this when RemoteAddr is well-formed.
func (l *ipLimiter) allowConnect(ip string) bool {
	if l == nil || l.connectsPerSec <= 0 {
		return true
	}
	addr, ok := parseIP(ip)
	if !ok {
		return true
	}
	e := l.entryFor(addr, true)
	if e.connect == nil {
		return true
	}
	now := l.nowFunc()
	e.mu.Lock()
	e.lastSeen = now
	e.mu.Unlock()
	return e.connect.AllowN(now, 1)
}

// recordAuthFailure ticks the auth-failure bucket for ip. When the
// bucket is exhausted the IP enters a `penaltyDuration` cool-off during
// which inPenaltyBox returns true. Already-in-box IPs are no-ops here —
// the box is sticky for the configured window.
func (l *ipLimiter) recordAuthFailure(ip string) {
	if l == nil || l.authFailuresPerMin <= 0 {
		return
	}
	addr, ok := parseIP(ip)
	if !ok {
		return
	}
	e := l.entryFor(addr, true)
	if e.auth == nil {
		return
	}
	now := l.nowFunc()
	e.mu.Lock()
	e.lastSeen = now
	if !e.until.IsZero() && now.Before(e.until) {
		// Already in box — don't extend, just refresh lastSeen.
		e.mu.Unlock()
		return
	}
	allowed := e.auth.AllowN(now, 1)
	if !allowed {
		e.until = now.Add(l.penaltyDuration)
	}
	e.mu.Unlock()
}

// inPenaltyBox returns true when ip is currently in the bcrypt-skip
// cool-off period. False when the limiter is disabled or the IP has
// never tripped the auth-failure bucket.
func (l *ipLimiter) inPenaltyBox(ip string) bool {
	if l == nil || l.authFailuresPerMin <= 0 {
		return false
	}
	addr, ok := parseIP(ip)
	if !ok {
		return false
	}
	e := l.entryFor(addr, false)
	if e == nil {
		return false
	}
	now := l.nowFunc()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.until.IsZero() {
		return false
	}
	if now.Before(e.until) {
		return true
	}
	// Penalty expired — clear so subsequent failures rearm the box from a
	// fresh bucket rather than stay sticky.
	e.until = time.Time{}
	return false
}

// entryFor returns the per-IP entry. When create is true (hot path) it
// allocates a fresh entry and races against other callers for the same
// IP via LoadOrStore; when create is false (read-only paths like
// inPenaltyBox) it returns nil for never-seen IPs so the caller can
// short-circuit.
func (l *ipLimiter) entryFor(addr netip.Addr, create bool) *ipEntry {
	if v, ok := l.entries.Load(addr); ok {
		return v.(*ipEntry)
	}
	if !create {
		return nil
	}
	now := l.nowFunc()
	e := &ipEntry{lastSeen: now}
	if l.connectsPerSec > 0 {
		e.connect = rate.NewLimiter(rate.Limit(l.connectsPerSec), l.connectsPerSec)
	}
	if l.authFailuresPerMin > 0 {
		// rate.Limiter takes events/sec; convert per-minute to per-second.
		// Burst stays at the per-minute count so an IP can fail
		// `authFailuresPerMin` times back-to-back before tripping the box.
		perSec := rate.Limit(float64(l.authFailuresPerMin) / 60.0)
		e.auth = rate.NewLimiter(perSec, l.authFailuresPerMin)
	}
	actual, _ := l.entries.LoadOrStore(addr, e)
	return actual.(*ipEntry)
}

// gcLoop reaps entries that haven't been touched in gcIdle. Cheap — one
// pass per gcInterval, O(n_active_IPs). Exits on ctx cancel.
func (l *ipLimiter) gcLoop(ctx context.Context) {
	t := time.NewTicker(l.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.gcOnce()
		}
	}
}

// gcOnce reaps stale entries. Exposed (lower-case) for tests to drive
// without waiting on the ticker.
func (l *ipLimiter) gcOnce() {
	cutoff := l.nowFunc().Add(-l.gcIdle)
	l.entries.Range(func(k, v any) bool {
		e := v.(*ipEntry)
		e.mu.Lock()
		idle := e.lastSeen.Before(cutoff)
		// Don't reap an IP that's still in its penalty box — the box is
		// the whole reason the entry exists.
		inBox := !e.until.IsZero() && l.nowFunc().Before(e.until)
		e.mu.Unlock()
		if idle && !inBox {
			l.entries.Delete(k)
		}
		return true
	})
}

// len returns the number of live entries. Test-only.
func (l *ipLimiter) len() int {
	if l == nil {
		return 0
	}
	n := 0
	l.entries.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// parseIP extracts a normalized netip.Addr from a host:port or bare host.
// IPv4-mapped IPv6 addresses are unmapped so 4-in-6 doesn't double-count
// against an IPv4 source.
func parseIP(s string) (netip.Addr, bool) {
	if s == "" {
		return netip.Addr{}, false
	}
	// SplitHostPort handles both bracketed v6 ([::1]:1883) and v4 (1.2.3.4:1883).
	host, _, err := net.SplitHostPort(s)
	if err != nil {
		// Fall back to bare-host parse — RemoteAddr() can omit the port
		// for unix sockets and certain test transports.
		host = s
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}
