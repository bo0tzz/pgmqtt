package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

// newTestConn builds the minimum *Conn shell that AllocPacketID needs. We
// don't open a network socket; the allocator only touches eng.pool and
// clientID, plus its own atomic state. seedSession=true seeds an empty row in
// sessions so handleConnect-style tests that follow takeover work.
func newTestConn(t *testing.T, clientID string) (*Conn, func()) {
	t.Helper()
	pool := dbtest.FreshPool(t)
	cfg := &config.Config{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := New(context.Background(), cfg, pool, logger)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO sessions(client_id, protocol_version, clean_start) VALUES($1, 5, true)`,
		clientID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	c := &Conn{eng: eng, clientID: clientID}
	return c, func() {}
}

// TestAllocPacketIDFreshSession asserts that on a freshly-seeded session
// (no deliveries) AllocPacketID returns 1, 2, 3, ... starting from 1 (zero
// is reserved per spec).
func TestAllocPacketIDFreshSession(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "fresh")
	defer cleanup()

	ctx := context.Background()
	for i := uint16(1); i <= 5; i++ {
		got, err := c.AllocPacketID(ctx)
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		if got != i {
			t.Errorf("alloc %d: got %d, want %d", i, got, i)
		}
	}
}

// TestAllocPacketIDSeedFromMax asserts that on takeover (existing deliveries
// present), the first id is MAX(packet_id)+1. This is the production path —
// a resumed session must not re-issue an id that's still in flight.
func TestAllocPacketIDSeedFromMax(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "resumed")
	defer cleanup()

	ctx := context.Background()
	// Seed three messages, three deliveries, packet_ids 17..19.
	for i := 0; i < 3; i++ {
		var msgID int64
		if err := c.eng.pool.QueryRow(ctx, `
			INSERT INTO messages(topic, payload, qos, retain) VALUES('t', '\x00', 1, false)
			RETURNING id
		`).Scan(&msgID); err != nil {
			t.Fatalf("seed msg: %v", err)
		}
		if _, err := c.eng.pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, packet_id, state)
			VALUES($1, $2, 1, $3, 1)
		`, c.clientID, msgID, 17+i); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}

	got, err := c.AllocPacketID(ctx)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	// MAX is 19; first allocation must be 20.
	if got != 20 {
		t.Errorf("first alloc after seed: got %d, want 20", got)
	}
}

// TestAllocPacketIDCollisionRetry asserts that when the candidate id collides
// with an existing un-acked delivery, AllocPacketID skips ahead. We seed the
// counter to 0 (so first candidate = 1), pre-create deliveries with packet_id
// 1, 2, 3 — the allocator should return 4.
func TestAllocPacketIDCollisionRetry(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "collide")
	defer cleanup()

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		var msgID int64
		if err := c.eng.pool.QueryRow(ctx, `
			INSERT INTO messages(topic, payload, qos, retain) VALUES('t', '\x00', 1, false)
			RETURNING id
		`).Scan(&msgID); err != nil {
			t.Fatalf("seed msg: %v", err)
		}
		if _, err := c.eng.pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, packet_id, state)
			VALUES($1, $2, 1, $3, 1)
		`, c.clientID, msgID, i); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}
	// Force seed=0 (override the lazy seed to skip past MAX-based start).
	c.packetIDState.Store(0)
	c.packetIDSeeded.Store(true)

	got, err := c.AllocPacketID(ctx)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 4 {
		t.Errorf("collision retry: got %d, want 4", got)
	}
}

// TestAllocPacketIDExhaustsRetries asserts that when collisions occur for the
// full retry budget, AllocPacketID returns errNoPacketID. We pre-create
// deliveries with packet_id 1..N where N == retry limit and force seed=0.
func TestAllocPacketIDExhaustsRetries(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "exhaust")
	defer cleanup()

	ctx := context.Background()
	for i := 1; i <= allocPacketIDMaxRetries; i++ {
		var msgID int64
		if err := c.eng.pool.QueryRow(ctx, `
			INSERT INTO messages(topic, payload, qos, retain) VALUES('t', '\x00', 1, false)
			RETURNING id
		`).Scan(&msgID); err != nil {
			t.Fatalf("seed msg: %v", err)
		}
		if _, err := c.eng.pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, packet_id, state)
			VALUES($1, $2, 1, $3, 1)
		`, c.clientID, msgID, i); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}
	c.packetIDState.Store(0)
	c.packetIDSeeded.Store(true)

	if _, err := c.AllocPacketID(ctx); !errors.Is(err, errNoPacketID) {
		t.Fatalf("expected errNoPacketID, got %v", err)
	}
}

// TestAllocPacketIDWraps asserts that when the counter is at 65535, the next
// allocation wraps to 1 (skipping 0). We force-set the counter and verify.
func TestAllocPacketIDWraps(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "wrap")
	defer cleanup()

	ctx := context.Background()
	// Seed completes (no deliveries) -> packetIDState=0. Now manually set to
	// 65535 to test the wrap.
	if _, err := c.AllocPacketID(ctx); err != nil {
		t.Fatalf("warm seed: %v", err)
	}
	c.packetIDState.Store(65535)

	got, err := c.AllocPacketID(ctx)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 1 {
		t.Errorf("wrap: got %d, want 1", got)
	}

	// And the one after should be 2 (no skip).
	got, err = c.AllocPacketID(ctx)
	if err != nil {
		t.Fatalf("alloc 2: %v", err)
	}
	if got != 2 {
		t.Errorf("post-wrap: got %d, want 2", got)
	}
}

// TestAllocPacketIDConcurrent stresses concurrent allocation on a single
// *Conn. Two goroutines × 200 allocs must yield 400 distinct ids in 1..400
// (since seed=0, no collisions, no wrap).
func TestAllocPacketIDConcurrent(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "concurrent")
	defer cleanup()

	ctx := context.Background()
	const goroutines = 2
	const perGoroutine = 200

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[uint16]struct{}, goroutines*perGoroutine)
	)
	wg.Add(goroutines)
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				pid, err := c.AllocPacketID(ctx)
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				if _, dup := seen[pid]; dup {
					mu.Unlock()
					errCh <- errors.New("duplicate pid issued")
					return
				}
				seen[pid] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("alloc: %v", err)
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("expected %d unique ids, got %d", goroutines*perGoroutine, len(seen))
	}
}

// TestAllocPacketIDSeedZeroOnCleanStart asserts that on clean-start (no
// deliveries) the first allocated id is 1 — the seed read returns NULL,
// COALESCEs to 0, and Add(1)+map yields 1.
func TestAllocPacketIDSeedZeroOnCleanStart(t *testing.T) {
	t.Parallel()
	c, cleanup := newTestConn(t, "clean")
	defer cleanup()

	ctx := context.Background()
	got, err := c.AllocPacketID(ctx)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 1 {
		t.Errorf("clean-start first alloc: got %d, want 1", got)
	}
}
