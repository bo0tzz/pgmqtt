package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
)

// TestPublishCapBoundary pins the publish-cap CTE boundary semantics that
// migrations 0010 (EXISTS+OFFSET short-circuit) and 0011 (off-by-one fix)
// codify in `mqtt_publish`.
//
// The cap reads "reject when in-flight depth >= cap", i.e. with cap=N the
// (N-1)th queued delivery still lands; the Nth triggers DISCONNECT 0x97.
// Sub-cases:
//   - cap=2, depth=1   → publish accepted (one row remains, sub gets msg).
//   - cap=2, depth=2   → publish rejected (DISCONNECT 0x97).
//   - cap=10000, depth=9999 → accepted (large-cap fast path).
//   - cap=10000, depth=10000 → rejected.
//   - state=3 exclusion: cap=3, depth=3 with one row at state=3 (acked,
//     awaiting delete). Migration 0010's `state IN (0,1,2)` filter means
//     state=3 rows must NOT count toward the cap; effective in-flight is 2
//     so publish accepted.
func TestPublishCapBoundary(t *testing.T) {
	t.Parallel()

	t.Run("cap=2 depth=1 accepted", func(t *testing.T) {
		t.Parallel()
		runCapBoundary(t, 2, 1, false)
	})
	t.Run("cap=2 depth=2 rejected", func(t *testing.T) {
		t.Parallel()
		runCapBoundary(t, 2, 2, true)
	})
	t.Run("cap=10000 depth=9999 accepted", func(t *testing.T) {
		t.Parallel()
		runCapBoundary(t, 10000, 9999, false)
	})
	t.Run("cap=10000 depth=10000 rejected", func(t *testing.T) {
		t.Parallel()
		runCapBoundary(t, 10000, 10000, true)
	})
	t.Run("state=3 row excluded from cap", func(t *testing.T) {
		t.Parallel()
		runStateThreeExclusion(t)
	})
}

// runCapBoundary seeds `seed` queued (state=0) delivery rows for "cap-sub"
// then publishes once on a different conn. expectReject=true asserts the
// publishing-side fanout drops the new row AND the broker DISCONNECTs the
// over-cap subscriber with reason 0x97.
func runCapBoundary(t *testing.T, cap, seed int, expectReject bool) {
	h := enginetest.NewHarness(t)
	h.Engine.SetMaxQueuedDeliveriesForTest(cap)

	sub := h.Connect(t, "cap-sub")
	defer sub.Close()
	sub.Subscribe(t, "cap/#", 1)

	ctx := context.Background()
	if seed > 0 {
		// Bulk-insert seed rows in a single round-trip so depth=10000 doesn't
		// take 10s of round-trips. One messages row, N delivery rows pointing
		// at it — the publish-cap CTE only counts deliveries.client_id rows.
		var msgID int64
		if err := h.Pool.QueryRow(ctx, `
			INSERT INTO messages(topic, payload, qos, retain) VALUES ('cap/seed', $1, 1, false)
			RETURNING id`, []byte("seed")).Scan(&msgID); err != nil {
			t.Fatalf("seed message: %v", err)
		}
		if _, err := h.Pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, state)
			SELECT 'cap-sub', $1, 1, 0 FROM generate_series(1, $2)`,
			msgID, seed); err != nil {
			t.Fatalf("seed deliveries: %v", err)
		}
	}

	pub := h.Connect(t, "cap-pub")
	defer pub.Close()
	pub.Publish(t, "cap/y", []byte("boundary"), 1, false)

	if expectReject {
		if err := sub.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("deadline: %v", err)
		}
		pk, err := sub.NextRaw()
		if err != nil {
			t.Fatalf("expected DISCONNECT, got read err: %v", err)
		}
		if pk.FixedHeader.Type != packets.Disconnect {
			t.Fatalf("expected DISCONNECT, got type=%d", pk.FixedHeader.Type)
		}
		if pk.ReasonCode != 0x97 {
			t.Fatalf("expected reason 0x97, got 0x%X", pk.ReasonCode)
		}
		return
	}

	// Accepted path: deliveries row count for cap-sub must increase by 1.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := h.Pool.QueryRow(ctx,
			`SELECT count(*) FROM deliveries WHERE client_id='cap-sub'`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == seed+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected deliveries depth %d after publish, never observed", seed+1)
}

// runStateThreeExclusion seeds three deliveries — state=0, state=1, state=3 —
// for cap=3. The CTE's `state IN (0,1,2)` filter means state=3 doesn't count;
// effective in-flight depth is 2 (states 0+1). cap=3 means depth<3 still
// admits the publish — without the filter the count would be 3 (= cap) and
// the publish would be rejected.
func runStateThreeExclusion(t *testing.T) {
	h := enginetest.NewHarness(t)
	h.Engine.SetMaxQueuedDeliveriesForTest(3)

	sub := h.Connect(t, "s3-sub")
	defer sub.Close()
	sub.Subscribe(t, "s3/#", 1)

	ctx := context.Background()
	var msgID int64
	if err := h.Pool.QueryRow(ctx, `
		INSERT INTO messages(topic, payload, qos, retain) VALUES ('s3/seed', $1, 1, false)
		RETURNING id`, []byte("seed")).Scan(&msgID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	// Three rows: states 0, 1, 3. With the state IN (0,1,2) filter the
	// effective depth is 2 (states 0+1); cap=3 → publish accepted.
	for _, state := range []int{0, 1, 3} {
		if _, err := h.Pool.Exec(ctx, `
			INSERT INTO deliveries(client_id, message_id, qos, state) VALUES ($1, $2, 1, $3)`,
			"s3-sub", msgID, state); err != nil {
			t.Fatalf("seed delivery state=%d: %v", state, err)
		}
	}

	pub := h.Connect(t, "s3-pub")
	defer pub.Close()
	pub.Publish(t, "s3/y", []byte("excluded"), 1, false)

	// Expect a 4th delivery row to land (in-flight states=0,1,3 plus the
	// new row at state=0). If the filter regressed back to counting all
	// states, the publish would have been rejected and depth stays at 3.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := h.Pool.QueryRow(ctx,
			`SELECT count(*) FROM deliveries WHERE client_id='s3-sub'`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 4 {
			// Sub must NOT have been DISCONNECTed.
			if err := sub.Conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
				t.Fatalf("deadline: %v", err)
			}
			pk, err := sub.NextRaw()
			if err == nil && pk.FixedHeader.Type == packets.Disconnect {
				t.Fatalf("sub got unexpected DISCONNECT 0x%X — state=3 row counted toward cap",
					pk.ReasonCode)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected 4 delivery rows after publish (state=3 excluded from cap), never observed")
}
