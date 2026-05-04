package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
)

// TestSessionTokenRotatesOnTakeover asserts that every CONNECT (whether
// it inserts a fresh sessions row or updates an existing one) rotates
// session_token. This is the property handleDisconnect relies on to
// distinguish "my own session row" from "a peer that took over before I
// got to clean up".
func TestSessionTokenRotatesOnTakeover(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	// First connect creates the row with token T1.
	persistent := func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	}
	c1 := h.Connect(t, "sess-token-rotate", persistent)
	var t1 uuid.UUID
	if err := h.Pool.QueryRow(ctx,
		`SELECT session_token FROM sessions WHERE client_id=$1`,
		"sess-token-rotate").Scan(&t1); err != nil {
		t.Fatalf("read t1: %v", err)
	}
	c1.Close()

	// Second connect (same client_id) UPDATEs the row. Should rotate
	// session_token to a different value.
	c2 := h.Connect(t, "sess-token-rotate", persistent)
	defer c2.Close()
	var t2 uuid.UUID
	if err := h.Pool.QueryRow(ctx,
		`SELECT session_token FROM sessions WHERE client_id=$1`,
		"sess-token-rotate").Scan(&t2); err != nil {
		t.Fatalf("read t2: %v", err)
	}
	if t1 == t2 {
		t.Fatalf("session_token did not rotate on UPDATE: t1=t2=%s", t1)
	}
}

// TestStaleHandleDisconnectDoesNotWipeTakeover simulates the race that
// surfaced in Paho test_session_expiry. Conn A sets up a session,
// captures its token T1. The "client" reconnects (Conn B), takeOwnership
// rotates token to T2 — this is what would happen if the same client_id
// reconnects before A's handleDisconnect's session-DELETE tx fires.
// Then A's handleDisconnect runs (using its captured T1) and *must not*
// wipe Conn B's session.
//
// Test drives the SQL guard directly rather than racing two real
// connections (which would be timing-dependent). The end-to-end Paho
// test_session_expiry exercises the same property under the actual
// race conditions.
func TestStaleHandleDisconnectDoesNotWipeTakeover(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	const clientID = "sess-stale-disconnect"

	// Conn A: insert a session with a fresh token (the value Conn A
	// would have stored on c.sessionToken at takeOwnership time).
	t1 := uuid.New()
	if _, err := h.Pool.Exec(ctx, `
		INSERT INTO sessions
		    (client_id, broker_id, connected, protocol_version, clean_start,
		     session_token, last_seen)
		VALUES ($1, NULL, false, 5, false, $2, now())
	`, clientID, t1); err != nil {
		t.Fatalf("insert A: %v", err)
	}

	// Conn B "takes over": rotate token to T2 (UPDATE). This stands in
	// for what the second client.connect() would do via takeOwnership.
	t2 := uuid.New()
	if _, err := h.Pool.Exec(ctx, `
		UPDATE sessions SET session_token=$2 WHERE client_id=$1
	`, clientID, t2); err != nil {
		t.Fatalf("update B: %v", err)
	}

	// Conn A's stale handleDisconnect now runs with its captured t1.
	// The token-scoped DELETE is the production query — copy it here.
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := h.Pool.BeginTx(deadline, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(deadline)
	ct, err := tx.Exec(deadline,
		`DELETE FROM sessions WHERE client_id=$1 AND session_token=$2`,
		clientID, t1)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if ct.RowsAffected() != 0 {
		t.Fatalf("stale delete affected %d rows; expected 0 (takeover should preserve)", ct.RowsAffected())
	}
	// Production code would early-return here (no commit); the test
	// just rolls back via defer.

	// Verify Conn B's row is still present with t2.
	var observed uuid.UUID
	if err := h.Pool.QueryRow(ctx,
		`SELECT session_token FROM sessions WHERE client_id=$1`,
		clientID).Scan(&observed); err != nil {
		t.Fatalf("post-stale-delete read: %v", err)
	}
	if observed != t2 {
		t.Fatalf("session_token: got %s, want t2=%s", observed, t2)
	}
}
