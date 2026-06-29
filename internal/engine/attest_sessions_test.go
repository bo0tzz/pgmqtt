package engine_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
)

// TestAttestOwnedSessionsRestoresReapedRows pins the 2026-06-29 fix.
// Setup: a client connects, gets a sessions row with broker_id=ME and
// connected=true. We simulate a spurious peer-broker reap (what
// `handleDeadBroker` would do during a CNPG-failover storm where every
// broker briefly loses its advisory lock) by manually setting the row
// to broker_id=NULL, connected=false. AttestOwnedSessions must restore
// it: broker_id back to ME, connected back to true, last_seen bumped.
// session_token is unchanged (the broker still owns this client; the
// reap was wrong about reality).
func TestAttestOwnedSessionsRestoresReapedRows(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	const clientID = "attest-restore"
	c := h.Connect(t, clientID, func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	})
	defer c.Close()

	// Capture the truth pre-reap.
	var myBroker uuid.UUID
	var token uuid.UUID
	if err := h.Pool.QueryRow(ctx,
		`SELECT broker_id, session_token FROM sessions WHERE client_id=$1`,
		clientID).Scan(&myBroker, &token); err != nil {
		t.Fatalf("read pre-reap: %v", err)
	}
	if myBroker == uuid.Nil {
		t.Fatalf("expected broker_id set, got nil")
	}

	// Simulate a peer-broker reap: handleDeadBroker's terminal UPDATE
	// (post-will-fire), without touching will_* columns to keep the
	// test isolated to the routing-bookkeeping path.
	if _, err := h.Pool.Exec(ctx, `
		UPDATE sessions SET broker_id=NULL, connected=false, last_seen=now()
		 WHERE client_id=$1
	`, clientID); err != nil {
		t.Fatalf("simulate reap: %v", err)
	}

	// Verify the reap actually happened (test self-check).
	var reapedBroker *uuid.UUID
	var reapedConnected bool
	if err := h.Pool.QueryRow(ctx,
		`SELECT broker_id, connected FROM sessions WHERE client_id=$1`,
		clientID).Scan(&reapedBroker, &reapedConnected); err != nil {
		t.Fatalf("read post-reap: %v", err)
	}
	if reapedBroker != nil || reapedConnected {
		t.Fatalf("self-check: expected reaped state, got broker=%v connected=%v",
			reapedBroker, reapedConnected)
	}

	// The fix: AttestOwnedSessions must rewrite the truth.
	h.Engine.AttestOwnedSessions(ctx)

	var afterBroker uuid.UUID
	var afterConnected bool
	var afterToken uuid.UUID
	if err := h.Pool.QueryRow(ctx,
		`SELECT broker_id, connected, session_token FROM sessions WHERE client_id=$1`,
		clientID).Scan(&afterBroker, &afterConnected, &afterToken); err != nil {
		t.Fatalf("read post-attest: %v", err)
	}
	if afterBroker != myBroker {
		t.Fatalf("broker_id not restored: want %s, got %s", myBroker, afterBroker)
	}
	if !afterConnected {
		t.Fatalf("connected not restored: want true, got false")
	}
	if afterToken != token {
		t.Fatalf("session_token mutated: want %s, got %s", token, afterToken)
	}
}

// TestAttestOwnedSessionsRespectsTokenRotation guards the takeover race.
// If a peer broker has legitimately taken over the client (rotated
// session_token via takeOwnership), the local broker MUST NOT overwrite
// the new owner's broker_id with its own — that would re-introduce the
// session_token-divergence class of bug that migration 0012 was added
// to close. The session_token captured in the local Conn at our own
// CONNECT time must match the row's current token for the attest UPDATE
// to apply; otherwise it's a no-op.
func TestAttestOwnedSessionsRespectsTokenRotation(t *testing.T) {
	t.Parallel()
	h := enginetest.NewHarness(t)
	ctx := context.Background()

	const clientID = "attest-token-guard"
	c := h.Connect(t, clientID, func(p *packets.Packet) {
		p.Connect.Clean = false
		p.Properties.SessionExpiryInterval = 3600
		p.Properties.SessionExpiryIntervalFlag = true
	})
	defer c.Close()

	// Simulate a peer broker taking over: install their broker_id and
	// rotate session_token to a fresh value. The local Conn's captured
	// token is now stale.
	peerBroker := uuid.New()
	newToken := uuid.New()
	if _, err := h.Pool.Exec(ctx, `
		UPDATE sessions SET broker_id=$2, connected=true, session_token=$3, last_seen=now()
		 WHERE client_id=$1
	`, clientID, peerBroker, newToken); err != nil {
		t.Fatalf("simulate peer takeover: %v", err)
	}

	// Attest with our (now-stale) token. Must NOT stomp the peer's
	// ownership.
	h.Engine.AttestOwnedSessions(ctx)

	var afterBroker uuid.UUID
	var afterToken uuid.UUID
	if err := h.Pool.QueryRow(ctx,
		`SELECT broker_id, session_token FROM sessions WHERE client_id=$1`,
		clientID).Scan(&afterBroker, &afterToken); err != nil {
		t.Fatalf("read post-attest: %v", err)
	}
	if afterBroker != peerBroker {
		t.Fatalf("attest stomped peer takeover: want broker=%s, got %s",
			peerBroker, afterBroker)
	}
	if afterToken != newToken {
		t.Fatalf("attest mutated peer's token: want %s, got %s", newToken, afterToken)
	}
}
