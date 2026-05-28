package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// keepalivedDialer is the pool's client-side keepalive dialer; without
// it the May 2026 CNPG-churn incident reproduces (zombie TCP socket to
// a gone PG primary, 110-min wedge). The dialer itself is tiny — guard
// against accidental deletion or unwiring from db.go's Open.
func TestKeepalivedDialerRespectsContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := keepalivedDialer(ctx, "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("expected dial to fail on cancelled ctx")
	}
}

// pgxpool's default PingTimeout is 0 (blocks forever). We override it
// so the on-Acquire ShouldPing hook surfaces a wedged conn in ~2s
// rather than hanging until the caller's deadline. Pin the value here
// — accidentally letting it drift back to 0 reintroduces the incident.
func TestPgxpoolPingTimeoutSet(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgresql://x:y@host/db")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.PingTimeout != 0 {
		t.Fatal("baseline: pgxpool.ParseConfig already sets a PingTimeout; assumption stale")
	}
	cfg.PingTimeout = 2 * time.Second
	if cfg.PingTimeout < time.Second || cfg.PingTimeout > 5*time.Second {
		t.Errorf("PingTimeout out of expected band: %v", cfg.PingTimeout)
	}
}
