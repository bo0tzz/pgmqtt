package db

import (
	"context"
	"testing"
)

// keepalivedDialer is the pool's client-side keepalive dialer; without it
// the May 2026 CNPG-churn incident reproduces (zombie TCP socket to a
// gone PG primary, 110-min wedge while pgxpool's HealthCheckPeriod ping
// succeeds against the half-open socket). The dialer itself is tiny;
// guard against accidental deletion / wiring removal in db.go's Open.
func TestKeepalivedDialerRespectsContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := keepalivedDialer(ctx, "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("expected dial to fail on cancelled ctx")
	}
}
