package db_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bo0tzz/pgmqtt/internal/db"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

func TestMigrateIsIdempotent(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()

	// FreshPool already migrated; running again should be a no-op.
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n == 0 {
		t.Fatal("no migrations recorded")
	}

	// Sanity-check: tables and the helper function exist.
	for _, table := range []string{"users", "sessions", "subscriptions", "retained", "messages", "deliveries"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&exists); err != nil {
			t.Fatalf("check %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s missing after migrate", table)
		}
	}

	for _, m := range []struct {
		filter, topic string
		want          bool
	}{
		{"a/b", "a/b", true},
		{"a/+", "a/b", true},
		{"a/+/c", "a/b/c", true},
		{"a/#", "a/b/c/d", true},
		{"a/b", "a/b/c", false},
		{"a/+", "a", false},
		{"#", "a/b", true},
		{"+/+", "a/b", true},
		{"+/+", "a", false},
		{"#", "$SYS/foo", false},
		{"+/foo", "$SYS/foo", false},
		{"$SYS/#", "$SYS/foo/bar", true},
	} {
		var got bool
		if err := pool.QueryRow(ctx, `SELECT mqtt_topic_match($1, $2)`, m.filter, m.topic).Scan(&got); err != nil {
			t.Fatalf("match %q vs %q: %v", m.filter, m.topic, err)
		}
		if got != m.want {
			t.Errorf("mqtt_topic_match(%q, %q) = %v, want %v", m.filter, m.topic, got, m.want)
		}
	}
}

func TestStatementTimeoutFires(t *testing.T) {
	t.Parallel()
	url := dbtest.FreshURL(t)
	ctx := context.Background()
	pool, err := db.Open(ctx, url, db.Options{StatementTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	// pg_sleep(1) should be cancelled by the 100ms statement_timeout.
	// The exact error message comes from PG ("canceling statement due to
	// statement timeout") via SQLSTATE 57014.
	start := time.Now()
	_, err = pool.Exec(ctx, `SELECT pg_sleep(1)`)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected statement_timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "57014") && !strings.Contains(err.Error(), "statement timeout") {
		t.Fatalf("expected statement_timeout error, got %T: %v", err, err)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("statement ran for %v, expected ~100ms", elapsed)
	}
}

func TestStatementTimeoutZeroDoesNotSet(t *testing.T) {
	t.Parallel()
	url := dbtest.FreshURL(t)
	ctx := context.Background()
	pool, err := db.Open(ctx, url, db.Options{StatementTimeout: 0})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	// With timeout=0, statement_timeout falls back to PG's default (0 = no
	// limit) — a quick query that would otherwise be killed by a tiny
	// timeout should complete cleanly.
	if _, err := pool.Exec(ctx, `SELECT pg_sleep(0.05)`); err != nil {
		t.Fatalf("sleep with no timeout: %v", err)
	}
}

func TestMigrateConcurrent(t *testing.T) {
	t.Parallel()
	// Two pods racing to migrate the same fresh database — neither should fail.
	url := dbtest.FreshURL(t)
	ctx := context.Background()
	const n = 4
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			pool, err := db.Open(ctx, url, db.Options{})
			if err != nil {
				errs <- err
				return
			}
			defer pool.Close()
			errs <- db.Migrate(ctx, pool)
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent migrate: %v", err)
		}
	}
}

// TestSessionsHasNoNextPacketIDColumn asserts the next_packet_id column was
// dropped by migration 0009. Outbound packet-id allocation lives in
// internal/engine/conn.go::AllocPacketID — see TestAllocPacketID* in the
// engine package for behavioural tests.
func TestSessionsHasNoNextPacketIDColumn(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()

	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
		  SELECT 1 FROM information_schema.columns
		   WHERE table_name='sessions' AND column_name='next_packet_id'
		)`).Scan(&exists); err != nil {
		t.Fatalf("information_schema: %v", err)
	}
	if exists {
		t.Error("sessions.next_packet_id should have been dropped by migration 0009")
	}

	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
		  SELECT 1 FROM pg_proc WHERE proname='mqtt_next_packet_id'
		)`).Scan(&exists); err != nil {
		t.Fatalf("pg_proc: %v", err)
	}
	if exists {
		t.Error("mqtt_next_packet_id() should have been dropped by migration 0009")
	}
}

// TestUsersOwnerColumnsKeepOldStatementsWorking runs the statements a broker
// from before migration 0019 issues against the migrated table, as happens
// while old and new Pods overlap during a rolling upgrade.
func TestUsersOwnerColumnsKeepOldStatementsWorking(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()

	const oldUpsert = `
		INSERT INTO users(username, password_hash) VALUES($1, $2)
		ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash`
	if _, err := pool.Exec(ctx, oldUpsert, "legacy", "h1"); err != nil {
		t.Fatalf("old insert: %v", err)
	}

	var ns, name, uid *string
	if err := pool.QueryRow(ctx,
		`SELECT owner_namespace, owner_name, owner_uid FROM users WHERE username='legacy'`,
	).Scan(&ns, &name, &uid); err != nil {
		t.Fatalf("read owner columns: %v", err)
	}
	if ns != nil || name != nil || uid != nil {
		t.Errorf("row written by an old broker should have no owner, got %v/%v/%v", ns, name, uid)
	}

	if _, err := pool.Exec(ctx,
		`UPDATE users SET owner_namespace='mqtt', owner_name='legacy', owner_uid='u-1' WHERE username='legacy'`); err != nil {
		t.Fatalf("set owner: %v", err)
	}
	if _, err := pool.Exec(ctx, oldUpsert, "legacy", "h2"); err != nil {
		t.Fatalf("old upsert on an owned row: %v", err)
	}
	var hash string
	if err := pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE username=$1`, "legacy").Scan(&hash); err != nil {
		t.Fatalf("old select: %v", err)
	}
	if hash != "h2" {
		t.Errorf("password_hash = %q, want h2", hash)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM users WHERE username=$1`, "legacy"); err != nil {
		t.Fatalf("old delete: %v", err)
	}
}

// TestMigrateIgnoresPoolStatementTimeout holds the migration advisory lock
// from a second session for longer than the pool's statement_timeout, the
// way a peer pod running a slow migration would.
func TestMigrateIgnoresPoolStatementTimeout(t *testing.T) {
	t.Parallel()
	url := dbtest.FreshURL(t)
	ctx := context.Background()

	holder, err := db.Open(ctx, url, db.Options{})
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	hc, err := holder.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	defer hc.Release()
	if _, err := hc.Exec(ctx, `SELECT pg_advisory_lock($1)`, db.MigrateLockKeyForTest); err != nil {
		t.Fatalf("take lock: %v", err)
	}

	pool, err := db.Open(ctx, url, db.Options{StatementTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	done := make(chan error, 1)
	go func() { done <- db.Migrate(ctx, pool) }()

	select {
	case err := <-done:
		t.Fatalf("Migrate returned while the lock was held elsewhere: %v", err)
	case <-time.After(time.Second):
	}
	if _, err := hc.Exec(ctx, `SELECT pg_advisory_unlock($1)`, db.MigrateLockKeyForTest); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("migrate: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Migrate did not finish after the lock was released")
	}

	// The pool's own connections must still carry the timeout.
	_, err = pool.Exec(ctx, `SELECT pg_sleep(2)`)
	if err == nil || !strings.Contains(err.Error(), "57014") {
		t.Fatalf("pool statement_timeout no longer applies after Migrate: %v", err)
	}
}
