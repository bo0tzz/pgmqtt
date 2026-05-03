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

func TestNextPacketID(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `INSERT INTO sessions(client_id, protocol_version, clean_start) VALUES('c', 5, true)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for i := 2; i <= 5; i++ {
		var pid int
		if err := pool.QueryRow(ctx, `SELECT mqtt_next_packet_id('c')`).Scan(&pid); err != nil {
			t.Fatalf("alloc: %v", err)
		}
		if pid != i {
			t.Errorf("expected %d, got %d", i, pid)
		}
	}
}
