package db_test

import (
	"context"
	"testing"

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

func TestMigrateConcurrent(t *testing.T) {
	t.Parallel()
	// Two pods racing to migrate the same fresh database — neither should fail.
	url := dbtest.FreshURL(t)
	ctx := context.Background()
	const n = 4
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			pool, err := db.Open(ctx, url)
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
