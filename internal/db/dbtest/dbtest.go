// Package dbtest provides a Postgres testcontainer harness for integration tests.
//
// Tests must hit a real database (no mocks) because the broker's correctness
// depends on Postgres semantics: advisory locks, LISTEN/NOTIFY, and SQL functions.
package dbtest

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/bo0tzz/pgmqtt/internal/db"
)

var (
	once   sync.Once
	shared *Shared
	sErr   error
)

type Shared struct {
	URL       string
	container testcontainers.Container
}

// SharedContainer starts (lazily) a Postgres container for the test process and
// returns a connection URL. The container is shut down by a test main if needed.
//
// PGMQTT_TEST_DATABASE_URL overrides this to point at a pre-existing DB
// (template DB will be cloned per test). The override is intended for fast
// local iteration; CI uses the testcontainer.
func SharedContainer(ctx context.Context) (*Shared, error) {
	once.Do(func() {
		if url := os.Getenv("PGMQTT_TEST_DATABASE_URL"); url != "" {
			shared = &Shared{URL: url}
			return
		}
		ctr, err := tcpostgres.Run(ctx,
			"postgres:18-alpine",
			tcpostgres.WithDatabase("pgmqtt_template"),
			tcpostgres.WithUsername("pgmqtt"),
			tcpostgres.WithPassword("pgmqtt"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			sErr = err
			return
		}
		url, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sErr = err
			return
		}
		shared = &Shared{URL: url, container: ctr}
	})
	return shared, sErr
}

// sharedOrSkip returns the shared container. Without one, the test skips
// on a developer machine that has no Docker, and fails wherever a green
// run is taken to mean the database-backed tests ran: CI (which sets CI)
// and scripts/validate.sh (which sets PGMQTT_TEST_REQUIRE_DB). Those
// tests are most of the suite, so a skip there is a pass that tested
// almost nothing.
func sharedOrSkip(ctx context.Context, t *testing.T) *Shared {
	t.Helper()
	s, err := SharedContainer(ctx)
	if err != nil {
		if envOn("CI") || envOn("PGMQTT_TEST_REQUIRE_DB") {
			t.Fatalf("postgres testcontainer unavailable (start Docker or set PGMQTT_TEST_DATABASE_URL): %v", err)
		}
		t.Skipf("postgres testcontainer unavailable: %v", err)
	}
	return s
}

func envOn(key string) bool {
	v := os.Getenv(key)
	return v != "" && v != "0" && v != "false"
}

// FreshPool returns a pgxpool.Pool against an isolated database created for this
// test. The database is dropped on test cleanup. Migrations are applied.
func FreshPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	s := sharedOrSkip(ctx, t)

	dbName := freshDBName(t)
	admin, err := pgxpool.New(ctx, s.URL)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+dbName+`"`); err != nil {
		admin.Close()
		t.Fatalf("create db: %v", err)
	}
	admin.Close()

	url := replaceDBName(s.URL, dbName)
	pool, err := db.Open(ctx, url, db.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		admin, err := pgxpool.New(ctx, s.URL)
		if err != nil {
			return
		}
		defer admin.Close()
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+dbName+`" WITH (FORCE)`)
	})
	return pool
}

// FreshURL is like FreshPool but returns the raw connection URL — useful for
// tests that need to open additional pgx.Conn instances (e.g., LISTEN tests).
func FreshURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	s := sharedOrSkip(ctx, t)

	dbName := freshDBName(t)
	admin, err := pgxpool.New(ctx, s.URL)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+dbName+`"`); err != nil {
		admin.Close()
		t.Fatalf("create db: %v", err)
	}
	admin.Close()

	url := replaceDBName(s.URL, dbName)
	pool, err := db.Open(ctx, url, db.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool.Close()

	t.Cleanup(func() {
		admin, err := pgxpool.New(ctx, s.URL)
		if err != nil {
			return
		}
		defer admin.Close()
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+dbName+`" WITH (FORCE)`)
	})
	return url
}
