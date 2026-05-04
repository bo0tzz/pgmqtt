package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrateLockKey is a stable bigint held for the duration of a Migrate call
// so that concurrent Pods don't double-apply (and double-fail with
// unique-constraint races on `CREATE TABLE`).
const migrateLockKey int64 = 0x70676d717474_0001

// Migrate applies any unapplied migrations from the embedded `migrations` directory
// in lexical order. Each migration runs in a single transaction and the version is
// recorded in `schema_migrations`. Safe to call concurrently from multiple Pods —
// callers serialise on a session-scoped Postgres advisory lock.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migrate conn: %w", err)
	}
	defer conn.Release()

	// pg_advisory_lock serialises on a session — held until the connection is
	// released. We use a dedicated conn from the pool so the lock doesn't
	// linger past Migrate returning.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("acquire migrate lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLockKey)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	files, err := readMigrations()
	if err != nil {
		return err
	}

	for _, f := range files {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, f.version).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", f.version, err)
		}
		if exists {
			continue
		}
		if err := applyMigrationOnConn(ctx, conn.Conn(), f); err != nil {
			return err
		}
	}
	return nil
}

type migration struct {
	version string
	body    string
}

func readMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	var out []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := fs.ReadFile(migrationsFS, "migrations/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		out = append(out, migration{
			version: strings.TrimSuffix(e.Name(), ".sql"),
			body:    string(body),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

func applyMigrationOnConn(ctx context.Context, conn *pgx.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin %s: %w", m.version, err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, m.body); err != nil {
		return fmt.Errorf("apply %s: %w", m.version, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, m.version); err != nil {
		return fmt.Errorf("record %s: %w", m.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", m.version, err)
	}
	return nil
}
