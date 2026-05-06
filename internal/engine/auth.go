package engine

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// ErrAuthFailed is returned for both unknown user and bad password — the
// indistinguishable response is intentional.
var ErrAuthFailed = errors.New("authentication failed")

// dummyHash is a precomputed bcrypt hash of a constant string at our
// default cost. It's compared against (every time the SELECT returns no
// rows) purely so the no-rows branch and the bad-password branch take
// indistinguishable wall-clock time. The constant was generated once at
// build time to avoid a one-off ~75 ms startup penalty per process.
//
// CWE-208 mitigation: without this guard, "user doesn't exist" and
// "wrong password" would have measurably different wall-clock times
// (one bcrypt vs zero), revealing username existence.
var dummyHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

// Authenticate verifies username/password against the users table.
// Empty username allows anonymous access only if the env opts in (not yet wired);
// for now an empty username is rejected.
func Authenticate(ctx context.Context, pool *pgxpool.Pool, username, password string) error {
	if username == "" {
		return ErrAuthFailed
	}
	var hash string
	err := pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE username=$1`, username).Scan(&hash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Run a full bcrypt comparison against a dummy hash so the
			// no-rows path takes the same wall-clock time as the
			// wrong-password path. Discard the result.
			_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
			return ErrAuthFailed
		}
		return err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return ErrAuthFailed
	}
	return nil
}
