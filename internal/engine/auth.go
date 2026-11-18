package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// ErrAuthFailed is returned for both unknown user and bad password — the
// indistinguishable response is intentional.
var ErrAuthFailed = errors.New("authentication failed")

// dummyHashTail is the salt and digest part of a bcrypt hash, without the
// "$2a$<cost>$" prefix. CompareHashAndPassword takes the cost from the
// prefix and runs the whole key schedule before it compares digests, so
// putting any cost in front of this tail yields a comparison that takes as
// long as a real one at that cost. That avoids generating a hash at startup,
// which at a high configured cost takes seconds.
const dummyHashTail = "N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

// dummyHash is compared against whenever the SELECT returns no rows, so
// that "user doesn't exist" and "wrong password" take the same wall-clock
// time and usernames can't be enumerated by timing (CWE-208). That only
// holds while its cost equals the cost of the stored hashes, which the
// operator writes at PGMQTT_BCRYPT_COST: see SetAuthBcryptCost.
var dummyHash atomic.Pointer[[]byte]

func init() {
	SetAuthBcryptCost(bcrypt.DefaultCost)
}

// SetAuthBcryptCost sets the cost of the comparison Authenticate runs for
// an unknown username. Call it with the cost the operator hashes passwords
// at, before serving connections. A cost bcrypt would reject falls back to
// bcrypt.DefaultCost.
func SetAuthBcryptCost(cost int) {
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		cost = bcrypt.DefaultCost
	}
	h := []byte(fmt.Sprintf("$2a$%02d$%s", cost, dummyHashTail))
	dummyHash.Store(&h)
}

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
			_ = bcrypt.CompareHashAndPassword(*dummyHash.Load(), []byte(password))
			return ErrAuthFailed
		}
		return err
	}
	matched := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	// A row whose hash verifies the empty password is an account without a
	// credential. The operator refuses to create one; this refuses to honour
	// one that got into the table some other way. Checked after the
	// comparison so the reply takes as long as any other failure.
	if !matched || password == "" {
		return ErrAuthFailed
	}
	return nil
}
