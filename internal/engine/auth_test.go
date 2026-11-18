package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

func insertUserHash(t *testing.T, pool *pgxpool.Pool, username, hash string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO users(username, password_hash) VALUES($1, $2)`, username, hash); err != nil {
		t.Fatalf("insert %s: %v", username, err)
	}
}

func mustHash(t *testing.T, password string, cost int) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return string(h)
}

func TestAuthenticate(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()
	insertUserHash(t, pool, "alice", mustHash(t, "s3cret", bcrypt.MinCost))

	if err := Authenticate(ctx, pool, "alice", "s3cret"); err != nil {
		t.Errorf("correct password: %v", err)
	}
	for name, tc := range map[string]struct{ user, pass string }{
		"wrong password":  {"alice", "nope"},
		"empty password":  {"alice", ""},
		"unknown user":    {"bob", "s3cret"},
		"empty username":  {"", "s3cret"},
		"both empty":      {"", ""},
		"unknown + empty": {"bob", ""},
	} {
		if err := Authenticate(ctx, pool, tc.user, tc.pass); !errors.Is(err, ErrAuthFailed) {
			t.Errorf("%s: got %v, want ErrAuthFailed", name, err)
		}
	}
}

// TestAuthenticateRejectsAccountWithoutCredential covers rows that verify
// the empty password, or hold no hash at all: a username alone must never
// be enough to connect.
func TestAuthenticateRejectsAccountWithoutCredential(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()
	insertUserHash(t, pool, "hash-of-empty", mustHash(t, "", bcrypt.MinCost))
	insertUserHash(t, pool, "no-hash", "")

	for _, user := range []string{"hash-of-empty", "no-hash"} {
		for _, pass := range []string{"", "anything"} {
			if err := Authenticate(ctx, pool, user, pass); !errors.Is(err, ErrAuthFailed) {
				t.Errorf("user %q password %q: got %v, want ErrAuthFailed", user, pass, err)
			}
		}
	}
}

func TestSetAuthBcryptCost(t *testing.T) {
	t.Cleanup(func() { SetAuthBcryptCost(bcrypt.DefaultCost) })

	for _, cost := range []int{bcrypt.MinCost, 7, 10} {
		SetAuthBcryptCost(cost)
		h := *dummyHash.Load()
		got, err := bcrypt.Cost(h)
		if err != nil {
			t.Fatalf("cost %d: dummy hash does not parse: %v", cost, err)
		}
		if got != cost {
			t.Errorf("dummy hash cost = %d, want %d", got, cost)
		}
		// Anything other than a mismatch means bcrypt rejected the hash
		// before doing the work, which would make the unknown-user path
		// fast again.
		if err := bcrypt.CompareHashAndPassword(h, []byte("x")); !errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			t.Errorf("cost %d: compare returned %v, want ErrMismatchedHashAndPassword", cost, err)
		}
	}

	for _, bad := range []int{0, bcrypt.MinCost - 1, bcrypt.MaxCost + 1} {
		SetAuthBcryptCost(bad)
		if got, _ := bcrypt.Cost(*dummyHash.Load()); got != bcrypt.DefaultCost {
			t.Errorf("SetAuthBcryptCost(%d): cost = %d, want the default %d", bad, got, bcrypt.DefaultCost)
		}
	}
}

// TestAuthenticateUnknownUserCostsAsMuchAsWrongPassword compares the two
// failure paths at a cost other than bcrypt's default. Each bcrypt cost step
// doubles the work, so a dummy hash left at the default cost 10 would make
// the unknown-user path eight times slower than the wrong-password path at
// cost 7; the factor-of-two tolerance separates that from scheduling noise.
func TestAuthenticateUnknownUserCostsAsMuchAsWrongPassword(t *testing.T) {
	const cost = 7
	pool := dbtest.FreshPool(t)
	ctx := context.Background()
	insertUserHash(t, pool, "known", mustHash(t, "right", cost))

	t.Cleanup(func() { SetAuthBcryptCost(bcrypt.DefaultCost) })
	SetAuthBcryptCost(cost)

	fastest := func(user string) time.Duration {
		best := time.Duration(1<<63 - 1)
		for i := 0; i < 7; i++ {
			start := time.Now()
			if err := Authenticate(ctx, pool, user, "wrong"); !errors.Is(err, ErrAuthFailed) {
				t.Fatalf("authenticate %q: %v", user, err)
			}
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	known, unknown := fastest("known"), fastest("nobody")
	if unknown > 2*known || known > 2*unknown {
		t.Errorf("wrong password took %v, unknown user took %v: the paths are distinguishable", known, unknown)
	}
}
