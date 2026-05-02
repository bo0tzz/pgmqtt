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
			return ErrAuthFailed
		}
		return err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return ErrAuthFailed
	}
	return nil
}
