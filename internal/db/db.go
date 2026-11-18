package db

import (
	"context"
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ScrubURLError removes any plaintext password embedded in err's message
// when err originated from a config-parse / connect path that captured the
// connection URL. pgx's ParseConfig wraps the raw input on parse failure,
// so without scrubbing a stderr-bound `logger.Error("db open", "err", err)`
// leaks the Postgres password to anyone with `pods/log` RBAC.
//
// The scrub drops the wrap chain (errors.New returns an unwrappable error).
// This is intentional — the inner error's text is what carries the leak,
// and re-wrapping would re-introduce it.
func ScrubURLError(err error, url string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if u, perr := neturl.Parse(url); perr == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" {
			msg = strings.ReplaceAll(msg, pw, "REDACTED")
		}
	}
	return errors.New(msg)
}

// Options tunes pool-level behaviour callers care about. Zero values mean
// "leave at the pgx / Postgres default".
type Options struct {
	// StatementTimeout becomes the `statement_timeout` runtime param on
	// every connection from this pool. Bounds wedged Postgres so the
	// broker's publisher dispatch can't hang past keepalive.
	StatementTimeout time.Duration
}

// Open creates a pgxpool.Pool against the given URL with sensible defaults.
func Open(ctx context.Context, url string, opts Options) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", ScrubURLError(err, url))
	}
	// statement_timeout cannot help when the server itself is gone without
	// a FIN or RST (a CNPG primary on a lost node): a query then waits on
	// a socket nothing will ever answer. TCP keepalive is what ends it.
	cfg.ConnConfig.DialFunc = KeepaliveDialer
	// PingTimeout bounds the on-Acquire liveness ping. pgxpool's default
	// ShouldPing pings any conn idle > 1s before handing it out; with
	// PingTimeout=0 (upstream default) that ping inherits the caller's
	// context and blocks indefinitely against a half-open socket.
	// Setting a small explicit timeout means a wedged conn is detected
	// and destroyed on the NEXT Acquire, without waiting on keepalive. 2s is
	// generous for an in-cluster PG RTT (<1ms typical) and short enough
	// that callers don't perceive it as a hang.
	cfg.PingTimeout = 2 * time.Second
	if cfg.MaxConns < 8 {
		cfg.MaxConns = 8
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	// Without jitter, every conn opened in a startup burst would expire
	// at the same instant 30 min later — across N pods that's a
	// coordinated reconnect storm against Postgres. 5 min of jitter
	// smears the churn over a 17% window of the lifetime.
	cfg.MaxConnLifetimeJitter = 5 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	if opts.StatementTimeout > 0 {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(opts.StatementTimeout.Milliseconds(), 10)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", ScrubURLError(err, url))
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", ScrubURLError(err, url))
	}
	return pool, nil
}

// TCP keepalive schedule for every connection the broker makes to
// Postgres: a peer that has gone silent is given up on after
// KeepaliveIdle + KeepaliveCount*KeepaliveInterval, about 25 s.
const (
	KeepaliveIdle     = 10 * time.Second
	KeepaliveInterval = 5 * time.Second
	KeepaliveCount    = 3
)

// KeepaliveDialer dials with the keepalive schedule above. The whole
// schedule has to be set: Dialer.KeepAlive alone sets only the idle time
// and leaves the probe interval and count at Go's 15 s x 9, which is
// minutes before a dead peer is noticed.
func KeepaliveDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{
		Timeout: 5 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     KeepaliveIdle,
			Interval: KeepaliveInterval,
			Count:    KeepaliveCount,
		},
	}
	return d.DialContext(ctx, network, addr)
}
