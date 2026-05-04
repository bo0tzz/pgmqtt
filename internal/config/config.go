package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL  string
	TCPAddr      string
	WSAddr       string
	PodName      string
	PodNamespace string // K8s namespace this Pod runs in (Downward API).
	// Used as the LeaderElectionNamespace for the operator's
	// controller-runtime Lease. Empty falls back to the in-cluster
	// auto-detect path; explicit-via-env is needed when running
	// outside a cluster against a dev kubeconfig.

	// AllowAnonymous accepts CONNECT without username/password. Off by default.
	AllowAnonymous bool

	// Operator wire-details for auto-generated User Secrets.
	ServiceHost string
	ServicePort int
	WSPort      int

	// V5 server policy. All advertised to clients in CONNACK; the v5 codec
	// + per-conn handlers enforce the values during the session.
	//   ReceiveMaximum     — cap on un-ACKed inbound QoS>0 PUBLISHes per conn.
	//                        0 disables advertising (server picks).
	//   KeepaliveMax       — server cap on the negotiated keepalive interval.
	//
	// We intentionally do NOT expose a knob for `TopicAliasMaximum` because
	// the broker does not maintain an inbound topic-alias map — the value
	// advertised to clients is hardcoded to 0, which per [MQTT-3.3.2-12]
	// tells clients to never send a TopicAlias > 0. Outbound aliases
	// (server → client) are negotiated via the *client's*
	// TopicAliasMaximum in CONNECT and supported.
	V5ReceiveMaximum uint16
	V5KeepaliveMax   time.Duration

	// Bcrypt cost used by the User-CR reconciler when hashing passwords for
	// the broker's `users` table. 10 is bcrypt's library default.
	BcryptCost int

	// MaxQueuedDeliveriesPerClient bounds the per-conn outbound queue depth
	// in the deliveries table. When a slow subscriber is at the cap:
	//   * QoS-0 messages targeted at them are silently dropped (spec-OK).
	//   * QoS>0 messages targeted at them are dropped AND the subscriber is
	//     DISCONNECTed with reason 0x97 (Quota Exceeded).
	// 0 disables the cap entirely.
	MaxQueuedDeliveriesPerClient int

	// MaxConnections caps concurrent client connections this Pod will accept.
	// Above the cap, CONNACK is rejected with reason 0x9F (Connection Rate
	// Exceeded) and the socket is closed. 0 disables the cap.
	MaxConnections int

	// MaxInboundMsgsPerSec is the per-connection token-bucket rate for
	// inbound PUBLISH/SUBSCRIBE. Sustained over-rate triggers a DISCONNECT
	// 0x96 (Message Rate Too High). 0 disables the limit.
	MaxInboundMsgsPerSec int

	// MaxConnectsPerIPPerSec caps the per-source-IP CONNECT rate (and
	// burst). Over-rate connections are dropped pre-CONNACK with the
	// socket hard-closed (no CONNACK — sending one fans attackers' retries).
	// 0 disables the limit.
	//
	// MaxAuthFailuresPerIPPerMin caps the per-IP rate of bcrypt failures
	// before the IP enters a 60s "penalty box" — every CONNECT from
	// that IP is dropped pre-bcrypt for the cool-off window. Together
	// these mitigate the bcrypt-CPU DoS surface where an unauthenticated
	// attacker can pin cores by streaming bad-credential CONNECTs.
	// 0 disables the limit.
	MaxConnectsPerIPPerSec     int
	MaxAuthFailuresPerIPPerMin int

	// MaxPacketSize is the post-CONNECT cap on the inbound packet size
	// (bytes). The codec applies a hardcoded 1 MiB cap before CONNECT to
	// bound DoS allocations from unauthenticated peers; once CONNECT lands
	// the cap is raised to min(client_max_packet_size_v5, MaxPacketSize).
	// 0 means "no override" — the codec falls back to the absolute upper
	// bound (256 MiB) which is rarely what operators want. Default 16 MiB.
	MaxPacketSize int

	// MetricsAddr is the bind address for the Prometheus /metrics endpoint.
	// Empty disables metrics serving entirely.
	MetricsAddr string

	// PGStatementTimeout bounds individual SQL statements on connections
	// from the broker's pgxpool. Wedged Postgres (network blip, replica
	// catch-up, lock storm) would otherwise hang publisher dispatch
	// indefinitely — keepalive only re-arms between dispatch iterations,
	// so a stuck COMMIT bypasses it. 0 disables the timeout (matches PG
	// default). The dedicated listener connection does not currently
	// take this setting; LISTEN's WaitForNotification has no statement
	// to time out anyway.
	PGStatementTimeout time.Duration

	// LogFormat selects the slog handler emitted on stderr. "text" (default)
	// is human-readable; "json" emits one JSON object per line which log
	// aggregation systems (Loki, Elasticsearch, Datadog, …) parse without
	// a dedicated text-format extractor. Anything else is rejected.
	LogFormat string

	// WSAllowedOrigins restricts the Origin header on websocket upgrades.
	// Empty (default) allows any origin — matches the historical behavior
	// and is fine for the typical "broker behind an L4/L7 terminator" shape
	// where browsers can't reach the broker directly. When operators want
	// to mitigate cross-site WebSocket hijacking on a publicly-reachable
	// /mqtt endpoint, set a comma-separated list of allowed Origin values
	// (exact match, no globbing).
	WSAllowedOrigins []string

	// JanitorInterval is the base tick frequency (the GCD of stratified
	// per-job intervals). 1s default. fire_due_wills runs at this cadence
	// (Paho v5 test_will_delay asserts will-fire within 1s of WillDelay-
	// Interval); cleanup jobs run on stratified 5s/10s/30s cadences
	// internally, so idle DB churn is ~5× lower than naive "every-job-
	// every-tick". 0 leaves the janitor's own default (1s).
	JanitorInterval time.Duration
}

func FromEnv() (*Config, error) {
	c := &Config{
		DatabaseURL: os.Getenv("PGMQTT_DATABASE_URL"),
		TCPAddr:     getenv("PGMQTT_TCP_ADDR", ":1883"),
		WSAddr:      getenv("PGMQTT_WS_ADDR", ":8083"),
		PodName:      os.Getenv("POD_NAME"),
		PodNamespace: os.Getenv("POD_NAMESPACE"),
		ServiceHost:    os.Getenv("PGMQTT_SERVICE_HOST"),
		ServicePort:    getenvInt("PGMQTT_SERVICE_PORT", 1883),
		WSPort:         getenvInt("PGMQTT_SERVICE_WS_PORT", 8083),
		AllowAnonymous: os.Getenv("PGMQTT_ALLOW_ANONYMOUS") == "true",

		V5ReceiveMaximum:             uint16(getenvInt("PGMQTT_RECEIVE_MAXIMUM", 100)),
		V5KeepaliveMax:               time.Duration(getenvInt("PGMQTT_KEEPALIVE_MAX_SEC", 60)) * time.Second,
		BcryptCost:                   getenvInt("PGMQTT_BCRYPT_COST", 10),
		MaxQueuedDeliveriesPerClient: getenvInt("PGMQTT_MAX_QUEUED_DELIVERIES_PER_CLIENT", 10000),
		MaxConnections:               getenvInt("PGMQTT_MAX_CONNECTIONS", 5000),
		MaxInboundMsgsPerSec:         getenvInt("PGMQTT_MAX_INBOUND_MSGS_PER_SEC", 1000),
		MaxConnectsPerIPPerSec:     getenvInt("PGMQTT_MAX_CONNECTS_PER_IP_PER_SEC", 5),
		MaxAuthFailuresPerIPPerMin: getenvInt("PGMQTT_MAX_AUTH_FAILURES_PER_IP_PER_MIN", 30),
		MaxPacketSize:                getenvInt("PGMQTT_MAX_PACKET_SIZE", 16*1024*1024),
		MetricsAddr:                  getenv("PGMQTT_METRICS_ADDR", ":9090"),
		PGStatementTimeout:           time.Duration(getenvInt("PGMQTT_PG_STATEMENT_TIMEOUT_MS", 30000)) * time.Millisecond,
		LogFormat:                    getenvDefaultEmpty("PGMQTT_LOG_FORMAT", "text"),
		WSAllowedOrigins:             splitTrimmed(os.Getenv("PGMQTT_WS_ALLOWED_ORIGINS"), ","),
		JanitorInterval:              time.Duration(getenvInt("PGMQTT_JANITOR_INTERVAL_MS", 1000)) * time.Millisecond,
	}
	if c.DatabaseURL == "" {
		return nil, errors.New("PGMQTT_DATABASE_URL is required")
	}
	if c.TCPAddr == "" && c.WSAddr == "" {
		return nil, fmt.Errorf("at least one of PGMQTT_TCP_ADDR or PGMQTT_WS_ADDR must be set")
	}
	if c.V5KeepaliveMax <= 0 {
		return nil, fmt.Errorf("PGMQTT_KEEPALIVE_MAX_SEC must be > 0")
	}
	if c.BcryptCost < 4 || c.BcryptCost > 31 {
		return nil, fmt.Errorf("PGMQTT_BCRYPT_COST must be between 4 and 31")
	}
	if c.PGStatementTimeout < 0 {
		return nil, fmt.Errorf("PGMQTT_PG_STATEMENT_TIMEOUT_MS must be >= 0")
	}
	if c.MaxPacketSize < 0 || c.MaxPacketSize > 268435455 {
		return nil, fmt.Errorf("PGMQTT_MAX_PACKET_SIZE must be in [0, 268435455]")
	}
	if c.MaxConnectsPerIPPerSec < 0 {
		return nil, fmt.Errorf("PGMQTT_MAX_CONNECTS_PER_IP_PER_SEC must be >= 0")
	}
	if c.MaxAuthFailuresPerIPPerMin < 0 {
		return nil, fmt.Errorf("PGMQTT_MAX_AUTH_FAILURES_PER_IP_PER_MIN must be >= 0")
	}
	switch c.LogFormat {
	case "text", "json":
	default:
		return nil, fmt.Errorf("PGMQTT_LOG_FORMAT must be \"text\" or \"json\", got %q", c.LogFormat)
	}
	return c, nil
}

// getenv returns the env value if set (including empty string) — only
// substitutes the default when the variable is unset. Empty string means "I
// explicitly want this listener disabled".
func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// getenvDefaultEmpty is like getenv but also substitutes the default when
// the variable is set to the empty string. Use this for knobs where empty
// is meaningless (e.g. log format) and the only sane interpretation is
// "fall back to default", as opposed to listener addresses where empty has
// the explicit meaning "disable this listener".
func getenvDefaultEmpty(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitTrimmed splits s on sep and returns non-empty trimmed parts.
// Returns nil for empty input so callers see a clean "unconfigured" sentinel.
func splitTrimmed(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		// strconv.Atoi is strict: "1.6777216e+07" → error, "16777216" → ok.
		// fmt.Sscanf("%d") was previously used here but it accepts a leading
		// integer prefix and silently discards the rest, which let
		// helm-rendered scientific-notation values like "1.6777216e+07" be
		// silently truncated to 1. This bit production: a broker pod ran
		// with MaxPacketSize=1 (one byte) and rejected every PUBLISH as
		// "packet too large".
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
		slog.Warn("config: failed to parse env var as int — using default",
			"key", key, "value", v, "default", def, "err", err)
	}
	return def
}
