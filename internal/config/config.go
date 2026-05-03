package config

import (
	"errors"
	"fmt"
	"os"
	"time"
)

type Config struct {
	DatabaseURL string
	TCPAddr     string
	WSAddr      string
	PodName     string

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
	//   TopicAliasMaximum  — advertised inbound-alias capacity. 0 (default)
	//                        means we do not accept client-side aliases.
	//   KeepaliveMax       — server cap on the negotiated keepalive interval.
	V5ReceiveMaximum    uint16
	V5TopicAliasMaximum uint16
	V5KeepaliveMax      time.Duration

	// Bcrypt cost used by the User-CR reconciler when hashing passwords for
	// the broker's `users` table. 10 is bcrypt's library default.
	BcryptCost int
}

func FromEnv() (*Config, error) {
	c := &Config{
		DatabaseURL: os.Getenv("PGMQTT_DATABASE_URL"),
		TCPAddr:     getenv("PGMQTT_TCP_ADDR", ":1883"),
		WSAddr:      getenv("PGMQTT_WS_ADDR", ":8083"),
		PodName:     os.Getenv("POD_NAME"),
		ServiceHost:    os.Getenv("PGMQTT_SERVICE_HOST"),
		ServicePort:    getenvInt("PGMQTT_SERVICE_PORT", 1883),
		WSPort:         getenvInt("PGMQTT_SERVICE_WS_PORT", 8083),
		AllowAnonymous: os.Getenv("PGMQTT_ALLOW_ANONYMOUS") == "true",

		V5ReceiveMaximum:    uint16(getenvInt("PGMQTT_RECEIVE_MAXIMUM", 100)),
		V5TopicAliasMaximum: uint16(getenvInt("PGMQTT_TOPIC_ALIAS_MAXIMUM", 0)),
		V5KeepaliveMax:      time.Duration(getenvInt("PGMQTT_KEEPALIVE_MAX_SEC", 60)) * time.Second,
		BcryptCost:          getenvInt("PGMQTT_BCRYPT_COST", 10),
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

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		if err == nil {
			return n
		}
	}
	return def
}
