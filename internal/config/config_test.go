package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestFromEnvDefaults asserts the v5/bcrypt knobs default correctly when no
// env vars are set.
func TestFromEnvDefaults(t *testing.T) {
	t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.V5ReceiveMaximum != 100 {
		t.Errorf("V5ReceiveMaximum default: got %d want 100", c.V5ReceiveMaximum)
	}
	if c.V5KeepaliveMax != 60*time.Second {
		t.Errorf("V5KeepaliveMax default: got %v want 60s", c.V5KeepaliveMax)
	}
	if c.BcryptCost != 10 {
		t.Errorf("BcryptCost default: got %d want 10", c.BcryptCost)
	}
}

// TestFromEnvOverrides verifies env-var overrides land on the Config struct.
func TestFromEnvOverrides(t *testing.T) {
	t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
	t.Setenv("PGMQTT_RECEIVE_MAXIMUM", "256")
	t.Setenv("PGMQTT_KEEPALIVE_MAX_SEC", "120")
	t.Setenv("PGMQTT_BCRYPT_COST", "12")

	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.V5ReceiveMaximum != 256 {
		t.Errorf("V5ReceiveMaximum: got %d want 256", c.V5ReceiveMaximum)
	}
	if c.V5KeepaliveMax != 120*time.Second {
		t.Errorf("V5KeepaliveMax: got %v want 120s", c.V5KeepaliveMax)
	}
	if c.BcryptCost != 12 {
		t.Errorf("BcryptCost: got %d want 12", c.BcryptCost)
	}
}

// TestFromEnvBcryptOutOfRange rejects nonsensical bcrypt costs.
func TestFromEnvBcryptOutOfRange(t *testing.T) {
	t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
	t.Setenv("PGMQTT_BCRYPT_COST", "32")
	if _, err := FromEnv(); err == nil {
		t.Fatal("expected error for bcrypt cost 32")
	}
	t.Setenv("PGMQTT_BCRYPT_COST", "3")
	if _, err := FromEnv(); err == nil {
		t.Fatal("expected error for bcrypt cost 3")
	}
}

func TestFromEnvOperatorDefaults(t *testing.T) {
	t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.ServicePort != 1883 || c.WSPort != 8083 {
		t.Errorf("service ports: got %d/%d want 1883/8083", c.ServicePort, c.WSPort)
	}
	if c.TLSHost != "" || c.TLSPort != 8883 || c.WSSPort != 0 {
		t.Errorf("TLS defaults: got host=%q port=%d wss=%d", c.TLSHost, c.TLSPort, c.WSSPort)
	}
	if c.LeaderElectionID != "pgmqtt-operator" {
		t.Errorf("LeaderElectionID default: got %q want the pre-existing hard-coded value", c.LeaderElectionID)
	}
	if c.WatchNamespaces != nil {
		t.Errorf("WatchNamespaces default: got %v want nil (all namespaces)", c.WatchNamespaces)
	}
}

func TestFromEnvOperatorOverrides(t *testing.T) {
	t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
	t.Setenv("PGMQTT_SERVICE_PORT", "0")
	t.Setenv("PGMQTT_SERVICE_WS_PORT", "0")
	t.Setenv("PGMQTT_TLS_HOST", "mqtt.example.com")
	t.Setenv("PGMQTT_TLS_PORT", "8884")
	t.Setenv("PGMQTT_WSS_PORT", "443")
	t.Setenv("PGMQTT_LEADER_ELECTION_ID", "release-a-pgmqtt")
	t.Setenv("PGMQTT_WATCH_NAMESPACES", "mqtt, apps ,")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.ServicePort != 0 || c.WSPort != 0 {
		t.Errorf("a port of 0 must survive as 0 (listener disabled), got %d/%d", c.ServicePort, c.WSPort)
	}
	if c.TLSHost != "mqtt.example.com" || c.TLSPort != 8884 || c.WSSPort != 443 {
		t.Errorf("TLS: got host=%q port=%d wss=%d", c.TLSHost, c.TLSPort, c.WSSPort)
	}
	if c.LeaderElectionID != "release-a-pgmqtt" {
		t.Errorf("LeaderElectionID: got %q", c.LeaderElectionID)
	}
	if len(c.WatchNamespaces) != 2 || c.WatchNamespaces[0] != "mqtt" || c.WatchNamespaces[1] != "apps" {
		t.Errorf("WatchNamespaces: got %v want [mqtt apps]", c.WatchNamespaces)
	}
}

func TestFromEnvOperatorRejectsInvalid(t *testing.T) {
	for key, val := range map[string]string{
		"PGMQTT_SERVICE_PORT":       "-1",
		"PGMQTT_SERVICE_WS_PORT":    "65536",
		"PGMQTT_TLS_PORT":           "70000",
		"PGMQTT_WSS_PORT":           "-5",
		"PGMQTT_LEADER_ELECTION_ID": "Not_A_Lease_Name",
		"PGMQTT_WATCH_NAMESPACES":   "mqtt,Bad/Namespace",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
			t.Setenv(key, val)
			if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("%s=%q: got %v, want an error naming the variable", key, val, err)
			}
		})
	}
}

// TestFromEnvLogFormat covers the slog-handler selector. Unset / empty must
// default to "text"; "text" and "json" are the only accepted values; any
// other string is rejected with an error so misconfigured aggregators fail
// fast instead of silently emitting the wrong format.
func TestFromEnvLogFormat(t *testing.T) {
	t.Run("default is text when unset", func(t *testing.T) {
		t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
		c, err := FromEnv()
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if c.LogFormat != "text" {
			t.Errorf("LogFormat default: got %q want \"text\"", c.LogFormat)
		}
	})
	t.Run("empty string defaults to text", func(t *testing.T) {
		t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
		t.Setenv("PGMQTT_LOG_FORMAT", "")
		c, err := FromEnv()
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if c.LogFormat != "text" {
			t.Errorf("LogFormat: got %q want \"text\"", c.LogFormat)
		}
	})
	t.Run("text accepted", func(t *testing.T) {
		t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
		t.Setenv("PGMQTT_LOG_FORMAT", "text")
		c, err := FromEnv()
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if c.LogFormat != "text" {
			t.Errorf("LogFormat: got %q want \"text\"", c.LogFormat)
		}
	})
	t.Run("json accepted", func(t *testing.T) {
		t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
		t.Setenv("PGMQTT_LOG_FORMAT", "json")
		c, err := FromEnv()
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if c.LogFormat != "json" {
			t.Errorf("LogFormat: got %q want \"json\"", c.LogFormat)
		}
	})
	t.Run("yaml rejected", func(t *testing.T) {
		t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
		t.Setenv("PGMQTT_LOG_FORMAT", "yaml")
		if _, err := FromEnv(); err == nil {
			t.Fatal("expected error for PGMQTT_LOG_FORMAT=yaml")
		}
	})
}

// TestGetenvIntWarnsOnBadValue asserts a malformed integer env var falls
// back to the default AND emits a Warn log line naming the key + value,
// so the operator can spot the typo instead of silently getting defaults.
func TestGetenvIntWarnsOnBadValue(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	t.Setenv("PGMQTT_DATABASE_URL", "postgres://x")
	t.Setenv("PGMQTT_MAX_CONNECTIONS", "foobar")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.MaxConnections != 5000 {
		t.Errorf("MaxConnections: got %d want default 5000", c.MaxConnections)
	}
	logged := buf.String()
	if !strings.Contains(logged, "PGMQTT_MAX_CONNECTIONS") {
		t.Errorf("expected key in log; got: %q", logged)
	}
	if !strings.Contains(logged, "foobar") {
		t.Errorf("expected offending value in log; got: %q", logged)
	}
}
