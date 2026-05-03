package config

import (
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
