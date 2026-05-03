package config

import (
	"errors"
	"fmt"
	"os"
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
	}
	if c.DatabaseURL == "" {
		return nil, errors.New("PGMQTT_DATABASE_URL is required")
	}
	if c.TCPAddr == "" && c.WSAddr == "" {
		return nil, fmt.Errorf("at least one of PGMQTT_TCP_ADDR or PGMQTT_WS_ADDR must be set")
	}
	return c, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
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
