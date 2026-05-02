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
}

func FromEnv() (*Config, error) {
	c := &Config{
		DatabaseURL: os.Getenv("PGMQTT_DATABASE_URL"),
		TCPAddr:     getenv("PGMQTT_TCP_ADDR", ":1883"),
		WSAddr:      getenv("PGMQTT_WS_ADDR", ":8083"),
		PodName:     os.Getenv("POD_NAME"),
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
