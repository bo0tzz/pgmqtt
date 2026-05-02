package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bo0tzz/pgmqtt/internal/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("pgmqttd booting", "tcp", cfg.TCPAddr, "ws", cfg.WSAddr)
	<-ctx.Done()
	logger.Info("shutdown complete")
}
