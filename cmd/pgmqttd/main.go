package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/db"
	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/leader"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/operator"
)

func main() {
	level := slog.LevelInfo
	if os.Getenv("PGMQTT_LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("db open", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		logger.Error("migrate", "err", err)
		os.Exit(1)
	}

	eng, err := engine.New(ctx, cfg, pool, logger)
	if err != nil {
		logger.Error("engine new", "err", err)
		os.Exit(1)
	}

	lst, err := listener.Start(ctx, cfg.DatabaseURL, eng, logger)
	if err != nil {
		logger.Error("listener", "err", err)
		os.Exit(1)
	}
	defer lst.Stop()
	eng.SetBrokerID(lst.BrokerID())
	eng.SetNotifier(listener.NewNotifier(pool))
	eng.SetTakeoverNotifier(listener.NewTakeoverNotifier(pool))

	ld, err := leader.Start(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("leader", "err", err)
		os.Exit(1)
	}
	defer ld.Stop()

	jt := janitor.New(pool, eng, logger)
	go jt.RunWith(ctx, ld)

	go func() {
		opts := operator.Options{
			ServiceHost: cfg.ServiceHost,
			ServicePort: cfg.ServicePort,
			WSPort:      cfg.WSPort,
			BcryptCost:  cfg.BcryptCost,
		}
		if err := operator.Run(ctx, ld, pool, logger, opts); err != nil {
			logger.Error("operator", "err", err)
		}
	}()

	logger.Info("pgmqttd starting", "tcp", cfg.TCPAddr, "ws", cfg.WSAddr, "broker", eng.BrokerID())
	if err := eng.Serve(ctx); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}
