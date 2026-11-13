package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/zeroqual/ads_stats_service/internal/config"
	"github.com/zeroqual/ads_stats_service/internal/logger"
)

const serviceName = "ingester-service"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func run() error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("load .env: %w", err)
	}
	cfg, err := config.Load[config.Ingester]()
	if err != nil {
		return err
	}

	log := logger.New(serviceName, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("started", slog.Any("config", cfg))

	<-ctx.Done()

	slog.Info("shutdown")

	return nil
}
