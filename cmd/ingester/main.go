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
	"time"

	"github.com/joho/godotenv"
	"github.com/zeroqual/ads_stats_service/internal/config"
	"github.com/zeroqual/ads_stats_service/internal/kafka"
	"github.com/zeroqual/ads_stats_service/internal/logger"
)

const (
	statsInterval = 5 * time.Second
	serviceName   = "ingester-service"
)

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

	consumer, err := kafka.NewConsumer(ctx, cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.GroupID)
	if err != nil {
		return err
	}

	defer func() {
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := consumer.Commit(commitCtx); err != nil {
			log.Error("failed to commit",
				slog.Any("error", err),
			)
		}
		if err := consumer.Close(); err != nil {
			log.Error("close consumer", slog.Any("err", err))
		}
	}()

	consumeLoop(ctx, consumer, log)

	slog.Info("shutdown")

	return nil
}

func consumeLoop(ctx context.Context, consumer *kafka.Consumer, log *slog.Logger) {
	var consumed int // прочитано с момента прошлой статистики
	lastStats := time.Now()
	for {
		events, err := consumer.Consume(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, kafka.ErrClosed) {
				return
			}
			log.Error("consume error",
				slog.Any("error", err),
			)
			continue
		}
		consumed += len(events)

		if err := consumer.Commit(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error("failed to commit",
				slog.Any("error", err),
			)
		}
		if elapsed := time.Since(lastStats); elapsed >= statsInterval {
			log.Info("stats",
				slog.Int("consumed", consumed),
				slog.Float64("rps", float64(consumed)/elapsed.Seconds()),
			)
			consumed = 0
			lastStats = time.Now()
		}
	}
}
