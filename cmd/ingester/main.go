package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zeroqual/ads_stats_service/internal/config"
	"github.com/zeroqual/ads_stats_service/internal/dedup"
	"github.com/zeroqual/ads_stats_service/internal/ingester"
	"github.com/zeroqual/ads_stats_service/internal/kafka"
	"github.com/zeroqual/ads_stats_service/internal/logger"
	"github.com/zeroqual/ads_stats_service/internal/storage"
)

const (
	serviceName = "ingester"

	statsInterval = 5 * time.Second

	opTimeout = 10 * time.Second

	shutdownTimeout = 15 * time.Second

	backoffBase = 100 * time.Millisecond
	backoffMax  = 5 * time.Second

	minPollWait = 10 * time.Millisecond
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer, err := kafka.NewConsumer(ctx, cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.GroupID)
	if err != nil {
		return err
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Error("close consumer", slog.Any("err", err))
		}
	}()
	producerDLQ, err := kafka.NewProducer(ctx, cfg.Kafka.Brokers, cfg.Kafka.TopicDLQ, "ads-ingester-dlq")
	if err != nil {
		return err
	}
	defer func() {
		if err := producerDLQ.Close(); err != nil {
			log.Error("close dlq producer", slog.Any("err", err))
		}
	}()

	store, err := storage.NewCHConnector(ctx,
		cfg.ClickHouse.Address, cfg.ClickHouse.DatabaseName,
		cfg.ClickHouse.Username, cfg.ClickHouse.Password,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("close clickhouse", slog.Any("err", err))
		}
	}()

	deduper, err := dedup.New(ctx, cfg.Redis.Address, cfg.Redis.Password, cfg.DedupTTL)
	if err != nil {
		return err
	}
	defer func() {
		if err := deduper.Close(); err != nil {
			log.Error("close redis", slog.Any("err", err))
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	serv := http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("starting server", slog.String("PORT", serv.Addr))
		if err := serv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("failed to start server", slog.Any("err", err))
			return
		}
	}()

	ing := ingester.New(consumer, store, deduper, producerDLQ, ingester.Config{
		BatchSize:       cfg.BatchSize,
		FlushInterval:   cfg.FlushInterval,
		OpTimeout:       opTimeout,
		ShutdownTimeout: shutdownTimeout,
		BackoffBase:     backoffBase,
		BackoffMax:      backoffMax,
		StatsInterval:   statsInterval,
		MinPollWait:     minPollWait,
	}, log)

	log.Info("started", slog.Any("config", cfg))
	ing.Run(ctx)
	log.Info("stopped")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := serv.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics server shutdown", slog.Any("err", err))
	}
	slog.Info("Server exited gracefully")
	return nil
}
