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
	"github.com/zeroqual/ads_stats_service/internal/event"
	"github.com/zeroqual/ads_stats_service/internal/generator"
	"github.com/zeroqual/ads_stats_service/internal/kafka"
	"github.com/zeroqual/ads_stats_service/internal/logger"
	"github.com/zeroqual/ads_stats_service/internal/metrics"
)

const (
	serviceName = "generator"

	ticksPerSecond = 10

	statsInterval = 5 * time.Second
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
	cfg, err := config.Load[config.Generator]()
	if err != nil {
		return err
	}

	log := logger.New(serviceName, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("started", slog.Any("config", cfg))

	gen := generator.New(cfg, nil)
	producer, err := kafka.NewProducer(ctx, cfg.Kafka.Brokers, cfg.Kafka.Topic, "ads-generator")
	if err != nil {
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("close producer", slog.Any("err", err))
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
	produceLoop(ctx, log, gen, producer, cfg.RPS)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := serv.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics server shutdown", slog.Any("err", err))
	}
	slog.Info("Server exited gracefully")

	return nil
}

func produceLoop(ctx context.Context, log *slog.Logger, gen *generator.Generator, prod *kafka.Producer, rps int) {
	base := rps / ticksPerSecond
	extra := rps % ticksPerSecond
	sendTicker := time.NewTicker(time.Second / ticksPerSecond)
	defer sendTicker.Stop()

	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	batch := make([]event.Event, 0, base+1)

	var (
		tick   int
		sent   int
		failed int
	)

	for {
		select {
		case <-ctx.Done():
			return

		case <-statsTicker.C:
			log.Info("stats",
				slog.Int("sent", sent),
				slog.Int("failed", failed),
				slog.Float64("rps", float64(sent)/statsInterval.Seconds()),
			)
			sent, failed = 0, 0

		case <-sendTicker.C:
			n := base
			if tick < extra {
				n++
			}
			tick = (tick + 1) % ticksPerSecond

			batch = batch[:0]
			for range n {
				batch = append(batch, gen.Next())
			}

			if err := prod.Send(ctx, batch); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				failed += len(batch)
				metrics.GeneratorEventCountErr.Add(float64(len(batch)))
				log.Error("send batch", slog.Any("err", err), slog.Int("size", len(batch)))
				continue
			}
			sent += len(batch)
			metrics.GeneratorEventCount.Add(float64(len(batch)))
		}
	}
}
