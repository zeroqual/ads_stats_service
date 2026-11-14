package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/zeroqual/ads_stats_service/internal/config"
	"github.com/zeroqual/ads_stats_service/internal/event"
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

	ing := newIngester(cfg, consumer, store, log)

	log.Info("started", slog.Any("config", cfg))
	ing.run(ctx)
	ing.shutdown()
	log.Info("stopped")

	return nil
}

type ingester struct {
	consumer *kafka.Consumer
	store    *storage.CHConnector
	log      *slog.Logger

	batchSize     int
	flushInterval time.Duration

	buffer  []event.Event
	firstAt time.Time

	stats stats
}

type stats struct {
	since     time.Time
	consumed  int
	inserted  int
	flushes   int
	maxInsert time.Duration
}

func newIngester(cfg config.Ingester, consumer *kafka.Consumer, store *storage.CHConnector, log *slog.Logger) *ingester {
	return &ingester{
		consumer:      consumer,
		store:         store,
		log:           log,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		buffer:        make([]event.Event, 0, cfg.BatchSize),
		stats:         stats{since: time.Now()},
	}
}

func (i *ingester) run(ctx context.Context) {
	for {
		events, err := i.poll(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, kafka.ErrClosed) {
				return
			}
			i.log.Error("poll", slog.Any("err", err))
			continue
		}

		i.add(events)

		if i.shouldFlush() {
			if err := i.flush(ctx); err != nil {

				return
			}
		}

		if len(i.buffer) == 0 {
			i.consumer.AllowRebalance()
		}

		i.maybeLogStats()
	}
}

func (i *ingester) poll(ctx context.Context) ([]event.Event, error) {
	wait := i.flushInterval
	if len(i.buffer) > 0 {
		wait = max(i.flushInterval-time.Since(i.firstAt), minPollWait)
	}

	pollCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	events, err := i.consumer.Consume(pollCtx)

	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return nil, nil
	}
	return events, err
}

func (i *ingester) add(events []event.Event) {
	if len(events) == 0 {
		return
	}
	if len(i.buffer) == 0 {
		i.firstAt = time.Now()
	}
	i.buffer = append(i.buffer, events...)
	i.stats.consumed += len(events)
}

func (i *ingester) shouldFlush() bool {
	if len(i.buffer) == 0 {
		return false
	}
	return len(i.buffer) >= i.batchSize || time.Since(i.firstAt) >= i.flushInterval
}

func (i *ingester) flush(ctx context.Context) error {
	if len(i.buffer) == 0 {
		return nil
	}

	took, err := i.insertWithRetry(ctx)
	if err != nil {
		return err
	}

	i.stats.inserted += len(i.buffer)
	i.stats.flushes++
	i.stats.maxInsert = max(i.stats.maxInsert, took)

	i.buffer = i.buffer[:0]
	i.commit(ctx)
	return nil
}

func (i *ingester) insertWithRetry(ctx context.Context) (time.Duration, error) {
	for attempt := 0; ; attempt++ {
		start := time.Now()

		insertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
		err := i.store.Insert(insertCtx, i.buffer)
		cancel()

		if err == nil {
			return time.Since(start), nil
		}

		delay := backoff(attempt)
		i.log.Warn("insert failed, retrying",
			slog.Int("attempt", attempt+1),
			slog.Int("batch_size", len(i.buffer)),
			slog.Duration("retry_in", delay),
			slog.Any("err", err),
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (i *ingester) commit(ctx context.Context) {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()

	if err := i.consumer.Commit(commitCtx); err != nil {
		i.log.Error("commit offsets", slog.Any("err", err))
	}
}

func (i *ingester) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := i.flush(ctx); err != nil {

		i.log.Error("final flush failed, events will be re-read after restart",
			slog.Int("buffered", len(i.buffer)),
			slog.Any("err", err),
		)
		return
	}

	i.commit(ctx)
	i.maybeLogStats()
}

func (i *ingester) maybeLogStats() {
	elapsed := time.Since(i.stats.since)
	if elapsed < statsInterval {
		return
	}

	avgBatch := 0
	if i.stats.flushes > 0 {
		avgBatch = i.stats.inserted / i.stats.flushes
	}

	i.log.Info("stats",
		slog.Int("consumed", i.stats.consumed),
		slog.Int("inserted", i.stats.inserted),
		slog.Int("flushes", i.stats.flushes),
		slog.Int("avg_batch", avgBatch),
		slog.Duration("max_insert", i.stats.maxInsert),
		slog.Int("buffered", len(i.buffer)),
		slog.Float64("rps", float64(i.stats.inserted)/elapsed.Seconds()),
	)
	i.stats = stats{since: time.Now()}
}

func backoff(attempt int) time.Duration {
	d := backoffMax
	if attempt < 10 {
		d = min(backoffBase<<attempt, backoffMax)
	}
	return d/2 + rand.N(d/2+1)
}
