package ingester

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/zeroqual/ads_stats_service/internal/event"
)

type Source interface {
	Consume(ctx context.Context) ([]event.Event, error)
	Commit(ctx context.Context) error
	AllowRebalance()
}

type Sink interface {
	Insert(ctx context.Context, events []event.Event) error
}

type Config struct {
	BatchSize       int
	FlushInterval   time.Duration
	OpTimeout       time.Duration
	ShutdownTimeout time.Duration
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	StatsInterval   time.Duration
	MinPollWait     time.Duration
}

func (c Config) withDefaults() Config {
	if c.OpTimeout == 0 {
		c.OpTimeout = 10 * time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 15 * time.Second
	}
	if c.BackoffBase == 0 {
		c.BackoffBase = 100 * time.Millisecond
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = 5 * time.Second
	}
	if c.StatsInterval == 0 {
		c.StatsInterval = 5 * time.Second
	}
	if c.MinPollWait == 0 {
		c.MinPollWait = 10 * time.Millisecond
	}
	return c
}

type Deduper interface {
	Seen(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error)
	Mark(ctx context.Context, ids []uuid.UUID) error
}

type stats struct {
	since        time.Time
	consumed     int
	inserted     int
	flushes      int
	maxInsert    time.Duration
	dedupDropped int
}

type Ingester struct {
	src   Source
	sink  Sink
	dedup Deduper
	log   *slog.Logger
	cfg   Config

	buffer  []event.Event
	firstAt time.Time

	stats stats
}

func New(src Source, sink Sink, dedup Deduper, cfg Config, log *slog.Logger) *Ingester {
	if cfg.BatchSize <= 0 {
		panic("BATCH_SIZE must be > 0")
	}
	if cfg.FlushInterval <= 0 {
		panic("BATCH_FLUSH_INTERVAL must be > 0")
	}
	cfg = cfg.withDefaults()
	return &Ingester{
		src:    src,
		sink:   sink,
		dedup:  dedup,
		log:    log,
		cfg:    cfg,
		buffer: make([]event.Event, 0, cfg.BatchSize),
		stats:  stats{since: time.Now()},
	}
}

func (i *Ingester) Run(ctx context.Context) {
	defer i.shutdown()
	for {
		events, err := i.poll(ctx)
		if err != nil {
			if ctx.Err() == nil {
				i.log.Error("source stopped", slog.Any("err", err))
			}
			return
		}

		i.add(events)

		if i.shouldFlush() {
			if err := i.flush(ctx); err != nil {
				return
			}
		}

		if len(i.buffer) == 0 {
			i.src.AllowRebalance()
		}

		i.maybeLogStats()
	}
}

func (i *Ingester) poll(ctx context.Context) ([]event.Event, error) {
	wait := i.cfg.FlushInterval
	if len(i.buffer) > 0 {
		wait = max(i.cfg.FlushInterval-time.Since(i.firstAt), i.cfg.MinPollWait)
	}

	pollCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	events, err := i.src.Consume(pollCtx)

	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return nil, nil
	}
	return events, err
}

func (i *Ingester) add(events []event.Event) {
	if len(events) == 0 {
		return
	}
	if len(i.buffer) == 0 {
		i.firstAt = time.Now()
	}
	i.buffer = append(i.buffer, events...)
	i.stats.consumed += len(events)
}

func (i *Ingester) shouldFlush() bool {
	if len(i.buffer) == 0 {
		return false
	}
	return len(i.buffer) >= i.cfg.BatchSize || time.Since(i.firstAt) >= i.cfg.FlushInterval
}

func (i *Ingester) flush(ctx context.Context) error {
	if len(i.buffer) == 0 {
		return nil
	}

	i.dropDuplicates(ctx)

	if len(i.buffer) == 0 {
		i.commit(ctx)
		return nil
	}

	took, err := i.insertWithRetry(ctx)
	if err != nil {
		return err
	}

	i.stats.inserted += len(i.buffer)
	i.stats.flushes++
	i.stats.maxInsert = max(i.stats.maxInsert, took)

	i.markInserted(ctx)

	i.buffer = i.buffer[:0]
	i.commit(ctx)
	return nil
}

func (i *Ingester) insertWithRetry(ctx context.Context) (time.Duration, error) {
	for attempt := 0; ; attempt++ {
		start := time.Now()

		insertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), i.cfg.OpTimeout)
		err := i.sink.Insert(insertCtx, i.buffer)
		cancel()

		if err == nil {
			return time.Since(start), nil
		}

		delay := i.backoff(attempt)
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

func (i *Ingester) commit(ctx context.Context) {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), i.cfg.OpTimeout)
	defer cancel()

	if err := i.src.Commit(commitCtx); err != nil {
		i.log.Error("commit offsets", slog.Any("err", err))
	}
}

func (i *Ingester) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), i.cfg.ShutdownTimeout)
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

func (i *Ingester) maybeLogStats() {
	elapsed := time.Since(i.stats.since)
	if elapsed < i.cfg.StatsInterval {
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
		slog.Int("dedup", i.stats.dedupDropped),
	)
	i.stats = stats{since: time.Now()}
}

func (i *Ingester) backoff(attempt int) time.Duration {
	d := i.cfg.BackoffMax
	if attempt < 10 {
		d = min(i.cfg.BackoffBase<<attempt, i.cfg.BackoffMax)
	}
	return d/2 + rand.N(d/2+1)
}

func (i *Ingester) dropDuplicates(ctx context.Context) {
	before := len(i.buffer)

	inBuffer := make(map[uuid.UUID]struct{}, len(i.buffer))

	out := i.buffer[:0]

	for _, ev := range i.buffer {
		if _, dup := inBuffer[ev.EventID]; dup {
			continue
		}
		inBuffer[ev.EventID] = struct{}{}
		out = append(out, ev)
	}

	i.buffer = out

	if i.dedup != nil && len(i.buffer) > 0 {
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), i.cfg.OpTimeout)
		seen, err := i.dedup.Seen(opCtx, eventIDs(i.buffer))
		cancel()

		if err != nil {
			i.log.Warn("dedup unavailable, skipping", slog.Any("err", err))
		} else {
			out := i.buffer[:0]
			for _, ev := range i.buffer {
				if !seen[ev.EventID] {
					out = append(out, ev)
				}
			}
			i.buffer = out
		}
	}

	i.stats.dedupDropped += before - len(i.buffer)
}

func (i *Ingester) markInserted(ctx context.Context) {
	if i.dedup == nil {
		return
	}
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), i.cfg.OpTimeout)
	defer cancel()

	if err := i.dedup.Mark(opCtx, eventIDs(i.buffer)); err != nil {
		i.log.Warn("dedup mark failed", slog.Any("err", err))
	}
}

func eventIDs(events []event.Event) []uuid.UUID {
	ids := make([]uuid.UUID, len(events))
	for k, ev := range events {
		ids[k] = ev.EventID
	}
	return ids
}
