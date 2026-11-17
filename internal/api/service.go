package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/zeroqual/ads_stats_service/internal/storage"
)

type StatsReader interface {
	Stats(ctx context.Context, q storage.StatsQuery) ([]storage.Point, error)
	Ping(ctx context.Context) error
}

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
}

type Totals struct {
	Impressions uint64  `json:"impressions"`
	Clicks      uint64  `json:"clicks"`
	CostMicros  int64   `json:"cost_micros"`
	CTR         float64 `json:"ctr"`
}

type Report struct {
	AdvertiserID uint64              `json:"advertiser_id"`
	CampaignID   uint64              `json:"campaign_id,omitempty"`
	From         time.Time           `json:"from"`
	To           time.Time           `json:"to"`
	Granularity  storage.Granularity `json:"granularity"`
	Totals       Totals              `json:"totals"`
	Points       []storage.Point     `json:"points"`
}

type ServiceConfig struct {
	CacheTTL     time.Duration
	QueryTimeout time.Duration
}

type Service struct {
	reader StatsReader
	cache  Cache
	cfg    ServiceConfig
	log    *slog.Logger
}

func NewService(reader StatsReader, cache Cache, cfg ServiceConfig, log *slog.Logger) *Service {
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 5 * time.Second
	}
	if cfg.CacheTTL <= 0 {
		cache = nil
	}
	return &Service{reader: reader, cache: cache, cfg: cfg, log: log}
}

func (s *Service) Report(ctx context.Context, q storage.StatsQuery) (Report, bool, error) {
	key := cacheKey(q)

	if rep, ok := s.fromCache(ctx, key); ok {
		return rep, true, nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()

	start := time.Now()
	points, err := s.reader.Stats(queryCtx, q)
	queryDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		return Report{}, false, fmt.Errorf("read stats: %w", err)
	}

	rep := buildReport(q, points)
	s.toCache(ctx, key, rep)
	return rep, false, nil
}

func (s *Service) Ready(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.reader.Ping(pingCtx)
}

func (s *Service) fromCache(ctx context.Context, key string) (Report, bool) {
	if s.cache == nil {
		return Report{}, false
	}

	data, ok, err := s.cache.Get(ctx, key)
	if err != nil {
		cacheRequests.WithLabelValues(cacheError).Inc()
		s.log.Warn("cache get failed, going to clickhouse", slog.Any("err", err))
		return Report{}, false
	}
	if !ok {
		cacheRequests.WithLabelValues(cacheMiss).Inc()
		return Report{}, false
	}

	var rep Report
	if err := json.Unmarshal(data, &rep); err != nil {
		cacheRequests.WithLabelValues(cacheError).Inc()
		s.log.Warn("cache entry is corrupted", slog.String("key", key), slog.Any("err", err))
		return Report{}, false
	}
	cacheRequests.WithLabelValues(cacheHit).Inc()
	return rep, true
}

func (s *Service) toCache(ctx context.Context, key string, rep Report) {
	if s.cache == nil {
		return
	}

	data, err := json.Marshal(rep)
	if err != nil {
		s.log.Warn("marshal report for cache", slog.Any("err", err))
		return
	}
	if err := s.cache.Set(ctx, key, data, s.cfg.CacheTTL); err != nil {
		s.log.Warn("cache set failed", slog.Any("err", err))
	}
}

func cacheKey(q storage.StatsQuery) string {
	return fmt.Sprintf("stats:v1:%d:%d:%s:%d:%d",
		q.AdvertiserID, q.CampaignID, q.Granularity, q.From.Unix(), q.To.Unix())
}

func buildReport(q storage.StatsQuery, points []storage.Point) Report {
	if points == nil {
		points = []storage.Point{}
	}

	var t Totals
	for _, p := range points {
		t.Impressions += p.Impressions
		t.Clicks += p.Clicks
		t.CostMicros += p.CostMicros
	}
	if t.Impressions > 0 {
		t.CTR = float64(t.Clicks) / float64(t.Impressions)
	}

	return Report{
		AdvertiserID: q.AdvertiserID,
		CampaignID:   q.CampaignID,
		From:         q.From.UTC(),
		To:           q.To.UTC(),
		Granularity:  q.Granularity,
		Totals:       t,
		Points:       points,
	}
}
