package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	cacheHit   = "hit"
	cacheMiss  = "miss"
	cacheError = "error"
)

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ads_api_requests_total",
		Help: "HTTP requests by route and status code.",
	}, []string{"method", "route", "code"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ads_api_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"method", "route"})

	cacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ads_api_cache_requests_total",
		Help: "Report cache lookups by result: hit, miss, error.",
	}, []string{"result"})

	queryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ads_api_clickhouse_query_duration_seconds",
		Help:    "Latency of stats queries to ClickHouse.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
)
