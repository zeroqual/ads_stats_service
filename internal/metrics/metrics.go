package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Consumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_ingester_consumed_total",
		Help: "events read from kafka",
	})
	Inserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_ingester_inserted_total",
		Help: "events insert in ch",
	})
	InsertedErr = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_ingester_insert_errors_total",
		Help: "total errors on insert in ch",
	})
	DedupDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_ingester_dedup_dropped_total",
		Help: "dublicated dropped before insert in ch",
	})
	DLQTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_ingester_dlq_total",
		Help: "bad events total count",
	})
	InsertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ads_ingester_insert_duration_seconds",
		Help:    "ch insert latency",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ads_ingester_batch_size",
		Help:    "event batch size",
		Buckets: prometheus.ExponentialBuckets(10, 2, 12),
	})

	BufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ads_ingester_buffered",
		Help: "current count events in buffer",
	})
	GeneratorEventCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_generator_sent_total",
		Help: "count of events generator sent",
	})
	GeneratorEventCountErr = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ads_generator_send_errors_total",
		Help: "count of error in generator",
	})
)
