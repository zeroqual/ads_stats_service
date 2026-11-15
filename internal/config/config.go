package config

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/caarlos0/env/v11"
)

type Base struct {
	LogLevel slog.Level `env:"LOG_LEVEL" envDefault:"info"`
}

type Kafka struct {
	Brokers  []string `env:"BROKERS" envDefault:"localhost:9092" envSeparator:","`
	Topic    string   `env:"TOPIC" envDefault:"ad_events"`
	TopicDLQ string   `env:"DLQ_TOPIC" envDefault:"ad_events_dlq"`
}

type ClickHouse struct {
	Address      string `env:"ADDR" envDefault:"localhost:9000"`
	DatabaseName string `env:"DB" envDefault:"ads"`
	Username     string `env:"USER" envDefault:"ads"`
	Password     string `env:"PASSWORD,required" json:"-"`
}

type Redis struct {
	Address  string `env:"ADDR" envDefault:"localhost:6379"`
	Password string `env:"PASSWORD,required" json:"-"`
}

type Generator struct {
	Base
	Kafka         Kafka   `envPrefix:"KAFKA_"`
	RPS           int     `env:"GEN_RPS" envDefault:"1000"`
	Campaigns     int     `env:"GEN_CAMPAIGNS" envDefault:"1000"`
	Advertisers   int     `env:"GEN_ADVERTISERS" envDefault:"100"`
	ClickRate     float64 `env:"GEN_CLICK_RATE" envDefault:"0.02"`
	DuplicateRate float64 `env:"GEN_DUPLICATE_RATE" envDefault:"0"`
}

type Ingester struct {
	Base
	Kafka         Kafka         `envPrefix:"KAFKA_"`
	ClickHouse    ClickHouse    `envPrefix:"CLICKHOUSE_"`
	BatchSize     int           `env:"BATCH_SIZE" envDefault:"5000"`
	FlushInterval time.Duration `env:"BATCH_FLUSH_INTERVAL" envDefault:"1s"`
	GroupID       string        `env:"KAFKA_GROUP_ID" envDefault:"ads-ingester"`
	Redis         Redis         `envPrefix:"REDIS_"`
	DedupTTL      time.Duration `env:"DEDUP_TTL" envDefault:"1h"`
}

type ReportAPI struct {
	Base
	ClickHouse ClickHouse `envPrefix:"CLICKHOUSE_"`
	Addr       string     `env:"HTTP_ADDR" envDefault:":8080"`
}

// custom validate inside config.Load()
type validator interface {
	Validate() error
}

func Load[T validator]() (T, error) {
	cfg, err := env.ParseAs[T]()
	if err != nil {
		return cfg, fmt.Errorf("parse env: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c Generator) Validate() error {
	var errs []error
	if c.RPS <= 0 {
		errs = append(errs, errors.New("GEN_RPS must be > 0"))
	}
	if c.Campaigns <= 0 {
		errs = append(errs, errors.New("GEN_CAMPAIGNS must be > 0"))
	}
	if c.Advertisers <= 0 {
		errs = append(errs, errors.New("GEN_ADVERTISERS must be > 0"))
	}
	if c.ClickRate > 1 || c.ClickRate < 0 {
		errs = append(errs, errors.New("GEN_CLICK_RATE must be in [0, 1]"))
	}
	if c.DuplicateRate < 0 || c.DuplicateRate > 1 {
		errs = append(errs, errors.New("GEN_DUPLICATE_RATE must be in [0, 1]"))
	}
	return errors.Join(errs...)
}

func (c Ingester) Validate() error {
	var errs []error
	if c.BatchSize <= 0 {
		errs = append(errs, errors.New("BATCH_SIZE must be > 0"))
	}
	if c.FlushInterval <= 0 {
		errs = append(errs, errors.New("BATCH_FLUSH_INTERVAL must be > 0"))
	}
	if c.ClickHouse.Address == "" {
		errs = append(errs, errors.New("CLICKHOUSE_ADDR is empty"))
	}
	if c.Redis.Address == "" {
		errs = append(errs, errors.New("REDIS_ADDR is empty"))
	}
	if c.DedupTTL <= 0 {
		errs = append(errs, errors.New("DEDUP_TTL must be > 0"))
	}
	return errors.Join(errs...)
}

func (c ReportAPI) Validate() error {
	return nil
}
