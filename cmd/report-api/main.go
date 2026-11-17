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

	"github.com/zeroqual/ads_stats_service/internal/api"
	"github.com/zeroqual/ads_stats_service/internal/cache"
	"github.com/zeroqual/ads_stats_service/internal/config"
	"github.com/zeroqual/ads_stats_service/internal/logger"
	"github.com/zeroqual/ads_stats_service/internal/storage"
)

const serviceName = "report-api"

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

	cfg, err := config.Load[config.ReportAPI]()
	if err != nil {
		return err
	}

	log := logger.New(serviceName, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	var statsCache api.Cache
	redisCache, err := cache.NewRedis(ctx, cfg.Redis.Address, cfg.Redis.Password)
	if err != nil {
		log.Warn("redis unavailable, cache disabled", slog.Any("err", err))
	} else {
		statsCache = redisCache
		defer func() {
			if err := redisCache.Close(); err != nil {
				log.Error("close redis", slog.Any("err", err))
			}
		}()
	}

	svc := api.NewService(store, statsCache, api.ServiceConfig{
		CacheTTL:     cfg.CacheTTL,
		QueryTimeout: cfg.QueryTimeout,
	}, log)
	router := api.NewRouter(api.NewHandler(svc, log), log)

	log.Info("started", slog.Any("config", cfg))
	if err := api.Serve(ctx, cfg.Addr, router, log); err != nil {
		return err
	}
	log.Info("stopped")
	return nil
}
