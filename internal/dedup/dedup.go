package dedup

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type RedisDeduper struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(ctx context.Context, addr, password string, ttl time.Duration) (*RedisDeduper, error) {
	cl := redis.NewClient(&redis.Options{
		Addr:       addr,
		ClientName: "deduper",
		Password:   password,
	})

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := cl.Ping(ctxTimeout).Err(); err != nil {
		err = cl.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close redis and ping down: %w", err)
		}
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	return &RedisDeduper{
		rdb: cl,
		ttl: ttl,
	}, nil
}

func (d *RedisDeduper) Seen(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
	exists := make(map[uuid.UUID]bool, len(ids))
	if len(ids) == 0 {
		return exists, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = makeKey(id.String())
	}

	values, err := d.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to check ids in redis: %w", err)
	}

	for i, value := range values {
		exists[ids[i]] = value != nil
	}
	return exists, nil
}

func (d *RedisDeduper) Mark(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	pipe := d.rdb.Pipeline()
	for _, id := range ids {
		pipe.Set(ctx, makeKey(id.String()), 1, d.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (d *RedisDeduper) Close() error {
	if err := d.rdb.Close(); err != nil {
		return err
	}
	return nil
}

func makeKey(id string) string {
	return fmt.Sprintf("dedup:%s", id)
}
