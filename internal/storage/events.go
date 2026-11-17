package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/zeroqual/ads_stats_service/internal/event"
)

type CHConnector struct {
	conn driver.Conn
}

func NewCHConnector(ctx context.Context, address, dbName, username, password string) (*CHConnector, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: dbName,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open ch: %w", err)
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctxTimeout); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to ping ch: %w", err)
	}

	return &CHConnector{
		conn: conn,
	}, nil
}

func (c *CHConnector) Insert(ctx context.Context, events []event.Event) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO ad_events (
			event_id, event_type, advertiser_id, campaign_id, ad_id, cost_micros, event_time
		)`,
	)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer func() {
		_ = batch.Abort()
	}()
	for _, ev := range events {
		err := batch.Append(ev.EventID, string(ev.Type), ev.AdvertiserID, ev.CampaignID, ev.AdID, ev.CostMicros, ev.EventTime)
		if err != nil {
			return fmt.Errorf("failed to append in batch: %w", err)
		}
	}
	err = batch.Send()
	if err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}

func (c *CHConnector) Ping(ctx context.Context) error {
	if err := c.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	return nil
}

func (c *CHConnector) Close() error {
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close ch: %w", err)
	}
	return nil
}
