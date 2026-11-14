package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeroqual/ads_stats_service/internal/event"
)

var ErrClosed = errors.New("consumer closed")

type Consumer struct {
	client *kgo.Client
}

func NewConsumer(ctx context.Context, brokers []string, topic string, groupID string) (*Consumer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ClientID("ads-ingester"),
		kgo.ConsumerGroup(groupID),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to client for consumer: %w", err)
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(ctxTimeout); err != nil {
		cl.Close()
		return nil, fmt.Errorf("failed to ping kafka: %w", err)
	}

	return &Consumer{
		client: cl,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context) ([]event.Event, error) {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil, ErrClosed
		}

		for _, fe := range fetches.Errors() {

			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				continue
			}
			slog.Error("fetch", slog.String("topic", fe.Topic),
				slog.Int("partition", int(fe.Partition)), slog.Any("err", fe.Err))
		}

		events := make([]event.Event, 0, len(fetches.Records()))
		fetches.EachRecord(func(r *kgo.Record) {
			var evt event.Event
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				slog.Warn("bad event",
					slog.String("topic", r.Topic),
					slog.Int("partition", int(r.Partition)),
					slog.Any("error", err),
					slog.Int64("offset", r.Offset),
				)
				return
			}

			// slog.Debug("new event",
			// 	slog.Any("event", evt),
			// )
			events = append(events, evt)
		})

		if len(events) > 0 {
			return events, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (c *Consumer) Commit(ctx context.Context) error {
	err := c.client.CommitUncommittedOffsets(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit offsets: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error {
	c.client.CloseAllowingRebalance()
	return nil
}

// разрешаем ребаланс, только когда буфер пуст
func (c *Consumer) AllowRebalance() {
	c.client.AllowRebalance()
}
