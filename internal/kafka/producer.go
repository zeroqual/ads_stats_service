package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeroqual/ads_stats_service/internal/event"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(ctx context.Context, brokers []string, topic string, clientID string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic), kgo.ClientID(clientID))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for producer: %w", err)
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(ctxTimeout); err != nil {
		cl.Close()
		return nil, fmt.Errorf("failed to ping kafka: %w", err)
	}

	return &Producer{
		client: cl,
	}, nil
}

func (p *Producer) Send(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(events))

	for _, ev := range events {
		payload, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		records = append(records, &kgo.Record{
			Key:   strconv.AppendUint(nil, ev.CampaignID, 10),
			Value: payload,
		})
	}

	results := p.client.ProduceSync(ctx, records...)
	err := results.FirstErr()
	if err != nil {
		return fmt.Errorf("failed to produce: %w", err)
	}

	return nil
}

func (p *Producer) SendDLQ(ctx context.Context, badEvents []event.BadEvent) error {
	if len(badEvents) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(badEvents))

	for _, ev := range badEvents {
		records = append(records, &kgo.Record{
			Value: ev.Value,
			Headers: []kgo.RecordHeader{
				{
					Key:   "errors",
					Value: []byte(ev.Err.Error()),
				},
				{
					Key:   "source_partition",
					Value: strconv.AppendInt(nil, int64(ev.Partition), 10),
				},
				{
					Key:   "source_offset",
					Value: strconv.AppendInt(nil, ev.Offset, 10),
				},
			},
		})
	}

	results := p.client.ProduceSync(ctx, records...)
	err := results.FirstErr()
	if err != nil {
		return fmt.Errorf("failed to produce: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	p.client.Close()
	return nil
}
