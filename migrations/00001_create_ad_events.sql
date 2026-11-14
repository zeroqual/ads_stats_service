-- +goose Up
CREATE TABLE ad_events 
(
    event_id UUID,
    event_type LowCardinality(String),
    advertiser_id UInt64,
    campaign_id UInt64,
    ad_id UInt64,
    cost_micros Int64,
    event_time DateTime64(3, 'UTC'),
    inserted_at DateTime DEFAULT now(),
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (advertiser_id, campaign_id, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE;
-- +goose Down
DROP TABLE IF EXISTS ad_events;
