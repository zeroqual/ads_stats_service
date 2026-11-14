-- +goose Up
CREATE TABLE ad_stats_hourly
(
    hour DateTime('UTC'),
    advertiser_id UInt64,
    campaign_id UInt64,
    ad_id UInt64,
    impressions UInt64,
    clicks UInt64,
    cost_micros Int64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (advertiser_id, campaign_id, ad_id, hour);
-- +goose Down
DROP TABLE IF EXISTS ad_stats_hourly;
