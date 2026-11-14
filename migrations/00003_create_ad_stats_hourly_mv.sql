-- +goose Up
CREATE MATERIALIZED VIEW ad_stats_hourly_mv TO ad_stats_hourly AS
    SELECT toStartOfHour(event_time) AS hour, advertiser_id, ad_id, campaign_id,
        countIf(event_type = 'impression') AS impressions,
        countIf(event_type = 'click') AS clicks,
        sum(cost_micros) AS cost_micros
    FROM ad_events
GROUP BY hour, advertiser_id, campaign_id, ad_id;
-- +goose Down
DROP VIEW IF EXISTS ad_stats_hourly_mv;