package storage

import (
	"context"
	"fmt"
	"time"
)

type Granularity string

const (
	Hour Granularity = "hour"
	Day  Granularity = "day"
)

var bucketFuncs = map[Granularity]string{
	Hour: "toStartOfHour",
	Day:  "toStartOfDay",
}

func (g Granularity) Valid() bool {
	_, ok := bucketFuncs[g]
	return ok
}

type StatsQuery struct {
	AdvertiserID uint64
	CampaignID   uint64
	From         time.Time
	To           time.Time
	Granularity  Granularity
}

type Point struct {
	TS          time.Time `ch:"ts" json:"ts"`
	Impressions uint64    `ch:"impressions" json:"impressions"`
	Clicks      uint64    `ch:"clicks" json:"clicks"`
	CostMicros  int64     `ch:"cost_micros" json:"cost_micros"`
}

func (c *CHConnector) Stats(ctx context.Context, q StatsQuery) ([]Point, error) {
	bucket, ok := bucketFuncs[q.Granularity]
	if !ok {
		return nil, fmt.Errorf("unknown granularity %q", q.Granularity)
	}

	args := []any{q.AdvertiserID}
	campaignFilter := ""
	if q.CampaignID != 0 {
		campaignFilter = "AND campaign_id = ?"
		args = append(args, q.CampaignID)
	}
	args = append(args, q.From.UTC(), q.To.UTC())

	query := fmt.Sprintf(`
		SELECT
			%s(hour)         AS ts,
			sum(impressions) AS impressions,
			sum(clicks)      AS clicks,
			sum(cost_micros) AS cost_micros
		FROM ad_stats_hourly
		WHERE advertiser_id = ? %s
		  AND hour >= ? AND hour < ?
		GROUP BY ts
		ORDER BY ts`, bucket, campaignFilter)

	var points []Point
	if err := c.conn.Select(ctx, &points, query, args...); err != nil {
		return nil, fmt.Errorf("select stats: %w", err)
	}
	return points, nil
}
