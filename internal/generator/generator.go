package generator

import (
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/zeroqual/ads_stats_service/internal/config"
	"github.com/zeroqual/ads_stats_service/internal/event"
)

const (
	adsPerCampaign = 5

	avgImpressionCostMicros = 150_000

	avgClickCostMicros = 15_000_000

	costJitter = 0.3
)

type Generator struct {
	clickRate   float64
	campaigns   int
	advertisers int

	rnd *rand.Rand
	now func() time.Time
}

func New(cfg config.Generator, rnd *rand.Rand) *Generator {
	if rnd == nil {
		rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &Generator{
		clickRate:   cfg.ClickRate,
		campaigns:   cfg.Campaigns,
		advertisers: cfg.Advertisers,
		rnd:         rnd,
		now:         time.Now,
	}
}

func (g *Generator) Next() event.Event {
	campaignID := g.pickCampaign()

	eventType := event.Impression
	cost := g.jitter(avgImpressionCostMicros)
	if g.rnd.Float64() < g.clickRate {
		eventType = event.Click
		cost = g.jitter(avgClickCostMicros)
	}

	return event.Event{
		EventID:      uuid.New(),
		Type:         eventType,
		AdvertiserID: g.advertiserOf(campaignID),
		CampaignID:   campaignID,
		AdID:         g.pickAd(campaignID),
		CostMicros:   cost,
		EventTime:    g.now().UTC(),
	}
}

func (g *Generator) pickCampaign() uint64 {
	return uint64(g.rnd.IntN(g.campaigns)) + 1
}

func (g *Generator) advertiserOf(campaignID uint64) uint64 {
	return (campaignID-1)%uint64(g.advertisers) + 1
}

func (g *Generator) pickAd(campaignID uint64) uint64 {
	return campaignID*100 + uint64(g.rnd.IntN(adsPerCampaign)) + 1
}

func (g *Generator) jitter(avg int64) int64 {
	k := 1 + costJitter*(2*g.rnd.Float64()-1)
	return int64(float64(avg) * k)
}
