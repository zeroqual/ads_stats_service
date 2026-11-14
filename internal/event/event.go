package event

import (
	"time"

	"github.com/google/uuid"
)

type Type string

const (
	Impression Type = "impression"
	Click      Type = "click"
)

type Event struct {
	EventID      uuid.UUID `json:"event_id"`
	Type         Type      `json:"type"`
	AdvertiserID uint64    `json:"advertiser_id"`
	CampaignID   uint64    `json:"campaign_id"`
	AdID         uint64    `json:"ad_id"`
	CostMicros   int64     `json:"cost_micros"`
	EventTime    time.Time `json:"event_time"`
}
