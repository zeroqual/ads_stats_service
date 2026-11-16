package event

import (
	"errors"
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

func (e Event) Validate() error {
	var errs []error

	if e.EventID == uuid.Nil {
		errs = append(errs, errors.New("EventID is empty"))
	}
	if e.CampaignID == 0 {
		errs = append(errs, errors.New("CampaignID is empty"))
	}
	if e.AdvertiserID == 0 {
		errs = append(errs, errors.New("AdvertiserID is empty"))
	}
	if e.Type != Impression && e.Type != Click {
		errs = append(errs, errors.New("bad type"))
	}
	if e.CostMicros < 0 {
		errs = append(errs, errors.New("CostMicros must be >=0"))
	}
	if e.EventTime.IsZero() {
		errs = append(errs, errors.New("EventTime is zero"))
	}
	return errors.Join(errs...)
}

type BadEvent struct {
	Value     []byte
	Partition int32
	Offset    int64
	Err       error
}
