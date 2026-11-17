package api

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zeroqual/ads_stats_service/internal/storage"
)

const (
	defaultRange = 24 * time.Hour
	maxHourRange = 31 * 24 * time.Hour
	maxDayRange  = 366 * 24 * time.Hour
)

var errBadRequest = errors.New("bad request")

type Handler struct {
	svc *Service
	log *slog.Logger
	now func() time.Time
}

func NewHandler(svc *Service, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log, now: time.Now}
}

func (h *Handler) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) Readyz(c *gin.Context) {
	if err := h.svc.Ready(c.Request.Context()); err != nil {
		h.log.Warn("not ready", slog.Any("err", err))
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (h *Handler) AdvertiserStats(c *gin.Context) {
	h.stats(c, false)
}

func (h *Handler) CampaignStats(c *gin.Context) {
	h.stats(c, true)
}

func (h *Handler) stats(c *gin.Context, withCampaign bool) {
	q, err := h.parseQuery(c, withCampaign)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rep, cached, err := h.svc.Report(c.Request.Context(), q)
	if err != nil {
		h.log.Error("build report", slog.Any("err", err), slog.String("path", c.Request.URL.Path))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}

	if cached {
		c.Header("X-Cache", "HIT")
	} else {
		c.Header("X-Cache", "MISS")
	}
	c.JSON(http.StatusOK, rep)
}

func (h *Handler) parseQuery(c *gin.Context, withCampaign bool) (storage.StatsQuery, error) {
	var q storage.StatsQuery

	advertiserID, err := parseID(c.Param("advertiserID"), "advertiserID")
	if err != nil {
		return q, err
	}
	q.AdvertiserID = advertiserID

	if withCampaign {
		campaignID, err := parseID(c.Param("campaignID"), "campaignID")
		if err != nil {
			return q, err
		}
		q.CampaignID = campaignID
	}

	q.Granularity = storage.Granularity(c.DefaultQuery("granularity", string(storage.Hour)))
	if !q.Granularity.Valid() {
		return q, fmt.Errorf("%w: granularity must be %q or %q", errBadRequest, storage.Hour, storage.Day)
	}

	q.To = h.now().UTC().Truncate(time.Hour).Add(time.Hour)
	if raw := c.Query("to"); raw != "" {
		if q.To, err = parseTime(raw, "to"); err != nil {
			return q, err
		}
	}

	q.From = q.To.Add(-defaultRange)
	if raw := c.Query("from"); raw != "" {
		if q.From, err = parseTime(raw, "from"); err != nil {
			return q, err
		}
	}

	if !q.From.Before(q.To) {
		return q, fmt.Errorf("%w: from must be before to", errBadRequest)
	}

	limit := maxHourRange
	if q.Granularity == storage.Day {
		limit = maxDayRange
	}
	if q.To.Sub(q.From) > limit {
		return q, fmt.Errorf("%w: period is too long for granularity %q (max %s)", errBadRequest, q.Granularity, limit)
	}

	return q, nil
}

func parseID(raw, name string) (uint64, error) {
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("%w: %s must be a positive integer", errBadRequest, name)
	}
	return id, nil
}

func parseTime(raw, name string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must be RFC3339, e.g. 2026-11-16T00:00:00Z", errBadRequest, name)
	}
	return t.UTC(), nil
}
