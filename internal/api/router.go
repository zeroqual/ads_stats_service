package api

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsPath = "/metrics"

func NewRouter(h *Handler, log *slog.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery(), observe(log))

	r.GET("/healthz", h.Healthz)
	r.GET("/readyz", h.Readyz)
	r.GET(metricsPath, gin.WrapH(promhttp.Handler()))

	r.GET("/v1/advertisers/:advertiserID/stats", h.AdvertiserStats)
	r.GET("/v1/advertisers/:advertiserID/campaigns/:campaignID/stats", h.CampaignStats)

	return r
}

func observe(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == metricsPath {
			return
		}
		if route == "" {
			route = "unmatched"
		}

		status := c.Writer.Status()
		took := time.Since(start)

		requestsTotal.WithLabelValues(c.Request.Method, route, strconv.Itoa(status)).Inc()
		requestDuration.WithLabelValues(c.Request.Method, route).Observe(took.Seconds())

		level := slog.LevelInfo
		switch {
		case status >= 500:
			level = slog.LevelError
		case status >= 400:
			level = slog.LevelWarn
		}
		log.LogAttrs(c.Request.Context(), level, "http request",
			slog.String("method", c.Request.Method),
			slog.String("route", route),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", status),
			slog.Duration("took", took),
			slog.String("cache", c.Writer.Header().Get("X-Cache")),
		)
	}
}
