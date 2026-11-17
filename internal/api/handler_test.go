package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zeroqual/ads_stats_service/internal/storage"
)

type fakeReader struct {
	mu      sync.Mutex
	points  []storage.Point
	err     error
	pingErr error
	calls   int
	last    storage.StatsQuery
}

func (r *fakeReader) Stats(_ context.Context, q storage.StatsQuery) ([]storage.Point, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.last = q
	return r.points, r.err
}

func (r *fakeReader) Ping(context.Context) error { return r.pingErr }

func (r *fakeReader) snapshot() (int, storage.StatsQuery) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.last
}

type fakeCache struct {
	mu   sync.Mutex
	data map[string][]byte
	down bool
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string][]byte{}} }

func (c *fakeCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down {
		return nil, false, errors.New("redis is down")
	}
	v, ok := c.data[key]
	return v, ok, nil
}

func (c *fakeCache) Set(_ context.Context, key string, val []byte, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down {
		return errors.New("redis is down")
	}
	c.data[key] = val
	return nil
}

var fixedNow = time.Date(2026, 11, 16, 12, 30, 0, 0, time.UTC)

func newTestRouter(t *testing.T, reader StatsReader, cache Cache) http.Handler {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := NewService(reader, cache, ServiceConfig{CacheTTL: time.Minute}, log)
	h := NewHandler(svc, log)
	h.now = func() time.Time { return fixedNow }
	return NewRouter(h, log)
}

func get(t *testing.T, router http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	return rec
}

func decodeReport(t *testing.T, rec *httptest.ResponseRecorder) Report {
	t.Helper()
	var rep Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return rep
}

func TestBadRequests(t *testing.T) {
	cases := map[string]string{
		"advertiser not a number": "/v1/advertisers/abc/stats",
		"advertiser zero":         "/v1/advertisers/0/stats",
		"campaign not a number":   "/v1/advertisers/1/campaigns/x/stats",
		"unknown granularity":     "/v1/advertisers/1/stats?granularity=week",
		"bad from":                "/v1/advertisers/1/stats?from=yesterday",
		"bad to":                  "/v1/advertisers/1/stats?to=2026-11-16",
		"from equals to":          "/v1/advertisers/1/stats?from=2026-11-16T00:00:00Z&to=2026-11-16T00:00:00Z",
		"from after to":           "/v1/advertisers/1/stats?from=2026-11-17T00:00:00Z&to=2026-11-16T00:00:00Z",
		"hour range too long":     "/v1/advertisers/1/stats?from=2026-01-01T00:00:00Z&to=2026-03-01T00:00:00Z",
		"day range too long":      "/v1/advertisers/1/stats?granularity=day&from=2024-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
	}

	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			reader := &fakeReader{}
			rec := get(t, newTestRouter(t, reader, nil), url)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("want JSON with non-empty error, got %s", rec.Body)
			}
			if calls, _ := reader.snapshot(); calls != 0 {
				t.Fatalf("reader must not be called on bad request, called %d times", calls)
			}
		})
	}
}

func TestAdvertiserStatsTotals(t *testing.T) {
	h1 := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	reader := &fakeReader{points: []storage.Point{
		{TS: h1, Impressions: 1000, Clicks: 20, CostMicros: 150_000_000},
		{TS: h1.Add(time.Hour), Impressions: 3000, Clicks: 40, CostMicros: 450_000_000},
	}}
	router := newTestRouter(t, reader, nil)

	rec := get(t, router, "/v1/advertisers/7/stats?from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}

	rep := decodeReport(t, rec)
	want := Totals{Impressions: 4000, Clicks: 60, CostMicros: 600_000_000, CTR: 0.015}
	if rep.Totals != want {
		t.Fatalf("totals = %+v, want %+v", rep.Totals, want)
	}
	if len(rep.Points) != 2 || rep.Granularity != storage.Hour || rep.AdvertiserID != 7 {
		t.Fatalf("unexpected report: %+v", rep)
	}

	_, q := reader.snapshot()
	if q.AdvertiserID != 7 || q.CampaignID != 0 {
		t.Fatalf("query = %+v, want advertiser 7 and no campaign filter", q)
	}
}

func TestCampaignStatsPassesCampaign(t *testing.T) {
	reader := &fakeReader{}
	rec := get(t, newTestRouter(t, reader, nil), "/v1/advertisers/2/campaigns/102/stats?granularity=day")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}

	_, q := reader.snapshot()
	if q.AdvertiserID != 2 || q.CampaignID != 102 || q.Granularity != storage.Day {
		t.Fatalf("query = %+v", q)
	}
	if rep := decodeReport(t, rec); rep.CampaignID != 102 {
		t.Fatalf("campaign_id in response = %d, want 102", rep.CampaignID)
	}
}

func TestEmptyPeriodReturnsZerosNotNull(t *testing.T) {
	rec := get(t, newTestRouter(t, &fakeReader{}, nil), "/v1/advertisers/1/stats")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["points"]) != "[]" {
		t.Fatalf(`points = %s, want [] (not null)`, raw["points"])
	}

	rep := decodeReport(t, rec)
	if rep.Totals != (Totals{}) {
		t.Fatalf("totals = %+v, want zeros (ctr must be 0, not NaN)", rep.Totals)
	}
}

func TestDefaultPeriodIsLast24Hours(t *testing.T) {
	reader := &fakeReader{}
	get(t, newTestRouter(t, reader, nil), "/v1/advertisers/1/stats")

	_, q := reader.snapshot()
	wantTo := time.Date(2026, 11, 16, 13, 0, 0, 0, time.UTC)
	if !q.To.Equal(wantTo) || q.To.Sub(q.From) != 24*time.Hour {
		t.Fatalf("period = %s .. %s, want 24h ending at %s", q.From, q.To, wantTo)
	}
}

func TestCacheHit(t *testing.T) {
	reader := &fakeReader{points: []storage.Point{{Impressions: 10, Clicks: 1}}}
	router := newTestRouter(t, reader, newFakeCache())
	url := "/v1/advertisers/1/stats?from=2026-11-16T00:00:00Z&to=2026-11-17T00:00:00Z"

	first := get(t, router, url)
	second := get(t, router, url)

	if first.Header().Get("X-Cache") != "MISS" || second.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("X-Cache = %q, %q; want MISS, HIT", first.Header().Get("X-Cache"), second.Header().Get("X-Cache"))
	}
	if calls, _ := reader.snapshot(); calls != 1 {
		t.Fatalf("reader called %d times, want 1", calls)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("cached body differs:\n%s\n%s", first.Body, second.Body)
	}
}

func TestDifferentQueriesDoNotShareCache(t *testing.T) {
	reader := &fakeReader{}
	router := newTestRouter(t, reader, newFakeCache())

	get(t, router, "/v1/advertisers/1/stats")
	get(t, router, "/v1/advertisers/1/campaigns/101/stats")
	get(t, router, "/v1/advertisers/1/stats?granularity=day")

	if calls, _ := reader.snapshot(); calls != 3 {
		t.Fatalf("reader called %d times, want 3", calls)
	}
}

func TestCacheDownFailOpen(t *testing.T) {
	reader := &fakeReader{}
	cache := newFakeCache()
	cache.down = true

	rec := get(t, newTestRouter(t, reader, cache), "/v1/advertisers/1/stats")
	if rec.Code != http.StatusOK || rec.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("status = %d, X-Cache = %q; want 200 MISS", rec.Code, rec.Header().Get("X-Cache"))
	}
	if calls, _ := reader.snapshot(); calls != 1 {
		t.Fatalf("reader called %d times, want 1", calls)
	}
}

func TestReaderErrorIs500(t *testing.T) {
	reader := &fakeReader{err: errors.New("clickhouse is down")}
	rec := get(t, newTestRouter(t, reader, nil), "/v1/advertisers/1/stats")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if want := `{"error":"internal server error"}`; rec.Body.String() != want {
		t.Fatalf("body = %s, want %s (internal error must not leak)", rec.Body, want)
	}
}

func TestReadyz(t *testing.T) {
	ok := get(t, newTestRouter(t, &fakeReader{}, nil), "/readyz")
	if ok.Code != http.StatusOK {
		t.Fatalf("ready: status = %d, want 200", ok.Code)
	}

	down := get(t, newTestRouter(t, &fakeReader{pingErr: errors.New("down")}, nil), "/readyz")
	if down.Code != http.StatusServiceUnavailable {
		t.Fatalf("not ready: status = %d, want 503", down.Code)
	}
}

func TestHealthz(t *testing.T) {
	rec := get(t, newTestRouter(t, &fakeReader{pingErr: errors.New("down")}, nil), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 even when clickhouse is down", rec.Code)
	}
}
