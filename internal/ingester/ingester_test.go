package ingester

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/zeroqual/ads_stats_service/internal/event"
)

// ---------------------------------------------------------------------------
// Журнал операций
// ---------------------------------------------------------------------------

// journal — общий список того, что происходило, в порядке вызовов.
// В него пишут и fakeSource, и fakeSink. По нему тесты проверяют ПОРЯДОК:
// например, что "commit" никогда не идёт раньше успешного "insert".
type journal struct {
	mu  sync.Mutex // Run крутится в своей горутине, тест читает из своей
	ops []string
}

func (j *journal) add(op string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.ops = append(j.ops, op)
}

// snapshot возвращает копию, чтобы тест не читал слайс, в который
// ingester в этот момент дописывает.
func (j *journal) snapshot() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.ops)
}

func (j *journal) has(op string) bool {
	return slices.Contains(j.snapshot(), op)
}

// ---------------------------------------------------------------------------
// Фейк источника (вместо Kafka)
// ---------------------------------------------------------------------------

type fakeSource struct {
	j *journal

	mu      sync.Mutex
	batches [][]event.Event // что отдать из Consume, по одной пачке за вызов

	// Если задан — вызывается сразу после того, как отдана последняя пачка.
	// Так воспроизводится гонка «poll вернул данные, и тут же отменили ctx».
	cancelAfterLast context.CancelFunc
}

func (s *fakeSource) Consume(ctx context.Context) ([]event.Event, error) {
	s.mu.Lock()
	if len(s.batches) > 0 {
		batch := s.batches[0]
		s.batches = s.batches[1:]
		last := len(s.batches) == 0
		s.mu.Unlock()

		s.j.add(fmt.Sprintf("consume:%d", len(batch)))
		if last && s.cancelAfterLast != nil {
			s.cancelAfterLast()
		}
		return batch, nil
	}
	s.mu.Unlock()

	// Данных больше нет — ведём себя как настоящий poll: ждём, пока
	// не истечёт ctx (таймаут poll'а или остановка).
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *fakeSource) Commit(context.Context) error {
	s.j.add("commit")
	return nil
}

func (s *fakeSource) AllowRebalance() {
	s.j.add("allow")
}

// ---------------------------------------------------------------------------
// Фейк хранилища (вместо ClickHouse)
// ---------------------------------------------------------------------------

type fakeSink struct {
	j *journal

	mu        sync.Mutex
	failTimes int           // сколько первых вставок должны упасть
	inserted  []event.Event // всё, что успешно «вставили»
}

func (s *fakeSink) Insert(_ context.Context, events []event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failTimes > 0 {
		s.failTimes--
		s.j.add("insert_fail")
		return errors.New("clickhouse is down")
	}

	// append копирует значения: ingester потом переиспользует свой буфер,
	// и если бы мы сохранили сам слайс, его содержимое бы перезаписалось.
	s.inserted = append(s.inserted, events...)
	s.j.add(fmt.Sprintf("insert:%d", len(events)))
	return nil
}

func (s *fakeSink) insertedEvents() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.inserted)
}

// ---------------------------------------------------------------------------
// Хелперы
// ---------------------------------------------------------------------------

func newEvents(n int) []event.Event {
	evs := make([]event.Event, n)
	for k := range evs {
		evs[k] = event.Event{EventID: uuid.New(), Type: event.Impression, CampaignID: 1}
	}
	return evs
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// start запускает Run в отдельной горутине. Возвращает канал, который
// закроется, когда Run вернётся. t.Cleanup гарантирует, что после теста
// горутина остановлена, даже если тест упал посередине.
func start(t *testing.T, ctx context.Context, ing *Ingester) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ing.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return done
}

// waitFor раз в 5 мс проверяет условие, пока оно не станет true.
// Если за 2 секунды не стало — тест падает. Это вместо time.Sleep:
// тест ждёт ровно столько, сколько нужно, и не «мигает».
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

// only оставляет в журнале только операции с нужными префиксами —
// чтобы сравнивать с ожидаемым списком без шума от "allow"/"consume".
func only(ops []string, prefixes ...string) []string {
	var out []string
	for _, op := range ops {
		for _, p := range prefixes {
			if strings.HasPrefix(op, p) {
				out = append(out, op)
				break
			}
		}
	}
	return out
}

// checkInvariants проверяет правила, которые должны выполняться всегда:
//  1. commit не бывает, пока есть прочитанные, но не вставленные события;
//  2. allow (разрешить ребалансировку) — тоже только когда всё вставлено.
func checkInvariants(t *testing.T, ops []string) {
	t.Helper()
	pending := 0 // прочитано, но ещё не вставлено
	for idx, op := range ops {
		var n int
		switch {
		case strings.HasPrefix(op, "consume:"):
			_, _ = fmt.Sscanf(op, "consume:%d", &n)
			pending += n
		case strings.HasPrefix(op, "insert:"):
			_, _ = fmt.Sscanf(op, "insert:%d", &n)
			pending -= n
		case op == "commit" && pending != 0:
			t.Errorf("op #%d: commit with %d events not inserted yet; ops=%v", idx, pending, ops)
		case op == "allow" && pending != 0:
			t.Errorf("op #%d: allow rebalance with %d events not inserted yet; ops=%v", idx, pending, ops)
		}
	}
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

func TestFlushBySize(t *testing.T) {
	j := &journal{}
	src := &fakeSource{j: j, batches: [][]event.Event{newEvents(2), newEvents(2)}}
	sink := &fakeSink{j: j}

	// Интервал огромный — флаш может случиться только по размеру.
	ing := New(src, sink, Config{BatchSize: 3, FlushInterval: time.Hour}, discardLogger())
	start(t, context.Background(), ing)

	waitFor(t, "commit after insert", func() bool { return j.has("commit") })

	got := only(j.snapshot(), "insert", "commit")
	want := []string{"insert:4", "commit"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkInvariants(t, j.snapshot())
}

func TestFlushByTime(t *testing.T) {
	j := &journal{}
	src := &fakeSource{j: j, batches: [][]event.Event{newEvents(1)}}
	sink := &fakeSink{j: j}

	// Размер огромный — флаш может случиться только по времени.
	ing := New(src, sink, Config{BatchSize: 1000, FlushInterval: 20 * time.Millisecond}, discardLogger())
	start(t, context.Background(), ing)

	waitFor(t, "insert by timer", func() bool { return j.has("insert:1") })
	checkInvariants(t, j.snapshot())
}

func TestCommitOnlyAfterSuccessfulInsert(t *testing.T) {
	j := &journal{}
	src := &fakeSource{j: j, batches: [][]event.Event{newEvents(2)}}
	sink := &fakeSink{j: j, failTimes: 2} // две вставки упадут, третья пройдёт

	ing := New(src, sink, Config{
		BatchSize:     2,
		FlushInterval: time.Hour,
		BackoffBase:   time.Millisecond, // чтобы ретраи не тянулись секундами
		BackoffMax:    2 * time.Millisecond,
	}, discardLogger())
	start(t, context.Background(), ing)

	waitFor(t, "commit", func() bool { return j.has("commit") })

	got := only(j.snapshot(), "insert", "commit")
	want := []string{"insert_fail", "insert_fail", "insert:2", "commit"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkInvariants(t, j.snapshot())
}

func TestNoCommitIfInsertNeverSucceeds(t *testing.T) {
	j := &journal{}
	src := &fakeSource{j: j, batches: [][]event.Event{newEvents(1)}}
	sink := &fakeSink{j: j, failTimes: 1 << 30} // ClickHouse лежит «навсегда»

	ing := New(src, sink, Config{
		BatchSize:       1,
		FlushInterval:   time.Hour,
		BackoffBase:     time.Millisecond,
		BackoffMax:      2 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond, // финальный flush не будет ждать долго
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := start(t, ctx, ing)

	waitFor(t, "insert attempts", func() bool { return j.has("insert_fail") })
	cancel() // Ctrl+C, пока ClickHouse лежит
	waitDone(t, done)

	if j.has("commit") {
		t.Fatalf("commit must not happen without successful insert; ops=%v", j.snapshot())
	}
	if n := len(sink.insertedEvents()); n != 0 {
		t.Fatalf("inserted %d events, want 0", n)
	}
}

// Тот самый баг: poll вернул данные, и сразу после этого отменили ctx.
// События должны быть вставлены финальным flush'ем, а не потеряны.
func TestEventsReturnedTogetherWithCancelAreNotLost(t *testing.T) {
	j := &journal{}
	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeSource{j: j, batches: [][]event.Event{newEvents(5)}, cancelAfterLast: cancel}
	sink := &fakeSink{j: j}

	// Ни размер, ни таймер не сработают — спасти события может только shutdown.
	ing := New(src, sink, Config{BatchSize: 1000, FlushInterval: time.Hour}, discardLogger())
	done := start(t, ctx, ing)
	waitDone(t, done)

	if n := len(sink.insertedEvents()); n != 5 {
		t.Fatalf("inserted %d events, want 5; ops=%v", n, j.snapshot())
	}
	got := only(j.snapshot(), "insert", "commit")
	if len(got) == 0 || got[0] != "insert:5" || !slices.Contains(got, "commit") {
		t.Fatalf("want insert:5 then commit, got %v", got)
	}
	checkInvariants(t, j.snapshot())
}

func TestEveryEventInsertedExactlyOnce(t *testing.T) {
	sizes := []int{3, 7, 1, 10, 4, 2, 8, 5, 6, 9}

	var batches [][]event.Event
	var all []event.Event
	for _, n := range sizes {
		b := newEvents(n)
		batches = append(batches, b)
		all = append(all, b...)
	}

	j := &journal{}
	src := &fakeSource{j: j, batches: batches}
	sink := &fakeSink{j: j}

	ing := New(src, sink, Config{BatchSize: 5, FlushInterval: 10 * time.Millisecond}, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := start(t, ctx, ing)

	waitFor(t, "all batches consumed", func() bool {
		return len(only(j.snapshot(), "consume")) == len(sizes)
	})
	cancel()
	waitDone(t, done)

	// Считаем, сколько раз вставлен каждый event_id.
	seen := make(map[uuid.UUID]int)
	for _, ev := range sink.insertedEvents() {
		seen[ev.EventID]++
	}
	for _, ev := range all {
		switch seen[ev.EventID] {
		case 1:
		case 0:
			t.Errorf("event %s lost", ev.EventID)
		default:
			t.Errorf("event %s inserted %d times", ev.EventID, seen[ev.EventID])
		}
	}
	if len(seen) != len(all) {
		t.Errorf("inserted %d unique events, want %d", len(seen), len(all))
	}
	checkInvariants(t, j.snapshot())
}
