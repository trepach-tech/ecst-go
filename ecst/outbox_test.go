package ecst

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/envelope"
)

// fakeStore - [OutboxStore], который считает вызовы и умеет падать
type fakeStore struct {
	mu sync.Mutex

	msgs []OutboxRecord

	fetchErr      error
	markSentErr   error
	markFailedErr error

	fetchCalls      int
	markSentCalls   int
	markFailedCalls int

	sentIDs    []string
	failedIDs  []string
	failCauses []error
}

func (f *fakeStore) Fetch(_ context.Context, limit int) ([]OutboxRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.fetchCalls++

	if f.fetchErr != nil {
		return nil, f.fetchErr
	}

	if limit > len(f.msgs) {
		limit = len(f.msgs)
	}

	return f.msgs[:limit], nil
}

func (f *fakeStore) MarkSent(_ context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.markSentCalls++

	if f.markSentErr != nil {
		return f.markSentErr
	}

	f.sentIDs = append(f.sentIDs, ids...)

	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, id string, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.markFailedCalls++

	if f.markFailedErr != nil {
		return f.markFailedErr
	}

	f.failedIDs = append(f.failedIDs, id)
	f.failCauses = append(f.failCauses, cause)

	return nil
}

// quietLog - логгер в никуда: тесты проверяют поведение, а не вывод
func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testWorker - воркер без продюсера: годится для всего, что не публикует
func testWorker(t *testing.T, store *fakeStore) *outboxWorker {
	t.Helper()

	cfg := DefaultOutboxConfig(store, testBroker)
	cfg.StoreMaxAttempts = 3
	cfg.Backoff.Min = 1 // не спим в тестах
	cfg.Backoff.Max = 1

	return newOutboxWorker(cfg, nil, quietLog())
}

func testEnvelope(t *testing.T, version int64) envelope.Envelope[order] {
	t.Helper()

	return envelope.New("order", "order-1", version, envelope.OpCreate, &order{Sum: 100}).
		WithSource("orders-service", "v1")
}

type order struct {
	Sum int `json:"sum"`
}

// errPermanentStore - ошибка стора, которую бессмысленно повторять:
// так он сообщает про нарушение констрейнта или битую схему
var errPermanentStore = fmt.Errorf("constraint violation: %w", consumer.ErrPermanent)

// Ключ записи - EntityID: события одной сущности должны лечь в одну партицию,
// иначе их порядок потеряется
func TestRecordFromEnvelope(t *testing.T) {
	e := testEnvelope(t, 7).WithTraceID("trace-1")

	msg, err := NewOutboxRecordFromEnvelope("row-1", "orders", e)
	if err != nil {
		t.Fatalf("new outbox message: %v", err)
	}

	rec, err := msg.record()
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	if rec.Topic != "orders" {
		t.Errorf("topic = %q, want %q", rec.Topic, "orders")
	}

	if string(rec.Key) != "order-1" {
		t.Errorf("key = %q, want %q", rec.Key, "order-1")
	}

	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}

	if headers[envelope.HeaderEntityType] != "order" {
		t.Errorf("%s = %q, want %q", envelope.HeaderEntityType, headers[envelope.HeaderEntityType], "order")
	}

	if headers[envelope.HeaderTraceID] != "trace-1" {
		t.Errorf("%s = %q, want %q", envelope.HeaderTraceID, headers[envelope.HeaderTraceID], "trace-1")
	}

	// Значение записи - это сам конверт
	out, err := envelope.Decode[order](rec.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.Version != 7 || out.Payload.Sum != 100 {
		t.Fatalf("envelope = %+v, want version 7 and sum 100", out)
	}
}

func TestNewOutboxMessageErrors(t *testing.T) {
	valid := testEnvelope(t, 1)

	tests := map[string]struct {
		id    string
		topic string
		e     envelope.Envelope[order]
	}{
		"no id":            {id: "", topic: "orders", e: valid},
		"no topic":         {id: "row-1", topic: "", e: valid},
		"broken envelope":  {id: "row-1", topic: "orders", e: envelope.Envelope[order]{}},
		"no entity id":     {id: "row-1", topic: "orders", e: envelope.New("order", "", 1, envelope.OpCreate, &order{})},
		"payload required": {id: "row-1", topic: "orders", e: envelope.New[order]("order", "order-1", 1, envelope.OpCreate, nil)},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewOutboxRecordFromEnvelope(tt.id, tt.topic, tt.e); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// Строка без топика в запись не собирается: публиковать ее некуда
func TestRecordWithoutTopic(t *testing.T) {
	msg, err := NewOutboxRecordFromEnvelope("row-1", "orders", testEnvelope(t, 1))
	if err != nil {
		t.Fatalf("new outbox message: %v", err)
	}

	msg.Topic = ""

	if _, err := msg.record(); err == nil {
		t.Fatal("want error")
	}
}

// Битую строку нельзя ни отправить, ни отметить отправленной:
// она уходит в MarkFailed с причиной
func TestMarkFailed(t *testing.T) {
	store := &fakeStore{}
	w := testWorker(t, store)

	cause := errors.New("broken envelope")
	w.markFailed(context.Background(), "row-1", cause)

	if store.markFailedCalls != 1 {
		t.Fatalf("MarkFailed calls = %d, want 1", store.markFailedCalls)
	}

	if len(store.failedIDs) != 1 || store.failedIDs[0] != "row-1" {
		t.Fatalf("failed ids = %v, want [row-1]", store.failedIDs)
	}

	if !errors.Is(store.failCauses[0], cause) {
		t.Fatalf("cause = %v, want %v", store.failCauses[0], cause)
	}
}

// Запрос к базе повторяется StoreMaxAttempts раз: база отваливается и возвращается
func TestStoreRetried(t *testing.T) {
	store := &fakeStore{markFailedErr: errors.New("db is down")}
	w := testWorker(t, store)

	w.markFailed(context.Background(), "row-1", errors.New("cause"))

	if store.markFailedCalls != w.cfg.StoreMaxAttempts {
		t.Fatalf("MarkFailed calls = %d, want %d", store.markFailedCalls, w.cfg.StoreMaxAttempts)
	}
}

// Ретраи прекращаются на ErrPermanent: повторять нарушение констрейнта
// или битую схему бессмысленно
func TestStoreRetryStopsOnPermanent(t *testing.T) {
	store := &fakeStore{markFailedErr: errPermanentStore}
	w := testWorker(t, store)

	w.markFailed(context.Background(), "row-1", errors.New("cause"))

	if store.markFailedCalls != 1 {
		t.Fatalf("MarkFailed calls = %d, want 1", store.markFailedCalls)
	}
}

// Пустая таблица - не ошибка и не повод дергать MarkSent
func TestProcessBatchEmpty(t *testing.T) {
	store := &fakeStore{}
	w := testWorker(t, store)

	sent, err := w.processBatch(context.Background())
	if err != nil {
		t.Fatalf("process batch: %v", err)
	}

	if sent != 0 {
		t.Fatalf("sent = %d, want 0", sent)
	}

	if store.markSentCalls != 0 {
		t.Fatalf("MarkSent calls = %d, want 0", store.markSentCalls)
	}
}

// Fetch не поднялся - батч не едет, но и воркер не падает:
// строки остались неотмеченными и приедут в следующем заходе
func TestProcessBatchFetchFails(t *testing.T) {
	store := &fakeStore{fetchErr: errors.New("db is down")}
	w := testWorker(t, store)

	if _, err := w.processBatch(context.Background()); err == nil {
		t.Fatal("want error")
	}

	if store.fetchCalls != w.cfg.StoreMaxAttempts {
		t.Fatalf("Fetch calls = %d, want %d", store.fetchCalls, w.cfg.StoreMaxAttempts)
	}
}

// Ни одна строка не собралась в запись: публиковать нечего, все уехали
// в MarkFailed. До продюсера (в тесте его нет) дело не доходит
func TestProcessBatchAllBroken(t *testing.T) {
	store := &fakeStore{msgs: []OutboxRecord{{ID: "row-1"}, {ID: "row-2"}}}
	w := testWorker(t, store)

	sent, err := w.processBatch(context.Background())
	if err != nil {
		t.Fatalf("process batch: %v", err)
	}

	if sent != 0 {
		t.Fatalf("sent = %d, want 0", sent)
	}

	if store.markFailedCalls != 2 {
		t.Fatalf("MarkFailed calls = %d, want 2", store.markFailedCalls)
	}

	if store.markSentCalls != 0 {
		t.Fatalf("MarkSent calls = %d, want 0", store.markSentCalls)
	}
}
