package outbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/message"
)

const testBroker = "localhost:9092"

// fakeStore - [OutboxStore], который считает вызовы и умеет падать
type fakeStore struct {
	mu sync.Mutex

	msgs []OutboxMessage

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

func (f *fakeStore) Fetch(_ context.Context, limit int) ([]OutboxMessage, error) {
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
func testWorker(t *testing.T, store *fakeStore) *Worker {
	t.Helper()

	cfg := DefaultConfig(store, testBroker)
	cfg.StoreMaxAttempts = 3
	cfg.Backoff.Min = 1 // не спим в тестах
	cfg.Backoff.Max = 1

	w, err := NewWorker(quietLog(), cfg)
	if err != nil {
		t.Fatalf("failed to create outbox worker: %s", err)
	}
	return w
}

func testMessage(t *testing.T, version int64) message.Message[order] {
	t.Helper()

	return message.New("id", "order-1", "order", version, message.OpCreate, &order{Sum: 100}).
		WithSource("orders-service", "v1")
}

type order struct {
	Sum int `json:"sum"`
}

// errPermanentStore - ошибка стора, которую бессмысленно повторять:
// так он сообщает про нарушение констрейнта или битую схему
var errPermanentStore = fmt.Errorf("constraint violation: %w", backoff.ErrPermanent)

// Ключ записи - EntityID: события одной сущности должны лечь в одну партицию,
// иначе их порядок потеряется
func TestRecordFromEnvelope(t *testing.T) {
	e := testMessage(t, 7).WithTraceID("trace-1")

	msg, err := NewOutboxMessage("orders", e)
	if err != nil {
		t.Fatalf("new outbox message: %v", err)
	}

	rec, err := msg.Record()
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

	if headers[message.HeaderEntityType] != "order" {
		t.Errorf("%s = %q, want %q", message.HeaderEntityType, headers[message.HeaderEntityType], "order")
	}

	if headers[message.HeaderTraceID] != "trace-1" {
		t.Errorf("%s = %q, want %q", message.HeaderTraceID, headers[message.HeaderTraceID], "trace-1")
	}

	// Значение записи - это сам конверт
	out, err := message.Decode[order](rec.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.EntityVersion != 7 || out.Payload.Sum != 100 {
		t.Fatalf("envelope = %+v, want version 7 and sum 100", out)
	}
}

func TestNewOutboxMessageErrors(t *testing.T) {
	valid := testMessage(t, 1)

	tests := map[string]struct {
		topic string
		e     message.Message[order]
	}{
		"no topic":         {topic: "", e: valid},
		"broken envelope":  {topic: "orders", e: message.Message[order]{}},
		"no entity id":     {topic: "orders", e: message.New("id", "order", "", 1, message.OpCreate, &order{})},
		"payload required": {topic: "orders", e: message.New[order]("id", "order", "order-1", 1, message.OpCreate, nil)},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewOutboxMessage(tt.topic, tt.e); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// Строка без топика в запись не собирается: публиковать ее некуда
func TestRecordWithoutTopic(t *testing.T) {
	msg, err := NewOutboxMessage("orders", testMessage(t, 1))
	if err != nil {
		t.Fatalf("new outbox message: %v", err)
	}

	msg.Topic = ""

	if _, err := msg.Record(); err == nil {
		t.Fatal("want error")
	}
}

// Битую строку нельзя ни отправить, ни отметить отправленной:
// она уходит в MarkFailed с причиной
func TestMarkFailed(t *testing.T) {
	store := &fakeStore{}
	w := testWorker(t, store)

	cause := errors.New("broken envelope")
	w.MarkFailed(context.Background(), "row-1", cause)

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

	w.MarkFailed(context.Background(), "row-1", errors.New("cause"))

	if store.markFailedCalls != w.storeMaxAttempts {
		t.Fatalf("MarkFailed calls = %d, want %d", store.markFailedCalls, w.storeMaxAttempts)
	}
}

// Ретраи прекращаются на ErrPermanent: повторять нарушение констрейнта
// или битую схему бессмысленно
func TestStoreRetryStopsOnPermanent(t *testing.T) {
	store := &fakeStore{markFailedErr: errPermanentStore}
	w := testWorker(t, store)

	w.MarkFailed(context.Background(), "row-1", errors.New("cause"))

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

	if store.fetchCalls != w.storeMaxAttempts {
		t.Fatalf("Fetch calls = %d, want %d", store.fetchCalls, w.storeMaxAttempts)
	}
}

// Ни одна строка не собралась в запись: публиковать нечего, все уехали
// в MarkFailed. До продюсера (в тесте его нет) дело не доходит
func TestProcessBatchAllBroken(t *testing.T) {
	store := &fakeStore{msgs: []OutboxMessage{{ID: "row-1"}, {ID: "row-2"}}}
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
