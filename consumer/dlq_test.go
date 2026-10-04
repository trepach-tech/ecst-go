package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/giicoo/ecst-go/backoff"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeDLQ считает отправки и умеет падать.
//
// В тестах пула в него пишут сразу несколько воркеров, поэтому под мьютексом
type fakeDLQ struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeDLQ) Send(context.Context, *kgo.Record, error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++

	return f.err
}

func (f *fakeDLQ) load() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// testProcessor собирает обработчик записей без kgo-клиента: он ему не нужен
func testProcessor(t *testing.T, dlq DLQ, handler Handler) *processor {
	t.Helper()

	cfg := DefaultConfig("localhost:9092")
	cfg.Group = "g"
	cfg.Topics = []string{"t"}
	cfg.DLQ = dlq
	cfg.Backoff = backoff.Config{Min: time.Millisecond, Max: time.Millisecond, Factor: 1, Jitter: 0}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	return newProcessor(cfg, handler, slog.Default())
}

func record() *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: 1, Offset: 42, Value: []byte("v")}
}

func TestHandleRetriesThenDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	calls := 0

	p := testProcessor(t, dlq, func(context.Context, *kgo.Record) error {
		calls++
		return errors.New("boom")
	})

	if err := p.process(context.Background(), record()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if calls != p.cfg.HandlerMaxAttempts {
		t.Fatalf("handler calls = %d, want %d", calls, p.cfg.HandlerMaxAttempts)
	}
	if dlq.load() != 1 {
		t.Fatalf("dlq calls = %d, want 1", dlq.load())
	}
}

func TestHandlePermanentGoesStraightToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	calls := 0

	p := testProcessor(t, dlq, func(context.Context, *kgo.Record) error {
		calls++
		return fmt.Errorf("parse: %w", ErrPermanent)
	})

	if err := p.process(context.Background(), record()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
	if dlq.load() != 1 {
		t.Fatalf("dlq calls = %d, want 1", dlq.load())
	}
}

func TestHandleDLQFailureStopsConsumer(t *testing.T) {
	dlq := &fakeDLQ{err: errors.New("broker down")}
	cause := errors.New("boom")

	p := testProcessor(t, dlq, func(context.Context, *kgo.Record) error { return cause })

	err := p.process(context.Background(), record())
	if err == nil {
		t.Fatal("handle: want error")
	}
	if dlq.load() != p.cfg.DLQMaxAttempts {
		t.Fatalf("dlq calls = %d, want %d", dlq.load(), p.cfg.DLQMaxAttempts)
	}
	// Причина исходного сбоя не теряется - она нужна, чтоб понять, что чинить
	if !errors.Is(err, cause) {
		t.Fatalf("error must wrap handler cause: %v", err)
	}
}

// Без DLQ девать необработанную запись некуда, и она встала бы поперек
// своей партиции навсегда, поэтому конфиг без DLQ не проходит валидацию
func TestConfigRequiresDLQ(t *testing.T) {
	cfg := DefaultConfig("localhost:9092")
	cfg.Group = "g"
	cfg.Topics = []string{"t"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("want error on nil DLQ")
	}
}

func TestKafkaDLQValidation(t *testing.T) {
	if _, err := NewKafkaDLQ(nil, "dlq"); err == nil {
		t.Fatal("want error on nil producer")
	}
}
