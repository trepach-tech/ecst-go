package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/inbox"
	"github.com/trepach-tech/ecst-go/message"
)

// encode - конверт как он лежит в топике
func encode[T any](t *testing.T, e message.Message[T]) []byte {
	t.Helper()

	raw, err := e.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	return raw
}

// Хендлер получает разобранный конверт, транспорт наружу не торчит
func TestTopicDecodesEnvelope(t *testing.T) {
	var got message.Message[order]

	reg := inbox.RegisterTypedHandler("orders", func(_ context.Context, e message.Message[order]) error {
		got = e

		return nil
	})

	rec := &kgo.Record{Topic: "orders", Value: encode(t, testMessage(t, 7))}

	if err := reg.Handler(context.Background(), rec); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got.EntityID != "order-1" || got.Version != 7 {
		t.Fatalf("envelope = %+v, want order-1 v7", got)
	}

	if got.Payload == nil || got.Payload.Sum != 100 {
		t.Fatalf("payload = %+v, want sum 100", got.Payload)
	}
}

// Ошибка хендлера едет наружу как есть: по ней консьюмер решает,
// ретраить запись или отправить в DLQ
func TestTopicPassesHandlerError(t *testing.T) {
	want := errors.New("apply failed")

	reg := inbox.RegisterTypedHandler("orders", func(context.Context, message.Message[order]) error {
		return want
	})

	rec := &kgo.Record{Value: encode(t, testMessage(t, 1))}

	if err := reg.Handler(context.Background(), rec); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// Все, что не разбирается, - постоянная ошибка: ретраить битые байты
// бессмысленно, запись уезжает в DLQ сразу
func TestTopicBrokenValueIsPermanent(t *testing.T) {
	// Конверт с payload чужой схемы: строка там, где ждут число
	type otherOrder struct {
		Sum string `json:"sum"`
	}

	wrongSchema := message.New("id", "order", "order-1", 1, message.OpCreate, &otherOrder{Sum: "сто"}).
		WithSource("orders-service", "v1")

	// Конверт без Source: Encode такой не выпустит, но приехать он может -
	// продюсером мог быть кто угодно
	noSource := `{"entity_type":"order","entity_id":"order-1","version":1,"op":"c","payload":{"sum":1},"timestamp":"2026-01-01T00:00:00Z"}`

	tests := map[string][]byte{
		"not json":     []byte("not json"),
		"wrong schema": encode(t, wrongSchema),
		"no source":    []byte(noSource),
	}

	var called bool

	reg := inbox.RegisterTypedHandler("orders", func(context.Context, message.Message[order]) error {
		called = true

		return nil
	})

	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			called = false

			err := reg.Handler(context.Background(), &kgo.Record{Value: value})
			if !errors.Is(err, backoff.ErrPermanent) {
				t.Fatalf("err = %v, want to wrap consumer.ErrPermanent", err)
			}

			if called {
				t.Fatal("handler called with a broken envelope")
			}
		})
	}
}

// Топика, партиции и офсета в конверте нет: их достают из ctx
func TestRecordFrom(t *testing.T) {
	want := &kgo.Record{Topic: "orders", Partition: 3, Offset: 42, Value: encode(t, testMessage(t, 1))}

	var got *kgo.Record

	reg := inbox.RegisterTypedHandler("orders", func(ctx context.Context, _ message.Message[order]) error {
		r, ok := inbox.RecordFrom(ctx)
		if !ok {
			t.Error("record not found in ctx")
		}

		got = r

		return nil
	})

	if err := reg.Handler(context.Background(), want); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

// Вне хендлера записи в контексте нет, и это не паника
func TestRecordFromEmptyContext(t *testing.T) {
	if r, ok := inbox.RecordFrom(context.Background()); ok || r != nil {
		t.Fatalf("record = %+v, ok = %v, want nil, false", r, ok)
	}
}

// RawTopic отдает запись как есть: для топиков, в которых лежит не конверт
func TestRawTopic(t *testing.T) {
	var got *kgo.Record

	reg := inbox.RegisterRawHandler("cdc.orders", func(_ context.Context, r *kgo.Record) error {
		got = r

		return nil
	})

	want := &kgo.Record{Topic: "cdc.orders", Value: []byte("not an envelope at all")}

	if err := reg.Handler(context.Background(), want); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}

	if reg.Topic != "cdc.orders" {
		t.Fatalf("registration = %+v, want cdc.orders", reg)
	}
}
