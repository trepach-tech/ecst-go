package ecst

import (
	"context"
	"errors"
	"testing"

	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// encode - конверт как он лежит в топике
func encode[T any](t *testing.T, e envelope.Envelope[T]) []byte {
	t.Helper()

	raw, err := e.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	return raw
}

// Хендлер получает разобранный конверт, транспорт наружу не торчит
func TestTopicDecodesEnvelope(t *testing.T) {
	var got envelope.Envelope[order]

	reg := Topic("orders", 1, func(_ context.Context, e envelope.Envelope[order]) error {
		got = e

		return nil
	})

	rec := &kgo.Record{Topic: "orders", Value: encode(t, testEnvelope(t, 7))}

	if err := reg.handler(context.Background(), rec); err != nil {
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

	reg := Topic("orders", 1, func(context.Context, envelope.Envelope[order]) error {
		return want
	})

	rec := &kgo.Record{Value: encode(t, testEnvelope(t, 1))}

	if err := reg.handler(context.Background(), rec); !errors.Is(err, want) {
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

	wrongSchema := envelope.New("order", "order-1", 1, envelope.OpCreate, &otherOrder{Sum: "сто"}).
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

	reg := Topic("orders", 1, func(context.Context, envelope.Envelope[order]) error {
		called = true

		return nil
	})

	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			called = false

			err := reg.handler(context.Background(), &kgo.Record{Value: value})
			if !errors.Is(err, consumer.ErrPermanent) {
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
	want := &kgo.Record{Topic: "orders", Partition: 3, Offset: 42, Value: encode(t, testEnvelope(t, 1))}

	var got *kgo.Record

	reg := Topic("orders", 1, func(ctx context.Context, _ envelope.Envelope[order]) error {
		r, ok := RecordFrom(ctx)
		if !ok {
			t.Error("record not found in ctx")
		}

		got = r

		return nil
	})

	if err := reg.handler(context.Background(), want); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

// Вне хендлера записи в контексте нет, и это не паника
func TestRecordFromEmptyContext(t *testing.T) {
	if r, ok := RecordFrom(context.Background()); ok || r != nil {
		t.Fatalf("record = %+v, ok = %v, want nil, false", r, ok)
	}
}

// RawTopic отдает запись как есть: для топиков, в которых лежит не конверт
func TestRawTopic(t *testing.T) {
	var got *kgo.Record

	reg := RawTopic("cdc.orders", 2, func(_ context.Context, r *kgo.Record) error {
		got = r

		return nil
	})

	want := &kgo.Record{Topic: "cdc.orders", Value: []byte("not an envelope at all")}

	if err := reg.handler(context.Background(), want); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}

	if reg.topic != "cdc.orders" || reg.poolSize != 2 {
		t.Fatalf("registration = %+v, want cdc.orders with pool 2", reg)
	}
}
