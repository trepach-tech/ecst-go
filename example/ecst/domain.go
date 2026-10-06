package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/giicoo/ecst-go/ecst"
	"github.com/giicoo/ecst-go/envelope"
)

// Сколько ждать между событиями, которые пишет бизнес-логика
const emitInterval = 2 * time.Second

// Order - состояние заказа, которое едет в событии.
//
// В ECST payload - это полное состояние сущности, а не дельта:
// получатель должен уметь собрать свою копию, ничего не доспрашивая
type Order struct {
	Sum int `json:"sum"`
}

// User - состояние пользователя. У каждого топика свой тип payload
type User struct {
	Email string `json:"email"`
}

// emitEvents изображает бизнес-логику: пишет события в outbox-таблицу,
// пока не отменят ctx.
//
// В боевом коде это делается той же транзакцией, что и изменение самой
// сущности - в этом весь смысл outbox: событие и состояние коммитятся атомарно
func emitEvents(ctx context.Context, store *memStore) {
	ticker := time.NewTicker(emitInterval)
	defer ticker.Stop()

	// Version растет с каждым изменением сущности: по ней получатель
	// отбрасывает события, которые уже применил
	for version := int64(1); ; version++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		order := envelope.New("order", "order-1", version, envelope.OpUpdate, &Order{Sum: 100 * int(version)}).
			WithSource("orders-service", "v1").
			WithTraceID(fmt.Sprintf("trace-%d", version))

		add(store, fmt.Sprintf("order-row-%d", version), ordersTopic, order)

		// Каждое третье изменение пользователя - удаление: у него нет payload,
		// потому что состояния у удаленной сущности не бывает
		user := envelope.New("user", "user-1", version, envelope.OpUpdate, &User{
			Email: fmt.Sprintf("user-1+v%d@example.com", version),
		})
		if version%3 == 0 {
			user = envelope.New[User]("user", "user-1", version, envelope.OpDelete, nil)
		}

		add(store, fmt.Sprintf("user-row-%d", version), usersTopic, user.WithSource("users-service", "v1"))
	}
}

// add кладет событие в таблицу. Дженерик-функция, а не метод memStore:
// тип payload у каждого события свой
func add[T any](store *memStore, id, topic string, e envelope.Envelope[T]) {
	// ID строки в таблице. В боевом коде его выдает БД
	msg, err := ecst.NewOutboxRecordFromEnvelope(id, topic, e)
	if err != nil {
		slog.Error("build outbox message", "id", id, "error", err)

		return
	}

	store.Add(msg)
}

// handleOrder применяет событие заказа к своей копии данных.
//
// Обязан быть идемпотентным: одно и то же событие может приехать повторно
func handleOrder(ctx context.Context, e envelope.Envelope[Order]) error {
	attrs := []any{
		"entity_id", e.EntityID,
		"version", e.Version,
		"op", e.Op,
		"sum", e.Payload.Sum,
		"trace_id", e.TraceID,
	}

	// Топика, партиции и офсета в конверте нет - их достают из ctx, когда нужны
	if r, ok := ecst.RecordFrom(ctx); ok {
		attrs = append(attrs, "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
	}

	slog.Info("order applied", attrs...)

	return nil
}

// handleUser применяет событие пользователя: у второго топика свой тип
// payload и свой размер пула консьюмеров
func handleUser(_ context.Context, e envelope.Envelope[User]) error {
	// Delete приезжает без payload: состояния у удаленной сущности нет
	if e.Op == envelope.OpDelete {
		slog.Info("user deleted", "entity_id", e.EntityID, "version", e.Version)

		return nil
	}

	slog.Info("user applied",
		"entity_id", e.EntityID,
		"version", e.Version,
		"email", e.Payload.Email,
	)

	return nil
}
