package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	"uuid"

	"github.com/trepach-tech/ecst-go/inbox"
	"github.com/trepach-tech/ecst-go/message"
	"github.com/trepach-tech/ecst-go/outbox"
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

		order := message.New(uuid.New().String(), "order", "order-1", version, message.OpUpdate, &Order{Sum: 100 * int(version)}).
			WithSource("orders-service", "v1").
			WithTraceID(fmt.Sprintf("trace-%d", version))

		add(store, fmt.Sprintf("order-row-%d", version), ordersTopic, order)

		// Каждое третье изменение пользователя - удаление: у него нет payload,
		// потому что состояния у удаленной сущности не бывает
		user := message.New(uuid.New().String(), "user", "user-1", version, message.OpUpdate, &User{
			Email: fmt.Sprintf("user-1+v%d@example.com", version),
		})
		if version%3 == 0 {
			user = message.New[User](uuid.New().String(), "user", "user-1", version, message.OpDelete, nil)
		}

		add(store, fmt.Sprintf("user-row-%d", version), usersTopic, user.WithSource("users-service", "v1"))
	}
}

// add кладет событие в таблицу. Дженерик-функция, а не метод memStore:
// тип payload у каждого события свой
func add[T any](store *memStore, id, topic string, m message.Message[T]) {
	// ID строки в таблице. В боевом коде его выдает БД
	msg, err := outbox.NewOutboxMessage(topic, m)
	if err != nil {
		slog.Error("build outbox message", "id", id, "error", err)

		return
	}

	store.Add(msg)
}

// handleOrder применяет событие заказа к своей копии данных.
//
// Обязан быть идемпотентным: одно и то же событие может приехать повторно
func handleOrder(ctx context.Context, m message.Message[Order]) error {
	attrs := []any{
		"key", m.Key,
		"version", m.Version,
		"op", m.Op,
		"sum", m.Payload.Sum,
		"trace_id", m.TraceID,
	}

	// Топика, партиции и офсета в конверте нет - их достают из ctx, когда нужны
	if r, ok := inbox.RecordFrom(ctx); ok {
		attrs = append(attrs, "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
	}

	slog.Info("order applied", attrs...)

	return nil
}

// handleUser применяет событие пользователя: у второго топика свой тип
// payload и свой размер пула консьюмеров
func handleUser(_ context.Context, m message.Message[User]) error {
	// Delete приезжает без payload: состояния у удаленной сущности нет
	if m.Op == message.OpDelete {
		slog.Info("user deleted", "version", m.Version)

		return nil
	}

	slog.Info("user applied",
		"version", m.Version,
		"email", m.Payload.Email,
	)

	return nil
}
