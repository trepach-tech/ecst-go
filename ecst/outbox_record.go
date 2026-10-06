package ecst

import (
	"errors"
	"fmt"

	"github.com/trepach-tech/ecst-go/envelope"
)

// OutboxRecord — запись outbox-таблицы, содержащая
// сериализованный envelope и данные маршрутизации.
type OutboxRecord struct {
	ID string

	// Топик, в который уйдёт событие. В [envelope.Envelope] его нет:
	// это часть маршрутизации, а не часть события
	Topic string

	// Сериализованный Envelope. Тип payload здесь [json.RawMessage]
	RawEnvelope envelope.Raw
}

// NewOutboxRecordFromEnvelope собирает запись outbox-таблицы из типизированного конверта.
func NewOutboxRecordFromEnvelope[T any](id, topic string, e envelope.Envelope[T]) (OutboxRecord, error) {
	if id == "" {
		return OutboxRecord{}, errors.New("ecst: outbox message id is required")
	}
	if topic == "" {
		return OutboxRecord{}, errors.New("ecst: outbox message topic is required")
	}

	// ToRaw валидирует конверт сам: до таблицы битое событие не доедет
	raw, err := envelope.ToRaw(e)
	if err != nil {
		return OutboxRecord{}, fmt.Errorf("ecst: %w", err)
	}

	return OutboxRecord{
		ID:          id,
		Topic:       topic,
		RawEnvelope: raw,
	}, nil
}
