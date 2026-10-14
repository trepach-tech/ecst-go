package outbox

import (
	"errors"
	"fmt"

	"github.com/trepach-tech/ecst-go/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OutboxMessage — запись outbox-таблицы, содержащая
// сериализованный [envelope.Envelope] и данные маршрутизации.
type OutboxMessage struct {
	ID string

	// Топик, в который уйдёт событие. В [envelope.Envelope] его нет:
	// это часть маршрутизации, а не часть события
	Topic string

	// Сериализованный [message.Message]. Тип payload здесь [json.RawMessage]
	RawMessage message.Raw
}

// NewOutboxMessage собирает запись outbox-таблицы из типизированного конверта.
func NewOutboxMessage[T any](topic string, e message.Message[T]) (OutboxMessage, error) {
	if e.ID == "" {
		return OutboxMessage{}, errors.New("ecst: outbox message id is required")
	}
	if topic == "" {
		return OutboxMessage{}, errors.New("ecst: outbox message topic is required")
	}

	// ToRaw валидирует конверт сам: до таблицы битое событие не доедет
	raw, err := message.ToRaw(e)
	if err != nil {
		return OutboxMessage{}, fmt.Errorf("ecst: %w", err)
	}

	return OutboxMessage{
		ID:         e.ID,
		Topic:      topic,
		RawMessage: raw,
	}, nil
}

// record собирает запись из конверта.
//
// Ключ - EntityID: так все события одной сущности ложатся в одну партицию
// и приезжают потребителю в порядке версий
func (m OutboxMessage) record() (*kgo.Record, error) {
	if m.Topic == "" {
		return nil, errors.New("outbox: message topic is required")
	}

	value, err := m.RawMessage.Encode()
	if err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}

	msgHeaders := m.RawMessage.Headers()
	headers := make([]kgo.RecordHeader, 0, len(msgHeaders))
	for k, v := range msgHeaders {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	return &kgo.Record{
		Topic:   m.Topic,
		Key:     []byte(m.RawMessage.EntityID),
		Value:   value,
		Headers: headers,
	}, nil
}
