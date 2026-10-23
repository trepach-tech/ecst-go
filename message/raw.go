package message

import (
	"encoding/json"
	"fmt"
)

// Raw - конверт с неразобранным payload.
//
// Нужен там, где тип payload еще неизвестен: в outbox-таблице и на транспорте
// лежат события разных сущностей, и разбирать их можно только зная топик
// или EntityType. Остальные поля конверта при этом доступны как обычно.
//
// Это псевдоним, поэтому Raw отдает и [Decode] с json.RawMessage
type Raw = Message[json.RawMessage]

// ToRaw прячет payload в JSON, оставляя конверт нетронутым.
//
// Так типизированное событие кладут в очередь или таблицу, которая
// про его тип ничего не знает
func ToRaw[T any](m Message[T]) (Raw, error) {
	const op = "envelope.ToRaw"

	raw := Raw{
		IdempotenceKey: m.IdempotenceKey,
		Key:            m.Key,
		EntityType:     m.EntityType,
		Version:        m.Version,
		Op:             m.Op,
		Timestamp:      m.Timestamp,
		TraceID:        m.TraceID,
		Source:         m.Source,
	}

	if m.Payload != nil {
		payload, err := json.Marshal(m.Payload)
		if err != nil {
			return Raw{}, fmt.Errorf("%s: %w", op, err)
		}

		raw.Payload = (*json.RawMessage)(&payload)
	}

	if err := raw.Validate(); err != nil {
		return Raw{}, fmt.Errorf("%s: %w", op, err)
	}

	return raw, nil
}

// FromRaw разбирает payload в T, оставляя конверт нетронутым.
//
// Ошибка здесь означает, что payload не той схемы, которую ждет получатель:
// повторять такое бессмысленно
func FromRaw[T any](raw Raw) (Message[T], error) {
	const op = "envelope.FromRaw"

	m := Message[T]{
		IdempotenceKey: raw.IdempotenceKey,
		Key:            raw.Key,
		EntityType:     raw.EntityType,
		Version:        raw.Version,
		Op:             raw.Op,
		Timestamp:      raw.Timestamp,
		TraceID:        raw.TraceID,
		Source:         raw.Source,
	}

	if raw.Payload != nil {
		var payload T
		if err := json.Unmarshal(*raw.Payload, &payload); err != nil {
			return Message[T]{}, fmt.Errorf("%s: %w", op, err)
		}

		m.Payload = &payload
	}

	if err := m.Validate(); err != nil {
		return Message[T]{}, fmt.Errorf("%s: %w", op, err)
	}

	return m, nil
}
