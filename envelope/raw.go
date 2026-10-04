package envelope

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
type Raw = Envelope[json.RawMessage]

// ToRaw прячет payload в JSON, оставляя конверт нетронутым.
//
// Так типизированное событие кладут в очередь или таблицу, которая
// про его тип ничего не знает
func ToRaw[T any](e Envelope[T]) (Raw, error) {
	const op = "envelope.ToRaw"

	raw := Raw{
		EntityType: e.EntityType,
		EntityID:   e.EntityID,
		Version:    e.Version,
		Op:         e.Op,
		Timestamp:  e.Timestamp,
		TraceID:    e.TraceID,
		Source:     e.Source,
	}

	if e.Payload != nil {
		payload, err := json.Marshal(e.Payload)
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
func FromRaw[T any](raw Raw) (Envelope[T], error) {
	const op = "envelope.FromRaw"

	e := Envelope[T]{
		EntityType: raw.EntityType,
		EntityID:   raw.EntityID,
		Version:    raw.Version,
		Op:         raw.Op,
		Timestamp:  raw.Timestamp,
		TraceID:    raw.TraceID,
		Source:     raw.Source,
	}

	if raw.Payload != nil {
		var payload T
		if err := json.Unmarshal(*raw.Payload, &payload); err != nil {
			return Envelope[T]{}, fmt.Errorf("%s: %w", op, err)
		}

		e.Payload = &payload
	}

	if err := e.Validate(); err != nil {
		return Envelope[T]{}, fmt.Errorf("%s: %w", op, err)
	}

	return e, nil
}
