package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Заголовки сообщения kafka
const (
	HeaderEntityType = "entity_type"
	HeaderTraceID    = "trace_id"
)

// Op - что случилось с сущностью
type Op string

func (o Op) known() bool {
	switch o {
	case OpCreate, OpUpdate, OpDelete, OpRead:
		return true
	}

	return false
}

// hasPayload - обязан ли конверт с такой операцией нести payload.
// Delete только сообщает, что сущности больше нет: состояния у нее уже не бывает
// Нужен был проверки полноо удаления из сущности (отсутсвие payload означало tomstone Для кафки - удаление всех записей с этим ключом)
func (o Op) hasPayload() bool { return o != OpDelete }

const (
	OpCreate Op = "c" // сущность создана
	OpRead   Op = "r" // снимок текущего состояния: бэкфилл, первичная загрузка
	OpUpdate Op = "u" // состояние сущности изменилось
	OpDelete Op = "d" // сущность удалена, payload не нужен

)

// New собирает конверт с текущим временем.
//
// Source обязателен и выставляется отдельно - [Message.WithSource]:
// без него конверт не пройдет [Message.Validate]
func New[T any](idempotenceKey, key, entityType string, version int64, op Op, payload *T) Message[T] {
	return Message[T]{
		IdempotenceKey: idempotenceKey,
		Key:            key,
		EntityType:     entityType,
		Version:        version,
		Op:             op,
		Payload:        payload,
		Timestamp:      time.Now().UTC(),
	}
}

// Message - событие вместе со всем, что нужно его получателю:
// какая сущность, какой версии, что с ней случилось и кто об этом сообщил.
//
// В ECST payload - полное состояние сущности, а не дельта: получатель
// должен уметь собрать свою копию, ничего не доспрашивая
type Message[T any] struct {
	IdempotenceKey string    `json:"idempotence_key"`
	Key            string    `json:"key"`
	EntityType     string    `json:"entity_type"`
	Version        int64     `json:"version"`
	Op             Op        `json:"op"`
	Payload        *T        `json:"payload,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
	TraceID        string    `json:"trace_id,omitempty"`
	Source         Source    `json:"source"`
}

// Source - кто опубликовал событие и по какой версии схемы
type Source struct {
	Service   string `json:"service"`
	SchemaVer string `json:"schema_ver"`
}

// WithSource заполняет [Source]. Без него получатель не знает,
// кто отправил событие и по какой схеме читать его payload
func (m Message[T]) WithSource(service, schemaVer string) Message[T] {
	m.Source = Source{Service: service, SchemaVer: schemaVer}

	return m
}

// WithTraceID связывает событие с запросом, в котором оно родилось
func (m Message[T]) WithTraceID(traceID string) Message[T] {
	m.TraceID = traceID

	return m
}

// Validate возвращает все найденные проблемы разом.
//
// Событие без EntityType, EntityID, Version или Source получатель
// не сможет ни смаршрутизировать, ни прочитать, поэтому до брокера
// такое доезжать не должно
func (m Message[T]) Validate() error {
	const op = "Envelope.Validate"

	var errs []error
	add := func(msg string) { errs = append(errs, fmt.Errorf("%s: %s", op, msg)) }

	if m.EntityType == "" {
		add("EntityType is required")
	}

	// Key - ключ партиционирования: без него события одной сущности
	// разъедутся по партициям и потеряют порядок
	if m.Key == "" {
		add("Key is required")
	}

	// Version нужна получателю, чтоб отбрасывать уже примененные события
	if m.Version <= 0 {
		add("Version must be > 0")
	}

	switch {
	case m.Op == "":
		add("Op is required")
	case !m.Op.known():
		add("unknown Op: " + string(m.Op))
	case m.Op.hasPayload() && m.Payload == nil:
		add("Payload is required for Op " + string(m.Op))
	}

	// Без Source непонятно, кто отправил событие
	// и по какой схеме читать его payload
	if m.Source.Service == "" {
		add("Source.Service is required")
	}
	if m.Source.SchemaVer == "" {
		add("Source.SchemaVer is required")
	}

	if m.Timestamp.IsZero() {
		add("Timestamp is required")
	}

	return errors.Join(errs...)
}

// Headers дублирует маршрутные поля конверта: по ним запись фильтруют
// и трейсят, не разбирая payload
func (m Message[T]) Headers() map[string]string {
	headers := map[string]string{
		HeaderEntityType: m.EntityType,
	}
	if m.TraceID != "" {
		headers[HeaderTraceID] = m.TraceID
	}

	return headers
}

// Encode сериализует конверт в JSON, предварительно его проверив:
// битый конверт дешевле поймать здесь, чем в каждом получателе
func (m Message[T]) Encode() ([]byte, error) {
	const op = "Envelope.Encode"

	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	raw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return raw, nil
}

// Decode разбирает конверт из JSON и проверяет его
func Decode[T any](raw []byte) (Message[T], error) {
	const op = "envelope.Decode"

	var m Message[T]

	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("%s: %w", op, err)
	}

	// Отправитель может быть старее этого получателя или вообще чужим
	// сервисом: то, что приехало с транспорта, проверяют, а не принимают на веру
	if err := m.Validate(); err != nil {
		return m, fmt.Errorf("%s: %w", op, err)
	}

	return m, nil
}
