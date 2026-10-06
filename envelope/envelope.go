package envelope

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
func (o Op) hasPayload() bool { return o != OpDelete }

const (
	OpCreate Op = "c" // сущность создана
	OpUpdate Op = "u" // состояние сущности изменилось
	OpDelete Op = "d" // сущность удалена, payload не нужен
	OpRead   Op = "r" // снимок текущего состояния: бэкфилл, первичная загрузка
)

// Envelope - событие вместе со всем, что нужно его получателю:
// какая сущность, какой версии, что с ней случилось и кто об этом сообщил.
//
// В ECST payload - полное состояние сущности, а не дельта: получатель
// должен уметь собрать свою копию, ничего не доспрашивая
type Envelope[T any] struct {
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Version    int64     `json:"version"`
	Op         Op        `json:"op"`
	Payload    *T        `json:"payload,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	TraceID    string    `json:"trace_id,omitempty"`
	Source     Source    `json:"source"`
}

// Source - кто опубликовал событие и по какой версии схемы
type Source struct {
	Service   string `json:"service"`
	SchemaVer string `json:"schema_ver"`
}

// New собирает конверт с текущим временем.
//
// Source обязателен и выставляется отдельно - [Envelope.WithSource]:
// без него конверт не пройдет [Envelope.Validate]
func New[T any](entityType, entityID string, version int64, op Op, payload *T) Envelope[T] {
	return Envelope[T]{
		EntityType: entityType,
		EntityID:   entityID,
		Version:    version,
		Op:         op,
		Payload:    payload,
		Timestamp:  time.Now().UTC(),
	}
}

// WithSource заполняет [Source]. Без него получатель не знает,
// кто отправил событие и по какой схеме читать его payload
func (e Envelope[T]) WithSource(service, schemaVer string) Envelope[T] {
	e.Source = Source{Service: service, SchemaVer: schemaVer}

	return e
}

// WithTraceID связывает событие с запросом, в котором оно родилось
func (e Envelope[T]) WithTraceID(traceID string) Envelope[T] {
	e.TraceID = traceID

	return e
}

// Validate возвращает все найденные проблемы разом.
//
// Событие без EntityType, EntityID, Version или Source получатель
// не сможет ни смаршрутизировать, ни прочитать, поэтому до брокера
// такое доезжать не должно
func (e Envelope[T]) Validate() error {
	const op = "Envelope.Validate"

	var errs []error
	add := func(msg string) { errs = append(errs, fmt.Errorf("%s: %s", op, msg)) }

	if e.EntityType == "" {
		add("EntityType is required")
	}

	// EntityID - ключ партиционирования: без него события одной сущности
	// разъедутся по партициям и потеряют порядок
	if e.EntityID == "" {
		add("EntityID is required")
	}

	// Version нужна получателю, чтоб отбрасывать уже примененные события
	if e.Version <= 0 {
		add("Version must be > 0")
	}

	switch {
	case e.Op == "":
		add("Op is required")
	case !e.Op.known():
		add("unknown Op: " + string(e.Op))
	case e.Op.hasPayload() && e.Payload == nil:
		add("Payload is required for Op " + string(e.Op))
	}

	// Без Source непонятно, кто отправил событие
	// и по какой схеме читать его payload
	if e.Source.Service == "" {
		add("Source.Service is required")
	}
	if e.Source.SchemaVer == "" {
		add("Source.SchemaVer is required")
	}

	if e.Timestamp.IsZero() {
		add("Timestamp is required")
	}

	return errors.Join(errs...)
}

// Headers дублирует маршрутные поля конверта: по ним запись фильтруют
// и трейсят, не разбирая payload
func (e Envelope[T]) Headers() map[string]string {
	headers := map[string]string{
		HeaderEntityType: e.EntityType,
	}
	if e.TraceID != "" {
		headers[HeaderTraceID] = e.TraceID
	}

	return headers
}

// Encode сериализует конверт в JSON, предварительно его проверив:
// битый конверт дешевле поймать здесь, чем в каждом получателе
func (e Envelope[T]) Encode() ([]byte, error) {
	const op = "Envelope.Encode"

	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return raw, nil
}

// Decode разбирает конверт из JSON и проверяет его
func Decode[T any](raw []byte) (Envelope[T], error) {
	const op = "envelope.Decode"

	var e Envelope[T]

	if err := json.Unmarshal(raw, &e); err != nil {
		return e, fmt.Errorf("%s: %w", op, err)
	}

	// Отправитель может быть старее этого получателя или вообще чужим
	// сервисом: то, что приехало с транспорта, проверяют, а не принимают на веру
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("%s: %w", op, err)
	}

	return e, nil
}
