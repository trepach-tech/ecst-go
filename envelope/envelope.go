package envelope

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Заголовки сообщения kafka
const (
	HeaderEnvelopeType string = "envelope_type"
	HeaderTraceID      string = "trace_id"
)

type Op string

func (o Op) known() bool {
	switch o {
	case OpCreate, OpUpdate, OpDelete, OpRead:
		return true
	}

	return false
}

// hasPayload определяет, содержит ли операция payload.
func (o Op) hasPayload() bool {
	// Все операции кроме [OpDelete] имеют payload
	return o != OpDelete
}

const (
	OpCreate Op = "c"
	OpRead   Op = "r"
	OpUpdate Op = "u"
	OpDelete Op = "d"
)

// Envelope - представляет событие и его метаданные.
type Envelope[T any] struct {
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Version    int64     `json:"version"`
	Op         Op        `json:"op"`
	Payload    *T        `json:"event,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	TraceID    string    `json:"trace_id,omitempty"`
	Source     Source    `json:"source"`
}

// Source - кто создал событие и по какой версии схемы
type Source struct {
	Service   string `json:"service"`
	SchemaVer string `json:"schema_ver"`
}

func New[T any](
	entityType string,
	entityID string,
	version int64,
	op Op,
	payload *T,
) Envelope[T] {
	return Envelope[T]{
		EntityType: entityType,
		EntityID:   entityID,
		Version:    version,
		Op:         op,
		Payload:    payload,
		Timestamp:  time.Now().UTC(),
	}
}

// WithSource заполняет [Source]. Без него потребитель не сможет определить,
// кто отправил событие и как прочитать его payload.
func (e Envelope[T]) WithSource(service, schemaVer string) Envelope[T] {
	e.Source = Source{Service: service, SchemaVer: schemaVer}

	return e
}

// WithTraceID links the event to the request it was born in
func (e Envelope[T]) WithTraceID(traceID string) Envelope[T] {
	e.TraceID = traceID

	return e
}

// Validate reports every problem at once.
//
// An event without EntityType, EntityID, Version or Source cannot be routed
// or read by a consumer, so it must never reach the broker
func (e Envelope[T]) Validate() error {
	const op = "Envelope.Validate"

	var errs []error
	add := func(msg string) { errs = append(errs, fmt.Errorf("%s: %s", op, msg)) }

	if e.EntityType == "" {
		add("EntityType is required")
	}

	// EntityID is the partition key: without it the events of one entity
	// scatter across partitions and lose their order
	if e.EntityID == "" {
		add("EntityID is required")
	}

	// Version lets the consumer drop events it has already applied
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

	// Without Source a consumer cannot tell who sent the event
	// and by which schema to read its payload
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

// Headers duplicates the routing fields of the envelope: with them a record
// is filtered and traced without decoding its payload
func (e Envelope[T]) Headers() map[string]string {
	headers := map[string]string{
		HeaderEnvelopeType: e.EntityType,
	}
	if e.TraceID != "" {
		headers[HeaderTraceID] = e.TraceID
	}

	return headers
}

// Json Encode. Validates first: a broken envelope is cheaper to catch here
// than in every consumer
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

// Json Decode
func Decode[T any](raw []byte) (Envelope[T], error) {
	const op = "envelope.Decode"

	var e Envelope[T]

	if err := json.Unmarshal(raw, &e); err != nil {
		return e, fmt.Errorf("%s: %w", op, err)
	}

	// The producer may be older than this consumer, or another service
	// entirely: what came off the wire is checked, not trusted
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("%s: %w", op, err)
	}

	return e, nil
}
