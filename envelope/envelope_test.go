package envelope

import (
	"testing"
	"time"
)

type order struct {
	Total int `json:"total"`
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := New("order", "order-1", 7, OpUpdate, &order{Total: 100}).
		WithSource("orders-service", "v1").
		WithTraceID("trace-1")

	raw, err := in.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := Decode[order](raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.EntityType != in.EntityType || out.EntityID != in.EntityID {
		t.Fatalf("entity = %s/%s, want %s/%s", out.EntityType, out.EntityID, in.EntityType, in.EntityID)
	}
	if out.Version != in.Version || out.Op != in.Op {
		t.Fatalf("version/op = %d/%s, want %d/%s", out.Version, out.Op, in.Version, in.Op)
	}
	if out.Source != in.Source {
		t.Fatalf("source = %+v, want %+v", out.Source, in.Source)
	}
	if out.TraceID != in.TraceID {
		t.Fatalf("trace id = %q, want %q", out.TraceID, in.TraceID)
	}
	if !out.Timestamp.Equal(in.Timestamp) {
		t.Fatalf("timestamp = %s, want %s", out.Timestamp, in.Timestamp)
	}
	if out.Payload == nil || out.Payload.Total != in.Payload.Total {
		t.Fatalf("payload = %+v, want %+v", out.Payload, in.Payload)
	}
}

// Delete приезжает без payload: nil должен пережить round trip
func TestDeleteHasNoPayload(t *testing.T) {
	raw, err := New[order]("order", "order-1", 8, OpDelete, nil).
		WithSource("orders-service", "v1").
		Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := Decode[order](raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.Payload != nil {
		t.Fatalf("payload = %+v, want nil", out.Payload)
	}
}

func TestDecodeBroken(t *testing.T) {
	if _, err := Decode[order]([]byte("not json")); err == nil {
		t.Fatal("want error")
	}
}

func TestValidate(t *testing.T) {
	valid := func() Envelope[order] {
		return New("order", "order-1", 1, OpCreate, &order{Total: 1}).
			WithSource("orders-service", "v1")
	}

	if err := valid().Validate(); err != nil {
		t.Fatalf("valid envelope: %v", err)
	}

	tests := map[string]func(e *Envelope[order]){
		"no entity type": func(e *Envelope[order]) { e.EntityType = "" },
		"no entity id":   func(e *Envelope[order]) { e.EntityID = "" },
		"zero version":   func(e *Envelope[order]) { e.Version = 0 },
		"no op":          func(e *Envelope[order]) { e.Op = "" },
		"unknown op":     func(e *Envelope[order]) { e.Op = "x" },
		"no payload":     func(e *Envelope[order]) { e.Payload = nil },
		"no service":     func(e *Envelope[order]) { e.Source.Service = "" },
		"no schema ver":  func(e *Envelope[order]) { e.Source.SchemaVer = "" },
		"no timestamp":   func(e *Envelope[order]) { e.Timestamp = time.Time{} },
	}

	for name, break_ := range tests {
		t.Run(name, func(t *testing.T) {
			e := valid()
			break_(&e)

			if err := e.Validate(); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// Delete без payload валиден, остальные операции - нет
func TestValidateDeleteWithoutPayload(t *testing.T) {
	e := New[order]("order", "order-1", 1, OpDelete, nil).WithSource("orders-service", "v1")

	if err := e.Validate(); err != nil {
		t.Fatalf("delete without payload: %v", err)
	}
}

func TestHeaders(t *testing.T) {
	h := New("order", "order-1", 1, OpCreate, &order{}).
		WithSource("orders-service", "v1").
		WithTraceID("trace-1").
		Headers()

	if h[HeaderEntityType] != "order" {
		t.Fatalf("%s = %q, want %q", HeaderEntityType, h[HeaderEntityType], "order")
	}
	if h[HeaderTraceID] != "trace-1" {
		t.Fatalf("%s = %q, want %q", HeaderTraceID, h[HeaderTraceID], "trace-1")
	}

	// Без trace id заголовка быть не должно: пустое значение хуже отсутствующего
	if _, ok := New("order", "order-1", 1, OpCreate, &order{}).Headers()[HeaderTraceID]; ok {
		t.Fatalf("%s must be absent without trace id", HeaderTraceID)
	}
}
