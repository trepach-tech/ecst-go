package inbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/message"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")

	ErrHandlerAlreadyRegistered = errors.New("handler is already registered")
	ErrIgnoredOp                = errors.New("op is ignored")
	ErrOpHasRegisteredHandler   = errors.New("op has registered handlers")
)

func newHandlerNotFoundError(op message.Op, schemaVer string) error {
	return fmt.Errorf(
		"%w: %w for op %q and schema version %q",
		backoff.ErrPermanent,
		ErrHandlerNotFound,
		op, schemaVer,
	)
}

func newHandlerAlreadyRegistered(op message.Op, schemaVer string) error {
	return fmt.Errorf(
		"%w: for op %q and schema version %q", ErrHandlerAlreadyRegistered, op, schemaVer)
}

func newIgnoredOpError(op message.Op) error {
	return fmt.Errorf("%w: for op %q", ErrIgnoredOp, op)
}

func newOpHasRegisteredHandlerError(op message.Op) error {
	return fmt.Errorf("%w: for op %q", ErrOpHasRegisteredHandler, op)
}

type HandlerKey struct {
	Op        message.Op
	SchemaVer string
}

var ignoredHandler RawHandler = func(ctx context.Context, raw message.Raw) error {
	return nil
}

func NewRegistry() *Registry {
	return &Registry{
		handlerCount: make(map[message.Op]int),
		handlers:     make(map[HandlerKey]RawHandler),
		ignored:      make(map[message.Op]struct{}),
	}
}

type Registry struct {
	mu           sync.RWMutex
	handlerCount map[message.Op]int
	handlers     map[HandlerKey]RawHandler
	ignored      map[message.Op]struct{}
}

func (r *Registry) Get(op message.Op, schemaVer string) (RawHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.ignored[op]; ok {
		return ignoredHandler, nil
	}

	key := HandlerKey{
		Op:        op,
		SchemaVer: schemaVer,
	}

	handler, ok := r.handlers[key]
	if !ok {
		return nil, newHandlerNotFoundError(key.Op, key.SchemaVer)
	}

	return handler, nil
}

func (r *Registry) Register[T any](op message.Op, schemaVer string, handler TypedHandler[T]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ignored := r.ignored[op]; ignored {
		return newIgnoredOpError(op)
	}

	key := HandlerKey{
		Op:        op,
		SchemaVer: schemaVer,
	}

	if _, exists := r.handlers[key]; exists {
		return newHandlerAlreadyRegistered(op, schemaVer)
	}

	registered := func(ctx context.Context, raw message.Raw) error {
		msg, err := message.FromRaw[T](raw)
		if err != nil {
			return fmt.Errorf("%w: %w", backoff.ErrPermanent, err)
		}

		return handler(ctx, msg)
	}

	r.handlerCount[op] += 1
	r.handlers[key] = registered

	return nil
}

func (r *Registry) RegisterIgnored(op message.Op) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.ignored[op]; exists {
		return newIgnoredOpError(op)
	}

	if r.handlerCount[op] > 0 {
		return newOpHasRegisteredHandlerError(op)
	}

	r.ignored[op] = struct{}{}
	return nil
}
