package ecst

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

const testBroker = "localhost:9092"

func noopRaw(context.Context, *kgo.Record) error { return nil }

func validOutbox() *OutboxConfig {
	return DefaultOutboxConfig(&fakeStore{}, testBroker)
}

func validInbox() *InboxConfig {
	cfg := DefaultInboxConfig("test-group", testBroker)
	cfg.Register(RawTopic("orders", 1, noopRaw))

	return cfg
}

// Обе половины опциональны, но хотя бы одна нужна:
// сервис без воркеров ничего не делает
func TestConfigValidateNoWorkers(t *testing.T) {
	if err := (Config{}).validate(); err == nil {
		t.Fatal("want error")
	}
}

func TestConfigValidateEitherWorker(t *testing.T) {
	tests := map[string]Config{
		"outbox only": {Outbox: validOutbox()},
		"inbox only":  {Inbox: validInbox()},
		"both":        {Outbox: validOutbox(), Inbox: validInbox()},
	}

	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if err := cfg.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestOutboxConfigValidate(t *testing.T) {
	tests := map[string]func(c *OutboxConfig){
		"no store":            func(c *OutboxConfig) { c.Store = nil },
		"zero batch size":     func(c *OutboxConfig) { c.BatchSize = 0 },
		"zero poll interval":  func(c *OutboxConfig) { c.PollInterval = 0 },
		"zero batch timeout":  func(c *OutboxConfig) { c.BatchTimeout = 0 },
		"zero store attempts": func(c *OutboxConfig) { c.StoreMaxAttempts = 0 },
		"broken backoff":      func(c *OutboxConfig) { c.Backoff.Min = 0 },
		"broken producer":     func(c *OutboxConfig) { c.Producer.Brokers = nil },
	}

	for name, brk := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validOutbox()
			brk(cfg)

			if err := cfg.validate(); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestInboxConfigValidate(t *testing.T) {
	tests := map[string]func(c *InboxConfig){
		"no handlers":          func(c *InboxConfig) { c.handlers = nil },
		"no group":             func(c *InboxConfig) { c.Consumer.Group = "" },
		"zero pool size":       func(c *InboxConfig) { c.PoolSize = 0 },
		"zero handler timeout": func(c *InboxConfig) { c.HandlerTimeout = 0 },
		"broken producer":      func(c *InboxConfig) { c.Producer.Brokers = nil },
		"empty topic":          func(c *InboxConfig) { c.Register(RawTopic("", 1, noopRaw)) },
		"nil handler":          func(c *InboxConfig) { c.Register(RawTopic("orders", 1, nil)) },
		"negative pool size":   func(c *InboxConfig) { c.Register(RawTopic("orders", -1, noopRaw)) },
	}

	for name, brk := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validInbox()
			brk(cfg)

			if err := cfg.validate(); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// Повторная регистрация топика перетирает предыдущую:
// два пула на один топик в одной группе - это просто два консьюмера,
// и какой из хендлеров получит запись, предсказать нельзя
func TestRegisterOverwrites(t *testing.T) {
	cfg := validInbox()
	cfg.Register(RawTopic("orders", 7, noopRaw))

	if len(cfg.handlers) != 1 {
		t.Fatalf("handlers = %d, want 1", len(cfg.handlers))
	}

	if got := cfg.handlers["orders"].poolSize; got != 7 {
		t.Fatalf("pool size = %d, want 7", got)
	}
}

func TestRegisterMany(t *testing.T) {
	cfg := DefaultInboxConfig("test-group", testBroker)
	cfg.Register(
		RawTopic("orders", 1, noopRaw),
		RawTopic("users", 2, noopRaw),
	)

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if len(cfg.handlers) != 2 {
		t.Fatalf("handlers = %d, want 2", len(cfg.handlers))
	}
}

// 0 в регистрации означает "взять InboxConfig.PoolSize"
func TestPoolSizeFallback(t *testing.T) {
	cfg := DefaultInboxConfig("test-group", testBroker)
	cfg.PoolSize = 4

	if got := cfg.size(topicHandler{poolSize: 0}); got != 4 {
		t.Fatalf("size = %d, want 4", got)
	}

	if got := cfg.size(topicHandler{poolSize: 2}); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
}

func TestDLQTopic(t *testing.T) {
	cfg := DefaultInboxConfig("test-group", testBroker)

	if got := cfg.dlqTopic("orders"); got != "orders.dlq" {
		t.Fatalf("dlq topic = %q, want %q", got, "orders.dlq")
	}

	cfg.DLQTopic = func(topic string) string { return "dlq." + topic }

	if got := cfg.dlqTopic("orders"); got != "dlq.orders" {
		t.Fatalf("dlq topic = %q, want %q", got, "dlq.orders")
	}
}
