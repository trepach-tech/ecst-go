package ecst

import (
	"context"
	"testing"

	"github.com/trepach-tech/ecst-go/inbox"
	"github.com/trepach-tech/ecst-go/internal/testkit"
	"github.com/trepach-tech/ecst-go/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

func noopRaw(context.Context, *kgo.Record) error { return nil }

func validInboxConfig() *inbox.Config {
	cfg := inbox.DefaultConfig("test-group", testkit.TestBroker)
	cfg.Register(inbox.RegisterRawHandler("orders", noopRaw))

	return cfg
}

func validOutboxConfig() *outbox.Config {
	return outbox.DefaultConfig(&testkit.TestStore{}, testkit.TestBroker)
}

func TestValidateEmptyConfig(t *testing.T) {
	if err := (Config{}).validate(); err == nil {
		t.Fatal("want error")
	}
}

func TestValidateNotEmptyConfig(t *testing.T) {
	tests := map[string]Config{
		"outbox only": {Outbox: validOutboxConfig()},
		"inbox only":  {Inbox: validInboxConfig()},
		"both":        {Outbox: validOutboxConfig(), Inbox: validInboxConfig()},
	}

	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if err := cfg.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestInboxConfigValidate(t *testing.T) {
	tests := map[string]func(c *inbox.Config){
		"no handlers":          func(c *inbox.Config) { c.TopicHandlers = nil },
		"no group":             func(c *inbox.Config) { c.Consumer.Group = "" },
		"zero handler timeout": func(c *inbox.Config) { c.HandlerTimeout = 0 },
		"broken producer":      func(c *inbox.Config) { c.DLQProducer.Brokers = nil },
		"empty topic":          func(c *inbox.Config) { c.Register(inbox.RegisterRawHandler("", noopRaw)) },
		"nil handler":          func(c *inbox.Config) { c.Register(inbox.RegisterRawHandler("orders", nil)) },
	}

	for name, brk := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validInboxConfig()
			brk(cfg)

			if err := cfg.Validate(); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestOutboxConfigValidate(t *testing.T) {
	tests := map[string]func(c *outbox.Config){
		"no store":            func(c *outbox.Config) { c.Store = nil },
		"zero batch size":     func(c *outbox.Config) { c.BatchSize = 0 },
		"zero poll interval":  func(c *outbox.Config) { c.PollInterval = 0 },
		"zero batch timeout":  func(c *outbox.Config) { c.BatchTimeout = 0 },
		"zero store attempts": func(c *outbox.Config) { c.StoreMaxAttempts = 0 },
		"broken backoff":      func(c *outbox.Config) { c.Backoff.Min = 0 },
		"broken producer":     func(c *outbox.Config) { c.Producer.Brokers = nil },
	}

	for name, brk := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validOutboxConfig()
			brk(cfg)

			if err := cfg.Validate(); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestRegisterMany(t *testing.T) {
	cfg := inbox.DefaultConfig("test-group", testkit.TestBroker)
	cfg.Register(
		inbox.RegisterRawHandler("orders", noopRaw),
		inbox.RegisterRawHandler("users", noopRaw),
	)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if len(cfg.TopicHandlers) != 2 {
		t.Fatalf("handlers = %d, want 2", len(cfg.TopicHandlers))
	}
}

func TestDLQTopic(t *testing.T) {
	cfg := inbox.DefaultConfig("test-group", testkit.TestBroker)

	if got := cfg.DLQTopic("orders"); got != "orders.dlq" {
		t.Fatalf("dlq topic = %q, want %q", got, "orders.dlq")
	}

	cfg.DLQTopic = func(topic string) string { return "dlq." + topic }

	if got := cfg.DLQTopic("orders"); got != "dlq.orders" {
		t.Fatalf("dlq topic = %q, want %q", got, "dlq.orders")
	}
}
