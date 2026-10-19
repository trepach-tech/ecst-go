package inbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/trepach-tech/ecst-go/internal/consumer"
	"github.com/trepach-tech/ecst-go/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	// Базовая конфигурация одного consumer'а [InboxWorker].
	//
	// Log, Topic, ProcessorConfig.Hanler и ProcessorConfig.DeadLetterSender
	// определяются самим [InboxWorker] при создании consumer'а.
	ConsumerConfigTemplate consumer.Config

	// Общая конфигурация DLQ producer'а.
	//
	// Для каждого consumer'а создаётся отдельный [consumer.DLQProducer]
	// на основе этой конфигурации.
	DLQProducer producer.Config

	// Возвращает имя DLQ-топика для указанного топика.
	//
	// По умолчанию к имени топика добавляется суффикс ".dlq".
	CustomDLQTopic func(topic string) string

	// Максимальное время выполнения одного вызова хендлера.
	//
	// Параметр обязателен: зависший хендлер блокирует свою партицию
	// и задерживает ребаланс, пока воркеры не завершат обработку батча.
	HandlerTimeout time.Duration

	// Хендлеры по топикам. Регистрируются через [InboxConfig.Register]
	TopicHandlers map[string]consumer.Handler
}

func DefaultConfig(group string, brokers ...string) *Config {
	cfg := consumer.DefaultConfig(brokers...)
	cfg.Group = group

	p := producer.DefaultConfig(brokers...)
	p.ClientID = group + "-dlq"

	return &Config{
		ConsumerConfigTemplate: cfg,
		DLQProducer:            p,
		HandlerTimeout:         30 * time.Second,
	}
}

// DLQTopic - имя DLQ-топика для топика t
func (c *Config) DLQTopic(t string) string {
	if c.CustomDLQTopic != nil {
		return c.CustomDLQTopic(t)
	}

	return t + ".dlq"
}

// Register привязывает хендлеры к своим топикам.
// Повторная регистрация к топику перетрет предыдущий хендлер.
//
// Используйте [RegisterTypedHandler] для своих событий и [RegisterRawHandler] для чужих,
// чтобы  зарегистрировать хендлер.
func (c *Config) Register(handlers ...HandlerToRegister) {
	if c.TopicHandlers == nil {
		c.TopicHandlers = make(map[string]consumer.Handler)
	}

	for _, h := range handlers {
		c.TopicHandlers[h.Topic] = h.Handler
	}
}

// withTimeout ограничивает время одного вызова хендлера.
//
// Хендлер обязан уважать ctx: сам по себе таймаут его не прервет,
// но по возврату ошибки запись пойдет на ретрай и потом в DLQ,
// а партиция не встанет молча
func (c *Config) WithTimeout(h consumer.Handler) consumer.Handler {
	return func(ctx context.Context, r *kgo.Record) error {
		ctx, cancel := context.WithTimeout(ctx, c.HandlerTimeout)
		defer cancel()

		return h(ctx, r)
	}
}

func (c *Config) Validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("inbox: "+msg)) }

	// Конфигурация консьюмера здесь не валидируется, поскольку у каждого консьюмера своя конфигурация.

	if err := c.DLQProducer.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("inbox: %w", err))
	}

	if c.HandlerTimeout <= 0 {
		add("HandlerTimeout must be > 0")
	}
	if len(c.TopicHandlers) == 0 {
		add("at least one handler must be registered")
	}
	for topic, h := range c.TopicHandlers {
		if topic == "" {
			add("topic must not be empty")
		}
		if h == nil {
			add("handler for topic " + topic + " is nil")
		}
	}

	return errors.Join(errs...)
}
