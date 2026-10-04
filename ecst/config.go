package ecst

import (
	"errors"
	"fmt"
	"time"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/producer"
)

// Config собирает ECST-сервис из двух независимых частей.
//
// Обе опциональны: nil - соответствующий воркер не поднимается.
// Сервис только с Outbox - производитель событий, только с Inbox - потребитель,
// оба сразу - типовой сервис, который и читает чужие события, и публикует свои
type Config struct {
	// Забирает записи из outbox-таблицы и публикует их в Kafka
	Outbox *OutboxConfig

	// Поднимает пул консьюмеров на каждый зарегистрированный топик
	Inbox *InboxConfig
}

type OutboxConfig struct {
	// Откуда читать и куда отмечать отправленное. Обязателен
	Store OutboxStore

	// Продюсер, которым публикуются события. Свой у каждой части: публикация
	// и DLQ требуют разных настроек надежности и не должны делить один буфер -
	// outbox пишет потоком и может занять его целиком, а DLQ-запись,
	// которой не хватило места, встает поперек своей партиции
	Producer producer.Config

	// Сколько записей забирать за один заход
	BatchSize int

	// Пауза между заходами, когда таблица пуста.
	// Если batch набрался целиком - следующий заход идет без паузы
	PollInterval time.Duration

	// Таймаут на один цикл "забрать - опубликовать - отметить",
	// вместе со всеми ретраями запросов к БД.
	// Без него зависший запрос к БД останавливает публикацию навсегда
	BatchTimeout time.Duration

	// Сколько раз пытаться выполнить один запрос к [OutboxStore].
	// 1 - без повторов.
	//
	// База отваливается и возвращается: без ретраев каждая такая пауза
	// стоила бы целого цикла публикации
	StoreMaxAttempts int

	// Задержка между попытками запроса к [OutboxStore].
	// Без нее все попытки выгорают за миллисекунды, пока база лежит
	Backoff backoff.Config
}

type InboxConfig struct {
	// Базовый конфиг консьюмера: Topics и DLQ выставляет сам Inbox,
	// остальное (Group, брокеры, таймауты) берется отсюда
	Consumer consumer.Config

	// Продюсер, которым пишутся DLQ-записи
	Producer producer.Config

	// Хендлеры по топикам. Регистрируются через [InboxConfig.Register]
	handlers map[string]topicHandler

	// Сколько ждать одного вызова хендлера. Обязателен: зависший хендлер
	// держит свою партицию, а заодно и ребаланс - он ждет, пока воркеры
	// доработают батч
	HandlerTimeout time.Duration

	// Сколько консьюмеров поднимать на топик, если не задано в Register.
	// Больше числа партиций топика смысла не имеет: лишние будут простаивать
	PoolSize int

	// Имя DLQ-топика для топика t. По умолчанию t + ".dlq"
	DLQTopic func(topic string) string
}

type topicHandler struct {
	handler  consumer.Handler
	poolSize int
}

// Register привязывает хендлеры к топикам.
//
// Регистрации собирают [Topic] для своих событий и [RawTopic] для чужих.
// Повторная регистрация топика перетирает предыдущую
func (c *InboxConfig) Register(regs ...Registration) {
	if c.handlers == nil {
		c.handlers = make(map[string]topicHandler)
	}

	for _, r := range regs {
		c.handlers[r.topic] = topicHandler{handler: r.handler, poolSize: r.poolSize}
	}
}

// DefaultOutboxConfig возвращает конфиг outbox-воркера с разумными значениями
func DefaultOutboxConfig(store OutboxStore, brokers ...string) *OutboxConfig {
	p := producer.DefaultConfig(brokers...)
	p.ClientID = "ecst-outbox"

	return &OutboxConfig{
		Store:            store,
		Producer:         p,
		BatchSize:        100,
		PollInterval:     time.Second,
		BatchTimeout:     30 * time.Second,
		StoreMaxAttempts: 3,
		Backoff:          backoff.Default(),
	}
}

// DefaultInboxConfig возвращает конфиг inbox-воркера.
// Хендлеры добавляются через [InboxConfig.Register]
func DefaultInboxConfig(group string, brokers ...string) *InboxConfig {
	cfg := consumer.DefaultConfig(brokers...)
	cfg.Group = group

	p := producer.DefaultConfig(brokers...)
	p.ClientID = "ecst-dlq"

	return &InboxConfig{
		Consumer:       cfg,
		Producer:       p,
		HandlerTimeout: 30 * time.Second,
		PoolSize:       1,
	}
}

func (c Config) validate() error {
	var errs []error

	if c.Outbox == nil && c.Inbox == nil {
		errs = append(errs, errors.New("ecst: at least one of Outbox/Inbox must be configured"))
	}
	if c.Outbox != nil {
		errs = append(errs, c.Outbox.validate())
	}
	if c.Inbox != nil {
		errs = append(errs, c.Inbox.validate())
	}

	return errors.Join(errs...)
}

func (c *OutboxConfig) validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("outbox: "+msg)) }

	if c.Store == nil {
		add("Store is required")
	}
	if c.BatchSize <= 0 {
		add("BatchSize must be > 0")
	}
	if c.PollInterval <= 0 {
		add("PollInterval must be > 0")
	}
	if c.BatchTimeout <= 0 {
		add("BatchTimeout must be > 0")
	}
	if c.StoreMaxAttempts <= 0 {
		add("StoreMaxAttempts must be > 0")
	}
	if err := c.Backoff.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("outbox: %w", err))
	}
	if err := c.Producer.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("outbox: %w", err))
	}

	return errors.Join(errs...)
}

func (c *InboxConfig) validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("inbox: "+msg)) }

	if len(c.handlers) == 0 {
		add("at least one handler must be registered")
	}
	if c.Consumer.Group == "" {
		add("Consumer.Group is required")
	}
	if c.PoolSize <= 0 {
		add("PoolSize must be > 0")
	}
	if c.HandlerTimeout <= 0 {
		add("HandlerTimeout must be > 0")
	}

	for topic, h := range c.handlers {
		if topic == "" {
			add("topic must not be empty")
		}
		if h.handler == nil {
			add("handler for topic " + topic + " is nil")
		}
		if h.poolSize < 0 {
			add("poolSize for topic " + topic + " must be >= 0")
		}
	}

	if err := c.Producer.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("inbox: %w", err))
	}

	return errors.Join(errs...)
}

// dlqTopic - имя DLQ-топика для топика t
func (c *InboxConfig) dlqTopic(t string) string {
	if c.DLQTopic != nil {
		return c.DLQTopic(t)
	}

	return t + ".dlq"
}

// size - сколько консьюмеров поднять на топик
func (c *InboxConfig) size(h topicHandler) int {
	if h.poolSize > 0 {
		return h.poolSize
	}

	return c.PoolSize
}
