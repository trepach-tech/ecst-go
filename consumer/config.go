package consumer

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	"github.com/giicoo/ecst-go/backoff"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"
)

// С какого офсета читать партицию, если у группы нет закоммиченного офсета
// (новая группа) или он уже вышел за retention
type StartOffset string

const (
	// С самой старой доступной записи: полный реплей, ничего не теряем
	StartOffsetEarliest StartOffset = "earliest"

	// Только новые записи
	StartOffsetLatest StartOffset = "latest"
)

func (s StartOffset) offset() (kgo.Offset, bool) {
	switch s {
	case StartOffsetEarliest:
		return kgo.NewOffset().AtStart(), true
	case StartOffsetLatest:
		return kgo.NewOffset().AtEnd(), true
	}
	return kgo.Offset{}, false
}

type Config struct {
	// Bootstrap servers
	Brokers  []string
	ClientID string

	// Consumer group, внутри которой делятся партиции
	Group string

	// Топики, которые читаем
	Topics []string

	// С какого офсета начинать, если группа еще ничего не коммитила
	StartOffset StartOffset

	//////////////
	// SECURITY //
	//////////////
	SASLUser string
	SASLPass string
	TLS      *tls.Config

	/////////////////
	// RELIABILITY //
	/////////////////

	// Время на подключение к брокеру
	DialTimeout time.Duration

	// Сколько ждать подтверждения коммита офсетов.
	// Коммит не отменяется вместе с ctx, поэтому таймаут обязателен
	CommitTimeout time.Duration

	// Сколько раз пытаться обработать одну запись хендлером.
	// 1 - без повторов
	HandlerMaxAttempts int

	// Куда отправлять записи, которые обработать не удалось. Обязательна:
	// без нее необработанную запись некуда деть, и она встанет поперек своей
	// партиции намертво - ребаланс и рестарт приведут нового владельца
	// к той же записи.
	//
	// Закрывает DLQ тот, кто ее создал: консьюмер этого не делает
	DLQ DLQ

	// Сколько раз пытаться отправить запись в DLQ
	DLQMaxAttempts int

	// Задержка между попытками обработки и между ретраями запросов к брокеру
	Backoff backoff.Config

	// Если за это время не пришел heartbeat - брокер считает консьюмера мертвым
	SessionTimeout time.Duration

	// Сколько группа ждет консьюмеров на ребалансе.
	// Ребаланс заблокирован на время обработки батча (BlockRebalanceOnPoll),
	// поэтому должен быть больше времени обработки одного батча
	RebalanceTimeout time.Duration

	////////////
	// MEMORY //
	////////////

	// Сколько брокер ждет наполнения фетча перед ответом
	FetchMaxWait time.Duration

	// Максимальный объем одного фетча
	FetchMaxBytes int32

	// Сколько записей максимум отдает один Poll.
	// Это же размер батча между коммитами
	MaxPollRecords int
}

// DefaultConfig возвращает конфиг с разумными значениями по умолчанию.
// Group, Topics и DLQ обязательны, их выставляют полями после вызова.
func DefaultConfig(brokers ...string) Config {
	return Config{
		Brokers:  brokers,
		ClientID: "ecst-consumer",

		StartOffset: StartOffsetEarliest,

		HandlerMaxAttempts: 3,
		DLQMaxAttempts:     3,
		Backoff:            backoff.Default(),

		DialTimeout:      5 * time.Second,
		CommitTimeout:    5 * time.Second,
		SessionTimeout:   45 * time.Second,
		RebalanceTimeout: 60 * time.Second,

		FetchMaxWait:   500 * time.Millisecond,
		FetchMaxBytes:  50 << 20, // 50 MiB
		MaxPollRecords: 500,
	}
}

// Validate проверяет конфиг и возвращает все найденные проблемы разом
func (c Config) Validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("config: "+msg)) }

	if len(c.Brokers) == 0 {
		add("brokers are required")
	}
	if c.Group == "" {
		add("Group is required")
	}
	if len(c.Topics) == 0 {
		add("Topics are required")
	}

	switch c.StartOffset {
	case StartOffsetEarliest, StartOffsetLatest:
	case "":
		add("StartOffset is required")
	default:
		add("unknown StartOffset: " + string(c.StartOffset))
	}

	if c.HandlerMaxAttempts <= 0 {
		add("HandlerMaxAttempts must be > 0")
	}
	if c.DLQ == nil {
		add("DLQ is required")
	}
	if c.DLQMaxAttempts <= 0 {
		add("DLQMaxAttempts must be > 0")
	}

	if err := c.Backoff.Validate(); err != nil {
		errs = append(errs, err)
	}

	if c.DialTimeout <= 0 {
		add("DialTimeout must be > 0")
	}
	if c.CommitTimeout <= 0 {
		add("CommitTimeout must be > 0")
	}
	if c.SessionTimeout <= 0 {
		add("SessionTimeout must be > 0")
	}
	if c.RebalanceTimeout <= 0 {
		add("RebalanceTimeout must be > 0")
	}

	if c.FetchMaxWait <= 0 {
		add("FetchMaxWait must be > 0")
	}
	if c.FetchMaxBytes <= 0 {
		add("FetchMaxBytes must be > 0")
	}
	if c.MaxPollRecords <= 0 {
		add("MaxPollRecords must be > 0")
	}

	if (c.SASLUser == "") != (c.SASLPass == "") {
		add("SASLUser and SASLPass must be set together")
	}

	return errors.Join(errs...)
}

// opts - перевод [Config] в опции клиента
func (c Config) opts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.DialTimeout(c.DialTimeout),

		kgo.ConsumerGroup(c.Group),
		kgo.ConsumeTopics(c.Topics...),

		// Экспоненциальная задержка с джиттером между ретраями запросов
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return c.Backoff.Delay(tries)
		}),

		// Коммитим сами, после обработки батча - иначе запись можно потерять
		kgo.DisableAutoCommit(),

		// Ребаланс не начнется, пока не вызван AllowRebalance.
		// Так партиции не уедут к другому консьюмеру, пока мы обрабатываем и коммитим батч
		kgo.BlockRebalanceOnPoll(),

		kgo.SessionTimeout(c.SessionTimeout),
		kgo.RebalanceTimeout(c.RebalanceTimeout),

		kgo.FetchMaxWait(c.FetchMaxWait),
		kgo.FetchMaxBytes(c.FetchMaxBytes),

		kgo.WithLogger(kslog.New(slog.Default())),
	}

	if offset, ok := c.StartOffset.offset(); ok {
		opts = append(opts, kgo.ConsumeResetOffset(offset))
	}

	if c.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(c.TLS))
	}
	if c.SASLUser != "" {
		opts = append(opts, kgo.SASL(
			scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha512Mechanism(),
		))
	}

	return opts
}
