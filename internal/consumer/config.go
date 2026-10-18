package consumer

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"

	"github.com/trepach-tech/ecst-go/backoff"
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
	Log *slog.Logger

	// Bootstrap серверы и описание consumer group
	Brokers     []string
	ClientID    string
	Group       string
	Topic       string
	StartOffset StartOffset

	// Security
	SASLUser string
	SASLPass string
	TLS      *tls.Config

	// Reliability and timeouts
	DialTimeout    time.Duration
	SessionTimeout time.Duration
	// Сколько группа ждет консьюмеров на ребалансе.
	// Ребаланс заблокирован на время обработки батча (BlockRebalanceOnPoll),
	// поэтому должен быть больше времени обработки одного батча
	RebalanceTimeout time.Duration
	// Настройки Backoff для клиента kafka:
	// экспоненциальная задержка с джиттером между ретраями запросов
	ClientBackoff backoff.Backoff

	// Fetch settings.
	FetchMaxWait  time.Duration
	FetchMaxBytes int32
	// Сколько записей максимум отдает один Poll.
	// Это же размер батча между коммитами
	MaxPollRecords int

	// Processing pipeline.
	PartitionWorkerConfig PartitionWorkerConfig
	ProcessorConfig       ProcessorConfig
}

// DefaultConfig возвращает конфиг с разумными значениями по умолчанию.
// [Config.Group], [Config.Topic], [ProcessorConfig.Handler] [ProcessorConfig.DeadLetterSender] обязательны, их выставляют полями после вызова.
func DefaultConfig(brokers ...string) Config {
	return Config{
		Brokers:     brokers,
		ClientID:    "ecst-consumer",
		StartOffset: StartOffsetEarliest,

		DialTimeout:      5 * time.Second,
		SessionTimeout:   45 * time.Second,
		RebalanceTimeout: 60 * time.Second,
		ClientBackoff:    backoff.Default(),

		FetchMaxWait:   500 * time.Millisecond,
		FetchMaxBytes:  50 << 20, // 50 MiB
		MaxPollRecords: 500,

		PartitionWorkerConfig: PartitionWorkerConfig{
			CommitTimeout: 5 * time.Second,
		},
		ProcessorConfig: ProcessorConfig{
			HandlerMaxAttempts: 3,
			DLQMaxAttempts:     3,
			Backoff:            backoff.Default(),
		},
	}
}

// Validate проверяет конфиг и возвращает все найденные проблемы разом
func (c Config) Validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("config: "+msg)) }

	if c.Log == nil {
		add("Logger is required")
	}

	if len(c.Brokers) == 0 {
		add("Brokers are required")
	}
	if c.ClientID == "" {
		add("ClientID id is required")
	}
	if c.Group == "" {
		add("Group is required")
	}
	if c.Topic == "" {
		add("Topic is required")
	}

	switch c.StartOffset {
	case StartOffsetEarliest, StartOffsetLatest:
	case "":
		add("StartOffset is required")
	default:
		add("unknown StartOffset: " + string(c.StartOffset))
	}

	if (c.SASLUser == "") != (c.SASLPass == "") {
		add("SASLUser and SASLPass must be set together")
	}

	if c.DialTimeout <= 0 {
		add("DialTimeout must be > 0")
	}
	if c.SessionTimeout <= 0 {
		add("SessionTimeout must be > 0")
	}
	if c.RebalanceTimeout <= 0 {
		add("RebalanceTimeout must be > 0")
	}
	if err := c.ClientBackoff.Validate(); err != nil {
		errs = append(errs, err)
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

	if c.PartitionWorkerConfig.CommitTimeout <= 0 {
		add("CommitTimeout must be > 0")
	}

	if c.ProcessorConfig.HandlerMaxAttempts <= 0 {
		add("HandlerMaxAttempts must be > 0")
	}
	if c.ProcessorConfig.Handler == nil {
		add("Handler is required")
	}
	if c.ProcessorConfig.DLQMaxAttempts <= 0 {
		add("DLQMaxAttempts must be > 0")
	}
	if c.ProcessorConfig.DeadLetterSender == nil {
		add("DeadLetterSender is required")
	}
	if err := c.ProcessorConfig.Backoff.Validate(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// opts - перевод [Config] в опции клиента
func (c Config) opts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.ConsumerGroup(c.Group),
		kgo.ConsumeTopics(c.Topic),

		kgo.DialTimeout(c.DialTimeout),
		kgo.SessionTimeout(c.SessionTimeout),
		kgo.RebalanceTimeout(c.RebalanceTimeout),
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return c.ClientBackoff.Delay(tries)
		}),

		kgo.FetchMaxWait(c.FetchMaxWait),
		kgo.FetchMaxBytes(c.FetchMaxBytes),

		// Коммитим сами, после обработки батча - иначе запись можно потерять
		kgo.DisableAutoCommit(),

		// Ребаланс не начнется, пока не вызван AllowRebalance.
		// Так партиции не уедут к другому консьюмеру, пока мы обрабатываем и коммитим батч
		kgo.BlockRebalanceOnPoll(),

		kgo.WithLogger(kslog.New(c.Log)),
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
