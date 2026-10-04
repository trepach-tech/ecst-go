package producer

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"
)

// Сколько подтверждений записи ждать от брокера
type Acks string

const (
	// Все ISR-реплики. Единственный вариант, при котором работает идемпотентность
	AcksAll Acks = "all"

	// Только лидер партиции: быстрее, но запись теряется при его падении
	AcksLeader Acks = "leader"

	// Не ждем подтверждения вообще: fire-and-forget
	AcksNone Acks = "none"
)

// Кодек сжатия батчей
type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionGzip   Compression = "gzip"
	CompressionSnappy Compression = "snappy"
	CompressionLz4    Compression = "lz4"
	CompressionZstd   Compression = "zstd"
)

func (c Compression) codec() (kgo.CompressionCodec, bool) {
	switch c {
	case CompressionNone:
		return kgo.NoCompression(), true
	case CompressionGzip:
		return kgo.GzipCompression(), true
	case CompressionSnappy:
		return kgo.SnappyCompression(), true
	case CompressionLz4:
		return kgo.Lz4Compression(), true
	case CompressionZstd:
		return kgo.ZstdCompression(), true
	}
	return kgo.CompressionCodec{}, false
}

type Config struct {
	// Bootstrap servers
	Brokers  []string
	ClientID string

	//////////////
	// SECURITY //
	//////////////
	SASLUser string
	SASLPass string
	TLS      *tls.Config

	/////////////////
	// RELIABILITY //
	/////////////////

	// Сколько подтверждений ждать от брокера
	Acks Acks

	// Задержка между повторными попытками отправки
	Backoff backoff.Config

	// Общее время "жизни" записи (сумма всех ретраев)
	RecordDeliveryTimeout time.Duration

	// Время ожидание ACKS от брокера
	ProduceRequestTimeout time.Duration

	// Время на подключение к брокеру
	DialTimeout time.Duration

	////////////
	// MEMORY //
	////////////

	// Время наполения батча до его отправки
	Linger time.Duration

	// Максимальный размер батча (отправляется при достижении)
	BatchMaxBytes int32

	// Кодеки сжатия батчей в порядке предпочтения:
	// брокер выберет первый, который поддерживает.
	// Пустой список - без сжатия
	Compression []Compression

	// Буффер - место в памяти куда складываются записи
	// после Produce, пока брокер недоступен
	//
	// Максимальное кол-во записей в буффере клиента
	MaxBufferedRecords int

	// Максимальный объем буффера клиента
	MaxBufferedBytes int
}

// DefaultConfig возвращает конфиг с разумными значениями по умолчанию.
// Нужные поля можно переопределить после вызова.
func DefaultConfig(brokers ...string) Config {
	return Config{
		Brokers:  brokers,
		ClientID: "ecst-producer",

		Acks:    AcksAll,
		Backoff: backoff.Default(),

		RecordDeliveryTimeout: 15 * time.Second,
		ProduceRequestTimeout: 5 * time.Second,
		DialTimeout:           5 * time.Second,

		Linger:        2 * time.Millisecond,
		BatchMaxBytes: 1 << 20, // 1 MiB, не больше message.max.bytes на брокере
		Compression:   []Compression{CompressionZstd, CompressionSnappy},

		MaxBufferedRecords: 50_000,
		MaxBufferedBytes:   256 << 20, // 256 MiB
	}
}

// Validate проверяет конфиг и возвращает все найденные проблемы разом
func (c Config) Validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("config: "+msg)) }

	if len(c.Brokers) == 0 {
		add("brokers are required")
	}

	switch c.Acks {
	case AcksAll, AcksLeader, AcksNone:
	case "":
		add("Acks is required")
	default:
		add("unknown Acks: " + string(c.Acks))
	}

	if err := c.Backoff.Validate(); err != nil {
		errs = append(errs, err)
	}

	for _, comp := range c.Compression {
		if _, ok := comp.codec(); !ok {
			add("unknown Compression: " + string(comp))
		}
	}

	// Без RecordDeliveryTimeout запись ретраится вечно, поэтому он обязателен
	if c.RecordDeliveryTimeout <= 0 {
		add("RecordDeliveryTimeout must be > 0")
	}
	if c.ProduceRequestTimeout <= 0 {
		add("ProduceRequestTimeout must be > 0")
	}
	if c.DialTimeout <= 0 {
		add("DialTimeout must be > 0")
	}

	if c.Linger < 0 {
		add("Linger must be >= 0")
	}
	if c.RecordDeliveryTimeout > 0 && c.RecordDeliveryTimeout <= c.Linger {
		add("RecordDeliveryTimeout must be > Linger")
	}

	if c.BatchMaxBytes <= 0 {
		add("BatchMaxBytes must be > 0")
	}
	if c.MaxBufferedRecords <= 0 {
		add("MaxBufferedRecords must be > 0")
	}
	if c.MaxBufferedBytes <= 0 {
		add("MaxBufferedBytes must be > 0")
	}
	if c.MaxBufferedBytes > 0 && c.BatchMaxBytes > 0 && c.MaxBufferedBytes < int(c.BatchMaxBytes) {
		add("MaxBufferedBytes must be >= BatchMaxBytes")
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

		kgo.RecordDeliveryTimeout(c.RecordDeliveryTimeout),
		kgo.ProduceRequestTimeout(c.ProduceRequestTimeout),

		// Экспоненциальная задержка с джиттером между ретраями
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return c.Backoff.Delay(tries)
		}),

		// Батчинг и сжатие
		kgo.ProducerLinger(c.Linger),
		kgo.ProducerBatchMaxBytes(c.BatchMaxBytes),
		kgo.ProducerBatchCompression(c.codecs()...),

		// Backpressure: при заполнении буфера Produce блокируется
		kgo.MaxBufferedRecords(c.MaxBufferedRecords),
		kgo.MaxBufferedBytes(c.MaxBufferedBytes),

		kgo.WithLogger(kslog.New(slog.Default())),
	}

	switch c.Acks {
	case AcksLeader:
		// Идемпотентность требует acks=all, иначе kgo.NewClient вернет ошибку
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case AcksNone:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
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

// Перевод [Config.Compression] в кодеки kgo
func (c Config) codecs() []kgo.CompressionCodec {
	codecs := make([]kgo.CompressionCodec, 0, len(c.Compression))

	for _, comp := range c.Compression {
		if codec, ok := comp.codec(); ok {
			codecs = append(codecs, codec)
		}
	}

	if len(codecs) == 0 {
		codecs = append(codecs, kgo.NoCompression())
	}

	return codecs
}
