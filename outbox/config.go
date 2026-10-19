package outbox

import (
	"errors"
	"fmt"
	"time"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/internal/producer"
)

type Config struct {
	// Откуда читать и где отмечать отправленное. Обязателен
	Store Store

	// Конфиг продюсера, которым публикуются события. Свой у каждой части: публикация
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

	// Сколько раз пытаться выполнить один запрос к [Store].
	// 1 - без повторов.
	//
	// База отваливается и возвращается: без ретраев каждая такая пауза
	// стоила бы целого цикла публикации
	StoreMaxAttempts int

	// Задержка между попытками запроса к [Store].
	// Без нее все попытки выгорают за миллисекунды, пока база лежит
	Backoff backoff.Backoff
}

func DefaultConfig(store Store, brokers ...string) *Config {
	p := producer.DefaultConfig(brokers...)
	p.ClientID = "ecst-outbox"

	return &Config{
		Store:            store,
		Producer:         p,
		BatchSize:        100,
		PollInterval:     time.Second,
		BatchTimeout:     30 * time.Second,
		StoreMaxAttempts: 3,
		Backoff:          backoff.Default(),
	}
}

func (c *Config) Validate() error {
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
