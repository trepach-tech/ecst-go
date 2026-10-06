// Package ecst собирает Event Carried State Transfer из двух подключаемых частей:
// outbox-воркера, который публикует события из таблицы, и inbox-воркера,
// который их читает и раздает по хендлерам.
//
// Обе части опциональны и настраиваются через [Config]
package ecst

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/giicoo/ecst-go/producer"
)

// Сколько ждать флаша буфера на закрытие продюсера.
//
// Отвязанный от отмены контекст без таймаута висел бы вечно, если брокер
// недоступен: RecordDeliveryTimeout ограничивает одну запись, а не весь буфер
const closeTimeout = 30 * time.Second

// Service - собранный ECST-модуль: подключенные воркеры со своими продюсерами
type Service struct {
	// producers — продюсеры, которыми владеет сервис.
	// Сервис отвечает за их создание и закрытие.
	// Каждый worker (outboxWorker и inboxWorker) использует свой продюсер; общего продюсера нет.
	producers []*producer.Producer

	outbox *outboxWorker
	inbox  *inboxWorker
	log    *slog.Logger
}

// New создаёт клиентов по конфигу, но ничего не читает и не пишет до вызова [Service.Run].
func New(cfg Config) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("ecst: %w", err)
	}

	log := slog.Default()
	s := &Service{log: log}

	if cfg.Outbox != nil {
		p, err := s.newProducer(cfg.Outbox.Producer)
		if err != nil {
			s.closeProducers(context.Background())
			return nil, err
		}

		s.outbox = newOutboxWorker(cfg.Outbox, p, log)
	}

	if cfg.Inbox != nil {
		// Продюсер нужен под DLQ-записи
		p, err := s.newProducer(cfg.Inbox.Producer)
		if err != nil {
			s.closeProducers(context.Background())
			return nil, err
		}

		inbox, err := newInboxWorker(cfg.Inbox, p, log)
		if err != nil {
			s.closeProducers(context.Background())
			return nil, err
		}

		s.inbox = inbox
	}

	return s, nil
}

// newProducer создаёт [producer.Producer] и передаёт его во владение Service.
// Service закроет [producer.Producer] при вызове [Service.Close].
func (s *Service) newProducer(cfg producer.Config) (*producer.Producer, error) {
	p, err := producer.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("ecst: %w", err)
	}

	s.producers = append(s.producers, p)

	return p, nil
}

// OutboxProducer возвращает продюсер outbox-воркера для прямой публикации
// в Kafka без записи события в outbox-таблицу (например, в тестах
// или при публикации в служебные топики).
//
// nil, если Outbox не подключен.
func (s *Service) OutboxProducer() *producer.Producer {
	if s.outbox == nil {
		return nil
	}

	return s.outbox.p
}

// Run блокируется и работает, пока не отменят ctx.
//
// Ошибку не возвращает: сбой одного батча или одной партиции не должен
// ронять весь сервис, все такие случаи уезжают в лог и DLQ.
func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup

	if s.outbox != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.outbox.run(ctx)
		}()
	}

	if s.inbox != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.inbox.run(ctx)
		}()
	}

	wg.Wait()
}

// Close останавливает консьюмеров и флашит продюсеров.
// Вызывать после возврата из Run.
//
// Продюсеры закрываются последними: через них консьюмеры отправляют
// сообщения в DLQ.
func (s *Service) Close(ctx context.Context) {
	if s.inbox != nil {
		s.inbox.closeAll()
	}

	s.closeProducers(ctx)
}

// closeProducers флашит и закрывает все клиенты, которые поднял сервис
func (s *Service) closeProducers(ctx context.Context) {
	// Контекст без отмены: Close флашит буфер, а на shutdown ctx уже отменен -
	// иначе недоотправленные записи потерялись бы. Таймаут - чтобы не висеть
	// на недоступном брокере
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()

	for _, p := range s.producers {
		if err := p.Close(closeCtx); err != nil {
			s.log.LogAttrs(closeCtx, slog.LevelError, "ecst: producer close failed", slog.Any("error", err))
		}
	}

	s.producers = nil
}
