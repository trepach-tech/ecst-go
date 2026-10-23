// Package ecst собирает [Service] из двух подключаемых частей:
// [outbox.Worker], который публикует события из таблицы,
// и [inbox.Worker], который обрабатывает входящие события.
//
// Обе части опциональны и настраиваются через [Config]
package ecst

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/trepach-tech/ecst-go/inbox"
	"github.com/trepach-tech/ecst-go/internal/producer"
	"github.com/trepach-tech/ecst-go/outbox"
)

// Service - собранный ECST-модуль: подключенные воркеры со своими продюсерами
type Service struct {
	log *slog.Logger

	outbox *outbox.Worker
	inbox  *inbox.Worker
}

// New создаёт клиентов по конфигу, но ничего не читает и не пишет до вызова [Service.Run].
func New(log *slog.Logger, cfg Config) (*Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cfg.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid ecst config: %w", err)
	}

	if log == nil {
		return nil, fmt.Errorf("log is nil")
	}
	s := &Service{log: log}

	if cfg.Outbox != nil {
		s.outbox, err = outbox.NewWorker(log, cfg.Outbox)
		if err != nil {
			s.Close(ctx)
			return nil, fmt.Errorf("failed to create outbox worker: %w", err)
		}
	}

	if cfg.Inbox != nil {
		s.inbox, err = inbox.NewWorker(cfg.Inbox, log)
		if err != nil {
			s.Close(ctx)
			return nil, fmt.Errorf("failed to create inbox worker: %w", err)
		}
	}

	return s, nil
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

	return s.outbox.P
}

// Run блокируется и работает, пока не отменят ctx.
//
// Ошибку не возвращает: сбой одного батча или одной партиции не должен
// ронять весь сервис, все такие случаи уезжают в лог и DLQ.
func (s *Service) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	if s.outbox != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.outbox.Run(ctx)
		}()
	}

	if s.inbox != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.inbox.Run(ctx)
		}()
	}

	wg.Wait()
	return nil
}

// Close останавливает [inbox.Worker] и [outbox.Worker]
func (s *Service) Close(ctx context.Context) error {
	if s.inbox != nil {
		s.inbox.Stop()
	}

	if s.outbox != nil {
		s.outbox.Stop()
	}

	return nil
}
