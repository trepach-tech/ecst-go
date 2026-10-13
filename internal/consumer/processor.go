package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/trepach-tech/ecst-go/backoff"
)

// DeadLetterSender - отправляет записи в DLQ, которые не удалось обработать
//
// Send должен завершаться без ошибок только тогда, когда запись действительно сохранена:
// сразу после этого консьюмер коммитит офсет и запись больше не приедет
type DeadLetterSender interface {
	Send(ctx context.Context, r *kgo.Record, cause error) error
}

type ProcessorConfig struct {
	// Сколько раз пытаться обработать одну запись хендлером.
	// 1 - без повторов
	HandlerMaxAttempts int
	Handler            Handler

	// Сколько раз пытаться отправить запись в DLQ
	DLQMaxAttempts   int
	DeadLetterSender DeadLetterSender

	// Задержка между попытками обработки и между ретраями запросов к брокеру
	Backoff backoff.Backoff
}

// processor обрабатывает одну запись:
// сначала с помощью Handler, а если возникла ошибка пытается отправить запись в DLQ
//
// Один на консьюмера и общий для всех его воркеров, поэтому состояния
// не держит: вызывается из горутины каждой партиции
type processor struct {
	log *slog.Logger

	handlerAttempts int
	handler         Handler

	dlqAttempts      int
	deadLetterSender DeadLetterSender

	backoffRetry backoff.Retry
}

func newProcessor(log *slog.Logger, cfg ProcessorConfig) (*processor, error) {
	if log == nil {
		return nil, errors.New("log is required")
	}
	return &processor{
		handlerAttempts:  cfg.HandlerMaxAttempts,
		handler:          cfg.Handler,
		dlqAttempts:      cfg.DLQMaxAttempts,
		deadLetterSender: cfg.DeadLetterSender,
		log:              log,
		backoffRetry: backoff.Retry{
			Backoff: cfg.Backoff,
		},
	}, nil
}

// Ошибка означает, что запись не пристроена: коммитить ее офсет нельзя,
// иначе запись потеряется. Батч перечитается заново (at-least-once)
func (p *processor) process(ctx context.Context, r *kgo.Record) error {
	err := p.retry(ctx, p.handlerAttempts, "handler", r, func() error {
		return p.handler(ctx, r)
	})
	if err == nil {
		return nil
	}

	// Нас останавливают: запись не теряем,
	// батч не коммитится и перечитается после рестарта
	if ctx.Err() != nil {
		return fmt.Errorf("handle record (topic %s, partition %d, offset %d): %w",
			r.Topic, r.Partition, r.Offset, err)
	}

	dlqErr := p.retry(ctx, p.dlqAttempts, "dlq send", r, func() error {
		return p.deadLetterSender.Send(ctx, r, err)
	})
	if dlqErr != nil {
		return fmt.Errorf("send to dlq (topic %s, partition %d, offset %d): %w",
			r.Topic, r.Partition, r.Offset, errors.Join(dlqErr, err))
	}

	p.log.LogAttrs(ctx, slog.LevelWarn, "consumer: record sent to dlq",
		slog.String("topic", r.Topic),
		slog.Int("partition", int(r.Partition)),
		slog.Int64("offset", r.Offset),
		slog.Any("cause", err),
	)

	// Запись пристроена, офсет закоммитится вместе с батчем
	return nil
}

// retry повторяет op, логируя каждую неудачную попытку.
//
// Прерывается сразу на [ErrPermanent] и на отмене ctx - см. [backoff.Retry.Do]
func (p *processor) retry(ctx context.Context, attempts int, what string, r *kgo.Record, op func() error) error {
	return p.backoffRetry.Do(ctx, attempts, op, func(attempt int, err error) {
		p.log.LogAttrs(ctx, slog.LevelWarn, "consumer: "+what+" failed, retrying",
			slog.String("topic", r.Topic),
			slog.Int("partition", int(r.Partition)),
			slog.Int64("offset", r.Offset),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)
	})
}
