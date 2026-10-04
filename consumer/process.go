package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/twmb/franz-go/pkg/kgo"
)

// processor обрабатывает одну запись: ретраи с бэкоффом, а если они
// не помогли - DLQ.
//
// Один на консьюмера и общий для всех его воркеров, поэтому состояния
// не держит: вызывается из горутины каждой партиции
type processor struct {
	cfg     Config
	handler Handler
	log     *slog.Logger
}

func newProcessor(cfg Config, handler Handler, log *slog.Logger) *processor {
	return &processor{
		cfg:     cfg,
		handler: handler,
		log:     log,
	}
}

// process обрабатывает запись: ретраи, а если они не помогли - DLQ.
//
// Ошибка означает, что запись не пристроена: коммитить ее офсет нельзя,
// иначе запись потеряется. Батч перечитается заново (at-least-once)
func (p *processor) process(ctx context.Context, r *kgo.Record) error {
	err := p.retry(ctx, p.cfg.HandlerMaxAttempts, "handler", r, func() error {
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

	dlqErr := p.retry(ctx, p.cfg.DLQMaxAttempts, "dlq send", r, func() error {
		return p.cfg.DLQ.Send(ctx, r, err)
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
	return backoff.Retry{
		Config:   p.cfg.Backoff,
		Attempts: attempts,
		OnRetry: func(attempt int, err error) {
			p.log.LogAttrs(ctx, slog.LevelWarn, "consumer: "+what+" failed, retrying",
				slog.String("topic", r.Topic),
				slog.Int("partition", int(r.Partition)),
				slog.Int64("offset", r.Offset),
				slog.Int("attempt", attempt),
				slog.Any("error", err),
			)
		},
	}.Do(ctx, op)
}
