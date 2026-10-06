package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrPermanent - ошибка, которую бессмысленно повторять: битый формат, чужая схема.
// Хендлер оборачивает ее (fmt.Errorf("parse: %w", consumer.ErrPermanent)),
// и запись уезжает в DLQ сразу, без ретраев и пауз
var ErrPermanent = errors.New("permanent error")

// Consumer читает партиции параллельно: на каждую назначенную партицию
// поднимается свой воркер, который сам обрабатывает и коммитит свои записи.
//
// Цикл поллинга только раздает батчи воркерам и потому не задерживает ребаланс
type Consumer struct {
	client *kgo.Client
	split  *splitConsume
}

func NewConsumer(cfg Config, handler Handler) (*Consumer, error) {
	if err := cfg.ValidateConsumer(); err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	if handler == nil {
		return nil, errors.New("consumer: handler is required")
	}

	split := newSplitConsume(cfg, handler, slog.Default())

	// Воркеры поднимаются и гасятся по назначению партиций, поэтому
	// пул цепляется к клиенту через callback'и ребаланса.
	// revoked и lost делают одно и то же: в обоих случаях партиция больше
	// не наша и воркера надо остановить
	opts := append(cfg.ConsumerOpts(),
		kgo.OnPartitionsAssigned(split.assigned),
		kgo.OnPartitionsRevoked(split.lost),
		kgo.OnPartitionsLost(split.lost),
	)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("consumer client: %w", err)
	}

	return &Consumer{
		client: client,
		split:  split,
	}, nil
}

// Run блокируется и читает записи, пока не отменят ctx или не закроют клиент.
//
// Ошибку не возвращает: сбой обработки останавливает только свою партицию,
// а не консьюмера целиком (см. [Handler]).
func (c *Consumer) Run(ctx context.Context) {
	c.split.poll(ctx, c.client)

	slog.LogAttrs(ctx, slog.LevelInfo, "consumer: stopped", slog.Any("reason", ctx.Err()))
}

// Закрывает клиент и выходит из группы, чтоб партиции сразу разъехались
// по живым консьюмерам, не дожидаясь SessionTimeout.
//
// Выход из группы дергает callback ребаланса, а тот дожидается, пока воркеры
// доработают текущие батчи и закоммитят их. Контекст поэтому не нужен:
// коммитят воркеры сами. Вызывать после возврата из Run.
//
// Config.DLQ не закрывает - ею владеет тот, кто ее создал
func (c *Consumer) Close() {
	c.client.Close()
}
