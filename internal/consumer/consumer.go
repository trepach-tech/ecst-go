package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer читает назначенный ему топик: на каждую партицию
// поднимается свой воркер, который сам обрабатывает и коммитит свои записи.
//
// Цикл поллинга только раздает батчи воркерам и потому не задерживает ребаланс
type Consumer struct {
	log *slog.Logger

	client *kgo.Client
	// Сколько записей максимум отдает один Poll.
	// Это же размер батча между коммитами
	maxPollRecords int

	router *partitionRouter

	proc *processor
}

func NewConsumer(cfg Config) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}

	proc, err := newProcessor(cfg.Log, cfg.ProcessorConfig)
	if err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	router, err := newPartitionRouter(cfg.Log, proc)
	if err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}

	// Воркеры поднимаются и гасятся по назначению партиций, поэтому
	// пул цепляется к клиенту через callback'и ребаланса.
	// revoked и lost делают одно и то же: в обоих случаях партиция больше
	// не наша и воркера надо остановить
	opts := append(cfg.opts(),
		kgo.OnPartitionsAssigned(router.assigned(cfg.PartitionWorkerConfig.CommitTimeout)),
		kgo.OnPartitionsRevoked(router.lost),
		kgo.OnPartitionsLost(router.lost),
	)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to init consumer client: %w", err)
	}

	return &Consumer{
		log:            cfg.Log,
		client:         client,
		maxPollRecords: cfg.MaxPollRecords,
		proc:           proc,
		router:         router,
	}, nil
}

// Run блокируется и читает записи, пока не отменят ctx или не закроют клиент.
//
// Ошибку не возвращает: сбой обработки останавливает только свою партицию,
// а не консьюмера целиком (см. [Handler]).
func (c *Consumer) Run(ctx context.Context) {
	for !c.pollOnce(ctx) {
	}

	c.log.LogAttrs(ctx, slog.LevelInfo, "consumer: stopped", slog.Any("reason", ctx.Err()))
}

// pollOnce - одна итерация: poll -> раздача батчей [partitionWorker] через [partitionRouter] -> AllowRebalance.
//
// Вынесено в отдельную функцию ради defer: пока не вызван AllowRebalance,
// ребаланс не начнется (см. [kgo.BlockRebalanceOnPoll]), поэтому партиции
// не уедут к другому консьюмеру, пока мы раздаем батчи
func (c *Consumer) pollOnce(ctx context.Context) (stop bool) {
	// PollRecords, а не PollFetches: при BlockRebalanceOnPoll размер батча
	// надо ограничивать, чтоб воркеры разобрали его быстрее, чем истечет
	// RebalanceTimeout. Верхняя граница на всех pconsumer'ов: батч может целиком
	// оказаться из одной партиции
	fetches := c.client.PollRecords(ctx, c.maxPollRecords)

	// Poll регистрирует поллера и блокирует ребаланс, даже если вернул ошибку,
	// поэтому разрешаем ребаланс на любом выходе
	defer c.client.AllowRebalance()

	if fetches.IsClientClosed() || ctx.Err() != nil {
		return true
	}

	// Ошибки фетча не фатальны: клиент сам переподключится и перечитает
	fetches.EachError(func(topic string, partition int32, err error) {
		c.log.LogAttrs(ctx, slog.LevelError, "consumer: fetch failed",
			slog.String("topic", topic),
			slog.Int("partition", int(partition)),
			slog.Any("error", err),
		)
	})

	var wg sync.WaitGroup

	fetches.EachPartition(func(fetch kgo.FetchTopicPartition) {
		wg.Add(1)

		go func(p kgo.FetchTopicPartition) {
			defer wg.Done()
			c.router.dispatch(ctx, c.client, p)
		}(fetch)
	})

	wg.Wait()

	return ctx.Err() != nil
}

// Close закрывает клиент и выходит из группы, чтоб партиции сразу разъехались
// по живым консьюмерам, не дожидаясь SessionTimeout.
//
// Выход из группы дергает callback ребаланса, а тот дожидается, пока воркеры
// доработают текущие батчи и закоммитят их. Контекст поэтому не нужен:
// коммитят воркеры сами. Вызывать после возврата из Run.
//
// [Config.ProccesorConfig.DeadLetterSender] не закрывает - ею владеет тот, кто его создал
func (c *Consumer) Close() {
	c.client.Close()
}
