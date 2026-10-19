package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Ключ [partitionWorker] - на каждую партицию в топике отдельный воркер
type tp struct {
	t string
	p int32
}

// Маршрутизатор [partitionWorker]
// Распределяет записи между worker'ами. Владеет ими: создаёт их при подключении и ребалансе.
type partitionRouter struct {
	log *slog.Logger

	// Меняется только в callback'ах ребаланса, а они не бегают
	// параллельно с раздачей батчей (kgo.BlockRebalanceOnPoll),
	// поэтому мьютекс не нужен
	workers map[tp]*partitionWorker

	proc *processor
}

func newPartitionRouter(log *slog.Logger, proc *processor) (*partitionRouter, error) {
	if log == nil {
		return nil, errors.New("consumer: log is required")
	}

	return &partitionRouter{
		log:     log,
		workers: make(map[tp]*partitionWorker),
		proc:    proc,
	}, nil
}

func (r *partitionRouter) dispatch(
	ctx context.Context,
	cl *kgo.Client,
	fetch kgo.FetchTopicPartition,
) {
	w, ok := r.workers[tp{fetch.Topic, fetch.Partition}]
	if !ok {
		// Для партиции нет worker'a.
		return
	}

	// Если worker уже завершился, не отправляем ему батч.
	select {
	case <-w.failed:
		r.pause(ctx, cl, w)
		return
	default:
	}

	// Ожидаем свободного места в буфере или завершения worker.
	select {
	case w.recs <- fetch.Records:
	case <-w.failed:
		r.pause(ctx, cl, w)
	case <-ctx.Done():
		return
	}
}

// pause останавливает чтение партиции, на которой встал worker.
//
// Офсет по ней уже не двинется, но брокер продолжает ее отдавать -
// без паузы мы бы качали данные прямо в мусор. Записи из буфера
// отбрасываются и перечитаются после Resume
func (r *partitionRouter) pause(ctx context.Context, cl *kgo.Client, pc *partitionWorker) {
	cl.PauseFetchPartitions(map[string][]int32{pc.topic: {pc.partition}})

	// ERROR, а не WARN: сама собой партиция не поедет - ребаланс
	// запускает только смена состава группы, а мы для брокера живы
	r.log.LogAttrs(ctx, slog.LevelError, "consumer: partition paused",
		slog.String("topic", pc.topic),
		slog.Int("partition", int(pc.partition)),
		slog.Any("cause", pc.err),
	)
}

func (r *partitionRouter) assigned(
	commitTimeout time.Duration,
) func(context.Context, *kgo.Client, map[string][]int32) {

	return func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
		// Партиция могла остаться на паузе с прошлого владения: тогда на ней встал
		// воркер. Пауза живет в клиенте и ребаланс ее не снимает, а воркер теперь
		// новый - иначе он вечно ждал бы записи, которые клиент не фетчит.
		// Для непаузных партиций это no-op
		cl.ResumeFetchPartitions(assigned)

		for topic, partitions := range assigned {
			for _, partition := range partitions {
				w := &partitionWorker{
					log:           r.log,
					commitTimeout: commitTimeout,
					commiter:      cl,
					processor:     r.proc,

					topic:     topic,
					partition: partition,

					quit:   make(chan struct{}),
					done:   make(chan struct{}),
					failed: make(chan struct{}),
					recs:   make(chan []*kgo.Record, 5),
				}
				r.workers[tp{topic, partition}] = w
				go w.consume(ctx)
			}
		}
	}
}

// lost гасит worker'ов, которые больше не наши, и ждет, пока они
// доработают текущий батч.
//
// Вешается и на revoked, и на lost: разница только в том, пройдет ли коммит.
// При штатном отзыве (revoked) партиция еще наша и коммит успеет,
// при потере (lost) - нет, и батч перечитает новый владелец
func (r *partitionRouter) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := tp{topic, partition}

			// worker обязан быть: assigned отрабатывает до фетча новых
			// партиций, а lost - до того, как поллинг разрешат снова.
			// Проверка на случай, если это когда-нибудь перестанет быть
			// правдой: паника из callback'а ребаланса убьет процесс
			pc, ok := r.workers[tp]
			if !ok {
				continue
			}

			delete(r.workers, tp)
			close(pc.quit)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}
