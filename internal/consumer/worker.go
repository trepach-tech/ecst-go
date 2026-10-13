package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type PartitionWorkerConfig struct {
	// Сколько ждать подтверждения коммита офсетов.
	// Коммит не отменяется вместе с ctx, поэтому таймаут обязателен
	CommitTimeout time.Duration
}

type recordCommiter interface {
	CommitRecords(ctx context.Context, recs ...*kgo.Record) error
}

type partitionWorker struct {
	log *slog.Logger

	commitTimeout time.Duration
	// commiter нужен, чтобы каждый воркер мог коммитить самостоятельно
	commiter recordCommiter

	processor *processor

	// Идентификатор воркера
	topic     string
	partition int32

	// Сигнал "завершаться"
	quit chan struct{}

	// Сигнал "завершился"
	done chan struct{}

	// Сигнал "встал на необработанной записи"
	failed chan struct{}

	// Причина остановки, читать после <-failed
	err error

	// Канал записей к обработке
	recs chan []*kgo.Record
}

// ctx - клиентский, отменяется только на закрытии клиента:
// по его отмене обрываются ретраи и ожидания бэкоффа
func (w *partitionWorker) consume(ctx context.Context) {
	defer close(w.done)
	for {
		select {
		case <-w.quit:
			return
		case recs := <-w.recs:
			for _, rec := range recs {
				// Получен сигнал завершаться, батч оставляем недоделанным.
				// Он не коммитится и перечитается новым владельцем,
				select {
				case <-w.quit:
					return
				default:
				}

				// Запись не удалось ни обработать, ни положить в DLQ.
				// Воркер встает: батч не коммитим, он перечитается после
				// ребаланса или рестарта. Коммитить дальше нельзя -
				// CommitRecords двигает офсет на максимум из батча
				// и перепрыгнул бы эту запись
				if err := w.processor.process(ctx, rec); err != nil {
					// Консьюмера закрывают: партиция не залипла,
					// и шуметь в логах не о чем
					if ctx.Err() != nil {
						return
					}

					w.err = err
					close(w.failed)

					w.log.LogAttrs(ctx, slog.LevelError, "consumer: worker stopped",
						slog.String("topic", w.topic),
						slog.Int("partition", int(w.partition)),
						slog.Int64("offset", rec.Offset),
						slog.Any("error", err),
					)

					return
				}
			}

			// Записи уже обработаны, а хендлер идемпотентен: незакоммиченный
			// батч просто приедет повторно. Ошибка ожидаема, если партицию
			// отобрали на ребалансе
			if err := w.commit(recs); err != nil {
				w.log.LogAttrs(ctx, slog.LevelError, "consumer: commit failed",
					slog.String("topic", w.topic),
					slog.Int("partition", int(w.partition)),
					slog.Any("error", err),
				)
			}
		}
	}
}

func (w *partitionWorker) commit(recs []*kgo.Record) error {
	// Контекст отвязан от отмены: батч уже обработан, и если не дать себя
	// закоммитить на shutdown, эти записи приедут повторно после перезапуска
	ctx, cancel := context.WithTimeout(context.Background(), w.commitTimeout)
	defer cancel()
	return w.commiter.CommitRecords(ctx, recs...)
}
