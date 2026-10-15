package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/internal/producer"
)

// Worker перекладывает [OutboxMessage] из [Store] в Kafka
type Worker struct {
	log *slog.Logger

	P *producer.Producer

	batchSize    int
	batchTimeout time.Duration

	pollInterval time.Duration
	store        Store

	storeMaxAttempts int
	backoffRetry     backoff.Retry
}

func NewWorker(log *slog.Logger, cfg *Config) (*Worker, error) {
	p, err := producer.NewProducer(cfg.Producer)
	if err != nil {
		return nil, fmt.Errorf("failed to create outbox producer: %w", err)
	}

	return &Worker{
		log:              log,
		P:                p,
		batchSize:        cfg.BatchSize,
		batchTimeout:     cfg.BatchTimeout,
		pollInterval:     cfg.PollInterval,
		store:            cfg.Store,
		storeMaxAttempts: cfg.StoreMaxAttempts,
		backoffRetry: backoff.Retry{
			Backoff: cfg.Backoff,
		},
	}, nil
}

// Run блокируется, пока не отменят ctx.
func (w *Worker) Run(ctx context.Context) {
	w.log.LogAttrs(ctx, slog.LevelInfo, "outbox: started",
		slog.Int("batch_size", w.batchSize),
		slog.Duration("poll_interval", w.pollInterval),
	)

	for {
		sent, err := w.processBatch(ctx)
		if err != nil {
			// Закрывают сервис - это не сбой
			if ctx.Err() != nil {
				break
			}

			// Логируем ошибку при обработке батча [OutboxMessage].
			// Повторная обработка произойдет при следующем заходе.
			w.log.LogAttrs(ctx, slog.LevelError, "outbox: batch failed", slog.Any("error", err))
		}

		// Батч набрался целиком - в таблице скорее всего есть еще,
		// идем за следующим без паузы
		if sent == w.batchSize {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(w.pollInterval):
		}

		if ctx.Err() != nil {
			return
		}
	}

	w.log.LogAttrs(ctx, slog.LevelInfo, "outbox: stopped", slog.Any("reason", ctx.Err()))
}

// processBatch - один заход: забрать - опубликовать - отметить.
//
// Возвращает число опубликованных сообщений
func (w *Worker) processBatch(parentCtx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(parentCtx, w.batchTimeout)
	defer cancel()

	var msgs []OutboxMessage

	err := w.retry(ctx, "fetch", func() (err error) {
		msgs, err = w.store.Fetch(ctx, w.batchSize)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("outbox: fetch: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	// Строка ищется по своей записи: результаты отправки приезжают вразнобой
	rows := make(map[*kgo.Record]string, len(msgs))
	records := make([]*kgo.Record, 0, len(msgs))

	for _, m := range msgs {
		rec, err := m.Record()
		if err != nil {
			// Ретраем это не лечится, поэтому строку убираем с дороги:
			// иначе она вернется из следующего Fetch и так до правки руками.
			// Батч из-за нее не останавливаем
			w.MarkFailed(ctx, m.ID, err)

			continue
		}

		rows[rec] = m.ID
		records = append(records, rec)
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Синхронно и по каждой записи отдельно: отметить строку отправленной
	// можно только после подтверждения брокером, а судьба у записей разная -
	// сводить батч к первой ошибке значило бы перепубликовывать доехавшие
	ids, failed := w.publish(ctx, rows, records)

	if len(ids) > 0 {
		// Отметка не отменяется вместе с ctx: неотмеченные строки поедут
		// в Kafka повторно, а это дубли на стороне потребителя
		markCtx, markCancel := context.WithTimeout(context.WithoutCancel(parentCtx), w.batchTimeout)
		defer markCancel()

		// Ретраи здесь особенно важны: незакоммиченная отметка - это дубли
		// на стороне потребителя, а не просто отложенный цикл
		markErr := w.retry(markCtx, "mark sent", func() error {
			return w.store.MarkSent(markCtx, ids)
		})
		if markErr != nil {
			return len(ids), fmt.Errorf("outbox: mark sent: %w", markErr)
		}

		w.log.LogAttrs(ctx, slog.LevelDebug, "outbox: batch published", slog.Int("count", len(ids)))
	}

	// Строки, которые не доехали по преходящей причине, остались
	// неотмеченными и приедут в следующем заходе
	if failed > 0 {
		return len(ids), fmt.Errorf("outbox: %d of %d records not delivered", failed, len(records))
	}

	return len(ids), nil
}

// publish публикует записи и разбирает результат по каждой.
//
// Возвращает id доехавших строк и число тех, которые стоит попробовать снова.
// Строки, которые брокер отверг окончательно, уезжают в MarkFailed:
// повтор их не вылечит
func (w *Worker) publish(ctx context.Context, rows map[*kgo.Record]string, records []*kgo.Record) (ids []string, failed int) {
	ids = make([]string, 0, len(records))

	for _, res := range w.P.ProduceSyncResults(ctx, records...) {
		id := rows[res.Record]

		switch {
		case res.Err == nil:
			ids = append(ids, id)

		case producer.Permanent(res.Err):
			w.MarkFailed(ctx, id, fmt.Errorf("outbox: produce: %w", res.Err))

		default:
			failed++

			w.log.LogAttrs(ctx, slog.LevelError, "outbox: record not delivered",
				slog.String("id", id),
				slog.String("topic", res.Record.Topic),
				slog.Any("error", res.Err),
			)
		}
	}

	return ids, failed
}

// MarkFailed уводит неисправимую строку с дороги.
//
// Контекст отвязан от отмены: если отметка не доедет, строка вернется
// из следующего Fetch и упрется в ту же ошибку
func (w *Worker) MarkFailed(parentCtx context.Context, id string, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), w.batchTimeout)
	defer cancel()

	w.log.LogAttrs(ctx, slog.LevelError, "outbox: message failed",
		slog.String("id", id),
		slog.Any("error", cause),
	)

	err := w.retry(ctx, "mark failed", func() error {
		return w.store.MarkFailed(ctx, id, cause)
	})
	if err != nil {
		w.log.LogAttrs(ctx, slog.LevelError, "outbox: mark failed failed",
			slog.String("id", id),
			slog.Any("error", err),
		)
	}
}

// retry повторяет запрос к [Store], логируя каждую неудачную попытку.
//
// Прерывается сразу на отмене ctx и на [backoff.ErrPermanent]: им стор
// сообщает, что повторять бессмысленно - нарушение констрейнта, битая схема
func (w *Worker) retry(ctx context.Context, what string, op func() error) error {
	return w.backoffRetry.Do(ctx, w.storeMaxAttempts, op, func(attempt int, err error) {
		w.log.LogAttrs(ctx, slog.LevelWarn, "outbox: "+what+" failed, retrying",
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)
	})
}

// Сколько ждать флаша буфера на закрытие продюсера.
//
// Отвязанный от отмены контекст без таймаута висел бы вечно, если брокер
// недоступен: RecordDeliveryTimeout ограничивает одну запись, а не весь буфер
const closeTimeout = 30 * time.Second

// Stop останаливает работу, останавливает консьюмеров. Вызывать после возврата из run
func (w *Worker) Stop() {
	// Контекст без отмены: Close флашит буфер, а на shutdown ctx уже отменен -
	// иначе недоотправленные записи потерялись бы. Таймаут - чтобы не висеть
	// на недоступном брокере
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), closeTimeout)
	defer cancel()

	w.P.Close(closeCtx)
	w.P = nil
}
