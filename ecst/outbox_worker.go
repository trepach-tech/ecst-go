package ecst

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/giicoo/ecst-go/backoff"
	"github.com/giicoo/ecst-go/consumer"
	"github.com/giicoo/ecst-go/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// outboxWorker перекладывает строки из outbox-таблицы в Kafka
type outboxWorker struct {
	cfg *OutboxConfig
	p   *producer.Producer
	log *slog.Logger
}

func newOutboxWorker(cfg *OutboxConfig, p *producer.Producer, log *slog.Logger) *outboxWorker {
	return &outboxWorker{cfg: cfg, p: p, log: log}
}

// run блокируется, пока не отменят ctx.
//
// Ошибка одного захода воркера не роняет: строки остались неотмеченными
// и приедут в следующем заходе
func (w *outboxWorker) run(ctx context.Context) {
	w.log.LogAttrs(ctx, slog.LevelInfo, "outbox: started",
		slog.Int("batch_size", w.cfg.BatchSize),
		slog.Duration("poll_interval", w.cfg.PollInterval),
	)

	for {
		sent, err := w.processBatch(ctx)
		if err != nil {
			// Закрывают сервис - это не сбой
			if ctx.Err() != nil {
				break
			}

			w.log.LogAttrs(ctx, slog.LevelError, "outbox: batch failed", slog.Any("error", err))
		}

		// Батч набрался целиком - в таблице скорее всего есть еще,
		// идем за следующим без паузы
		if sent == w.cfg.BatchSize {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(w.cfg.PollInterval):
		}
	}

	w.log.LogAttrs(ctx, slog.LevelInfo, "outbox: stopped", slog.Any("reason", ctx.Err()))
}

// processBatch - один заход: забрать - опубликовать - отметить.
//
// Возвращает число опубликованных сообщений
func (w *outboxWorker) processBatch(parentCtx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(parentCtx, w.cfg.BatchTimeout)
	defer cancel()

	var msgs []OutboxRecord

	err := w.retry(ctx, "fetch", func() (err error) {
		msgs, err = w.cfg.Store.Fetch(ctx, w.cfg.BatchSize)

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
		rec, err := m.record()
		if err != nil {
			// Ретраем это не лечится, поэтому строку убираем с дороги:
			// иначе она вернется из следующего Fetch и так до правки руками.
			// Батч из-за нее не останавливаем
			w.markFailed(ctx, m.ID, err)

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
		markCtx, markCancel := context.WithTimeout(context.WithoutCancel(parentCtx), w.cfg.BatchTimeout)
		defer markCancel()

		// Ретраи здесь особенно важны: незакоммиченная отметка - это дубли
		// на стороне потребителя, а не просто отложенный цикл
		markErr := w.retry(markCtx, "mark sent", func() error {
			return w.cfg.Store.MarkSent(markCtx, ids)
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
func (w *outboxWorker) publish(ctx context.Context, rows map[*kgo.Record]string, records []*kgo.Record) (ids []string, failed int) {
	ids = make([]string, 0, len(records))

	for _, res := range w.p.ProduceSyncResults(ctx, records...) {
		id := rows[res.Record]

		switch {
		case res.Err == nil:
			ids = append(ids, id)

		case producer.Permanent(res.Err):
			w.markFailed(ctx, id, fmt.Errorf("outbox: produce: %w", res.Err))

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

// record собирает запись из конверта.
//
// Ключ - EntityID: так все события одной сущности ложатся в одну партицию
// и приезжают потребителю в порядке версий
// retry повторяет запрос к [OutboxStore], логируя каждую неудачную попытку.
//
// Прерывается сразу на отмене ctx и на [consumer.ErrPermanent]: им стор
// сообщает, что повторять бессмысленно - нарушение констрейнта, битая схема
func (w *outboxWorker) retry(ctx context.Context, what string, op func() error) error {
	return backoff.Retry{
		Config:    w.cfg.Backoff,
		Attempts:  w.cfg.StoreMaxAttempts,
		Permanent: func(err error) bool { return errors.Is(err, consumer.ErrPermanent) },
		OnRetry: func(attempt int, err error) {
			w.log.LogAttrs(ctx, slog.LevelWarn, "outbox: "+what+" failed, retrying",
				slog.Int("attempt", attempt),
				slog.Any("error", err),
			)
		},
	}.Do(ctx, op)
}

// markFailed уводит неисправимую строку с дороги.
//
// Контекст отвязан от отмены: если отметка не доедет, строка вернется
// из следующего Fetch и упрется в ту же ошибку
func (w *outboxWorker) markFailed(parentCtx context.Context, id string, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), w.cfg.BatchTimeout)
	defer cancel()

	w.log.LogAttrs(ctx, slog.LevelError, "outbox: message failed",
		slog.String("id", id),
		slog.Any("error", cause),
	)

	err := w.retry(ctx, "mark failed", func() error {
		return w.cfg.Store.MarkFailed(ctx, id, cause)
	})
	if err != nil {
		w.log.LogAttrs(ctx, slog.LevelError, "outbox: mark failed failed",
			slog.String("id", id),
			slog.Any("error", err),
		)
	}
}

func (m OutboxRecord) record() (*kgo.Record, error) {
	if m.Topic == "" {
		return nil, errors.New("outbox: message topic is required")
	}

	// Encode валидирует конверт сам
	value, err := m.RawEnvelope.Encode()
	if err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}

	envHeaders := m.RawEnvelope.Headers()
	headers := make([]kgo.RecordHeader, 0, len(envHeaders))
	for k, v := range envHeaders {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	return &kgo.Record{
		Topic:   m.Topic,
		Key:     []byte(m.RawEnvelope.EntityID),
		Value:   value,
		Headers: headers,
	}, nil
}
