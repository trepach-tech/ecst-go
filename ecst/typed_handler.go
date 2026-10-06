package ecst

import (
	"context"
	"fmt"

	"github.com/giicoo/ecst-go/consumer"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TypedHandler — типизированный [consumer.Handler].
// Такой handler получает декодированный [envelope.Envelope], а не [kgo.Record].
//
// К нему применяются те же требования, что и к [consumer.Handler].
type TypedHandler[T any] func(ctx context.Context, e envelope.Envelope[T]) error

// decode - адаптер: приводит типизированный обработчик к [consumer.Handler].
func decode[T any](h TypedHandler[T]) consumer.Handler {
	return func(ctx context.Context, r *kgo.Record) error {
		// Добавляем информацию о топике, партиции и офсете в контекст
		ctx = withRecord(ctx, r)

		// Ошибка оборачивается в [consumer.ErrPermanent],
		// если повторная обработка события не приведёт к успеху.
		raw, err := envelope.DecodeRaw(r.Value)
		if err != nil {
			return fmt.Errorf("ecst: decode: %w: %w", err, consumer.ErrPermanent)
		}

		e, err := envelope.FromRaw[T](raw)
		if err != nil {
			return fmt.Errorf("ecst: payload: %w: %w", err, consumer.ErrPermanent)
		}

		return h(ctx, e)
	}
}

// Ключ записи в ctx. Свой тип, чтоб не столкнуться с чужими значениями
type recordKey struct{}

func withRecord(ctx context.Context, r *kgo.Record) context.Context {
	return context.WithValue(ctx, recordKey{}, r)
}

// RecordFrom возвращает Kafka-запись, из которой был разобран конверт.
// Запись предназначена для доступа к метаданным сообщения
// — топику, партиции, офсету и заголовкам. Обрабатывать сообщение через неё не следует.
//
// ok == false, если ctx не содержит запись, добавленную через [withRecord].
func RecordFrom(ctx context.Context) (r *kgo.Record, ok bool) {
	r, ok = ctx.Value(recordKey{}).(*kgo.Record)

	return r, ok
}
