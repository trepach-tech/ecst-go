package ecst

import (
	"context"
	"fmt"

	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TypedHandler — типизированный [consumer.Handler].
// Такой handler получает декодированный [envelope.Envelope], а не [kgo.Record].
//
// К нему применяются те же требования, что и к [consumer.Handler].
type TypedHandler[T any] func(ctx context.Context, e envelope.Envelope[T]) error

func decode[T any](h TypedHandler[T]) consumer.Handler {
	return func(ctx context.Context, r *kgo.Record) error {
		// Топик, партиция и офсет в конверте не лежат, а для логов нужны:
		// кладем запись в ctx, чтоб не тащить ее в сигнатуру каждого хендлера
		ctx = withRecord(ctx, r)

		// Разбор сразу в T, а не через [envelope.Raw]: иначе один и тот же
		// конверт пришлось бы разбирать и валидировать дважды на каждую запись
		e, err := envelope.Decode[T](r.Value)
		if err != nil {
			return fmt.Errorf("ecst: %w: %w", err, consumer.ErrPermanent)
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
