package inbox

import (
	"context"
	"fmt"

	"github.com/trepach-tech/ecst-go/backoff"
	"github.com/trepach-tech/ecst-go/internal/consumer"
	"github.com/trepach-tech/ecst-go/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// HandlerToRegister — готовый к регистрации через [Config.Register] handler к топику.
type HandlerToRegister struct {
	Topic   string
	Handler consumer.Handler
}

// RegisterTypedHandler привязывает [TypedHandler] к топику
func RegisterTypedHandler[T any](topic string, h TypedHandler[T]) HandlerToRegister {
	return HandlerToRegister{Topic: topic, Handler: toConsumerHandler(h)}
}

// RegisterRawHandler привязывает к топику [consumer.Handler].
//
// Нужен для топиков, в которых лежит не [envelope.Envelope].
// Например: чужой сервис, legacy-формат, CDC от Debezium. Для своих событий есть [RegisterTypedHandler]
func RegisterRawHandler(topic string, h consumer.Handler) HandlerToRegister {
	return HandlerToRegister{Topic: topic, Handler: h}
}

// TypedHandler — типизированный [consumer.Handler].
// Такой handler получает декодированный [message.Message], а не [kgo.Record].
//
// К нему применяются те же требования, что и к [consumer.Handler].
type TypedHandler[T any] func(ctx context.Context, m message.Message[T]) error

type RawHandler func(ctx context.Context, raw message.Raw) error

func toConsumerHandler[T any](h TypedHandler[T]) consumer.Handler {
	return func(ctx context.Context, r *kgo.Record) error {
		// Топик, партиция и офсет в конверте не лежат, а для логов нужны:
		// кладем запись в ctx, чтоб не тащить ее в сигнатуру каждого хендлера
		ctx = withRecord(ctx, r)

		// Разбор сразу в T, а не через [message.Raw]: иначе один и тот же
		// конверт пришлось бы разбирать и валидировать дважды на каждую запись
		m, err := message.Decode[T](r.Value)
		if err != nil {
			return fmt.Errorf("ecst: %w: %w", err, backoff.ErrPermanent)
		}

		return h(ctx, m)
	}
}

// Ключ записи в ctx.
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
