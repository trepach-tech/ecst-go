package ecst

import (
	"context"
	"errors"
	"fmt"

	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler обрабатывает одно событие.
//
// Вызывается параллельно - по горутине на партицию, поэтому должен быть
// безопасен для конкурентного вызова. В пределах одной партиции события
// одной сущности идут по порядку версий.
//
// Должен быть идемпотентным: одно и то же событие может приехать повторно
// (at-least-once). Version для этого и нужна - по ней отбрасывают то,
// что уже применено.
//
// Возврат ошибки приводит к повторным попыткам, после которых событие уезжает
// в DLQ топика. Ошибку, которую бессмысленно повторять, оборачивают
// в [consumer.ErrPermanent] - тогда событие уедет в DLQ сразу
type Handler[T any] func(ctx context.Context, e envelope.Envelope[T]) error

// Registration - готовая привязка хендлера к топику для [InboxConfig.Register].
//
// Тип payload параметризует только ее сборку ([Topic]), а не сам inbox:
// в одном inbox топики с разными payload
type Registration struct {
	topic    string
	poolSize int
	handler  consumer.Handler
}

// Topic привязывает типизированный хендлер к топику: конверт разбирается
// за вызывающего, и наружу транспорт не торчит.
//
// poolSize - сколько консьюмеров поднять на этот топик;
// 0 - взять [InboxConfig.PoolSize]
func Topic[T any](topic string, poolSize int, h Handler[T]) Registration {
	return Registration{topic: topic, poolSize: poolSize, handler: decode(h)}
}

// RawTopic привязывает к топику хендлер, который получает запись как есть.
//
// Нужен для топиков, в которых лежит не [envelope.Envelope]: чужой сервис,
// legacy-формат, CDC от Debezium. Для своих событий есть [Topic]
func RawTopic(topic string, poolSize int, h consumer.Handler) Registration {
	return Registration{topic: topic, poolSize: poolSize, handler: h}
}

// decode превращает типизированный хендлер в [consumer.Handler].
//
// Битый JSON, конверт без обязательных полей или payload чужой схемы -
// ошибка, которую бессмысленно повторять: она заворачивается
// в [consumer.ErrPermanent], и запись уезжает в DLQ сразу, без ретраев и пауз
func decode[T any](h Handler[T]) consumer.Handler {
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

// NewOutboxMessage собирает строку outbox-таблицы из типизированного конверта.
//
// Так событие кладут в таблицу, ничего не зная про Kafka: ключ, значение
// и заголовки записи соберет сам outbox-воркер
func NewOutboxMessage[T any](id, topic string, e envelope.Envelope[T]) (OutboxMessage, error) {
	if id == "" {
		return OutboxMessage{}, errors.New("ecst: outbox message id is required")
	}
	if topic == "" {
		return OutboxMessage{}, errors.New("ecst: outbox message topic is required")
	}

	// ToRaw валидирует конверт сам: до таблицы битое событие не доедет
	raw, err := envelope.ToRaw(e)
	if err != nil {
		return OutboxMessage{}, fmt.Errorf("ecst: %w", err)
	}

	return OutboxMessage{
		ID:    id,
		Topic: topic,
		Event: raw,
	}, nil
}

// Ключ записи в ctx. Свой тип, чтоб не столкнуться с чужими значениями
type recordKey struct{}

func withRecord(ctx context.Context, r *kgo.Record) context.Context {
	return context.WithValue(ctx, recordKey{}, r)
}

// RecordFrom отдает запись, из которой разобран конверт: топик, партиция,
// офсет и заголовки. Нужна для логов и трейсинга - обрабатывать событие
// по ней не надо.
//
// ok == false, если ctx не из хендлера, зарегистрированного через [Topic]
func RecordFrom(ctx context.Context) (r *kgo.Record, ok bool) {
	r, ok = ctx.Value(recordKey{}).(*kgo.Record)

	return r, ok
}
