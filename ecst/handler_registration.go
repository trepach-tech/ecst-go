package ecst

import (
	"github.com/trepach-tech/ecst-go/consumer"
)

// Registration — готовая привязка хендлера к топику для [InboxConfig.Register].
type Registration struct {
	topic    string
	poolSize int
	handler  consumer.Handler
}

// Topic привязывает [TypedHandler] к топику
//
// poolSize - сколько консьюмеров поднять на этот топик;
// 0 - взять [InboxConfig.PoolSize]
func Topic[T any](topic string, poolSize int, h TypedHandler[T]) Registration {
	return Registration{topic: topic, poolSize: poolSize, handler: decode(h)}
}

// RawTopic привязывает к топику [consumer.Handler].
// poolSize - сколько консьюмеров поднять на этот топик;
// 0 - взять [InboxConfig.PoolSize]
//
// Нужен для топиков, в которых лежит не [envelope.Envelope].
// Например: чужой сервис, legacy-формат, CDC от Debezium. Для своих событий есть [Topic]
func RawTopic(topic string, poolSize int, h consumer.Handler) Registration {
	return Registration{topic: topic, poolSize: poolSize, handler: h}
}
