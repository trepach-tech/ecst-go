package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/trepach-tech/ecst-go/internal/producer"
)

// Заголовки, которые [DLQProducer] добавляет к записи:
// без них в DLQ лежит анонимный блоб, по которому не найти ни источник, ни причину
const (
	HeaderDLQTopic     = "dlq_source_topic"
	HeaderDLQPartition = "dlq_source_partition"
	HeaderDLQOffset    = "dlq_source_offset"
	HeaderDLQError     = "dlq_error"
	HeaderDLQTime      = "dlq_time"
)

type SyncProducer interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) error
	Close(context.Context) error
}

// DLQProducer складывает необработанные записи в отдельный топик
type DLQProducer struct {
	SyncProducer
	topic string
}

func NewDLQProducer(p *producer.Producer, topic string) (*DLQProducer, error) {
	if p == nil {
		return nil, errors.New("dlq: producer is required")
	}
	if topic == "" {
		return nil, errors.New("dlq: topic is required")
	}

	return &DLQProducer{
		SyncProducer: p,
		topic:        topic,
	}, nil
}

// Send отправляет запись в DLQ-топик синхронно: вернуться раньше, чем брокер
// подтвердил запись, нельзя - консьюмер по возврату сразу коммитит офсет исходной
func (d *DLQProducer) Send(ctx context.Context, r *kgo.Record, cause error) error {
	if err := d.ProduceSync(ctx, d.record(r, cause)); err != nil {
		return fmt.Errorf("dlq: %w", err)
	}

	return nil
}

// record - копия записи в DLQ-топик с метаданными происхождения
func (d *DLQProducer) record(r *kgo.Record, cause error) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(r.Headers)+5)
	headers = append(headers, r.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderDLQTopic, Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: HeaderDLQPartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: HeaderDLQOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: HeaderDLQTime, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)

	return &kgo.Record{
		Topic:   d.topic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}
}
