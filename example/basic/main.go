// Пример на сырых записях: пакеты producer и consumer работают
// самостоятельно, без ecst и без конвертов.
//
// Вторая запись намеренно битая - она уезжает в DLQ и не останавливает поток.
//
// Нужен запущенный брокер и созданные топики:
//
//	docker compose up -d
//	docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
//		--bootstrap-server localhost:9092 --create --if-not-exists \
//		--topic orders --partitions 3
//	docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
//		--bootstrap-server localhost:9092 --create --if-not-exists \
//		--topic orders.dlq --partitions 1
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/producer"
)

const (
	broker   = "localhost:9092"
	topic    = "orders"
	dlqTopic = "orders.dlq"
	group    = "ecst-example"

	// Сколько ждать флаша буфера на закрытие продюсера
	closeTimeout = 30 * time.Second
)

func main() {
	// Ctrl+C / SIGTERM отменяют ctx - это сигнал к штатной остановке
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Println("run:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	p, err := producer.NewProducer(producer.DefaultConfig(broker))
	if err != nil {
		return err
	}
	// Тот же продюсер пишет и в DLQ, поэтому закрывается последним.
	// Close флашит буффер, поэтому контекст без отмены, но с таймаутом:
	// на недоступном брокере иначе висли бы вечно
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()

		if err := p.Close(closeCtx); err != nil {
			fmt.Println("close producer:", err)
		}
	}()

	// Вторая запись битая: она уедет в DLQ, но не остановит консьюмера
	p.Produce(ctx, &kgo.Record{Topic: topic, Key: []byte("order-1"), Value: []byte(`{"id":"order-1"}`)})
	p.Produce(ctx, &kgo.Record{Topic: topic, Key: []byte("order-2"), Value: []byte("not json")})

	return consume(ctx, p)
}

func consume(ctx context.Context, p *producer.Producer) error {
	// DLQ обязательна: без нее запись, которую не удалось обработать,
	// встала бы поперек своей партиции
	dlq, err := consumer.NewKafkaDLQ(p, dlqTopic)
	if err != nil {
		return err
	}

	cfg := consumer.DefaultConfig(broker)
	cfg.Group = group
	cfg.Topics = []string{topic}
	cfg.DLQ = dlq

	c, err := consumer.NewConsumer(cfg, handleOrder)
	if err != nil {
		return err
	}
	defer c.Close()

	c.Run(ctx)

	return nil
}

func handleOrder(_ context.Context, r *kgo.Record) error {
	var order struct {
		ID string `json:"id"`
	}

	// Битый JSON ретраить бессмысленно: ErrPermanent отправит запись в DLQ сразу
	if err := json.Unmarshal(r.Value, &order); err != nil {
		return fmt.Errorf("parse: %w: %w", err, consumer.ErrPermanent)
	}

	fmt.Printf("order %s from %s[%d]@%d\n", order.ID, r.Topic, r.Partition, r.Offset)

	return nil
}
