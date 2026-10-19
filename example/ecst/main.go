// Пример ECST-сервиса: события пишутся в outbox-таблицу, оттуда уезжают
// в Kafka и возвращаются в inbox-хендлеры этого же сервиса.
//
// Нужен запущенный брокер и созданные топики:
//
//	docker compose up -d
//	for t in orders orders.dlq users users.dlq; do
//		docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
//			--bootstrap-server localhost:9092 --create --if-not-exists \
//			--topic "$t" --partitions 3
//	done
//
// Топики нужны заранее: автосоздание у брокера выключено, а без топика
// ProduceSync роняет весь батч, и вместе с ним - строки, которым топик есть.
// DLQ-топики нужны по той же причине: туда уезжает то, что не обработалось
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/trepach-tech/ecst-go/ecst"
	"github.com/trepach-tech/ecst-go/inbox"
	"github.com/trepach-tech/ecst-go/outbox"
)

const (
	broker = "localhost:9092"
	group  = "ecst-example"

	ordersTopic = "orders"
	usersTopic  = "users"
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
	store := newMemStore()

	svc, err := ecst.New(slog.Default(), ecst.Config{
		// Опция 1: outbox-воркер забирает события из таблицы
		// и публикует их своим продюсером
		Outbox: outbox.DefaultConfig(store, broker),

		// Опция 2: inbox-воркер поднимает пул консьюмеров на каждый топик
		Inbox: inboxConfig(),
	})
	if err != nil {
		return err
	}
	defer svc.Close(ctx)

	// Бизнес-логика: пишет события в свою БД, про Kafka ничего не знает
	go emitEvents(ctx, store)

	svc.Run(ctx)

	return nil
}

func inboxConfig() *inbox.Config {
	cfg := inbox.DefaultConfig(group, broker)

	// ecst.Topic разбирает конверт за хендлер: битое событие уедет в DLQ
	// сразу, без ретраев. Второй аргумент - размер пула консьюмеров
	// на топик, 0 - взять InboxConfig.PoolSize
	cfg.Register(
		inbox.RegisterTypedHandler(ordersTopic, handleOrder),
		inbox.RegisterTypedHandler(usersTopic, handleUser),
	)

	return cfg
}
