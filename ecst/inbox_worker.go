package ecst

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/producer"
)

// inboxWorker держит по пулу консьюмеров на каждый зарегистрированный топик.
//
// Топики разведены по отдельным консьюмерам, а не собраны в один со списком
// топиков, чтоб пул и его размер настраивались на топик: у разных потоков
// событий разный объем и разная цена обработки
type inboxWorker struct {
	cfg *InboxConfig
	log *slog.Logger

	consumers []*consumer.Consumer
}

func newInboxWorker(cfg *InboxConfig, p *producer.Producer, log *slog.Logger) (*inboxWorker, error) {
	w := &inboxWorker{cfg: cfg, log: log}

	for topic, h := range cfg.handlers {
		// DLQ на топик своя: иначе разбирать завал в одной куче
		dlq, err := consumer.NewKafkaDLQ(p, cfg.dlqTopic(topic))
		if err != nil {
			w.closeAll()
			return nil, fmt.Errorf("inbox: %s: %w", topic, err)
		}

		size := cfg.size(h)
		handler := w.withTimeout(h.handler)

		for i := range size {
			c, err := w.newConsumer(topic, i, handler, dlq)
			if err != nil {
				w.closeAll()
				return nil, err
			}

			w.consumers = append(w.consumers, c)
		}

		log.LogAttrs(context.Background(), slog.LevelInfo, "inbox: topic registered",
			slog.String("topic", topic),
			slog.Int("pool_size", size),
		)
	}

	return w, nil
}

// withTimeout ограничивает время одного вызова хендлера.
//
// Хендлер обязан уважать ctx: сам по себе таймаут его не прервет,
// но по возврату ошибки запись пойдет на ретрай и потом в DLQ,
// а партиция не встанет молча
func (w *inboxWorker) withTimeout(h consumer.Handler) consumer.Handler {
	return func(ctx context.Context, r *kgo.Record) error {
		ctx, cancel := context.WithTimeout(ctx, w.cfg.HandlerTimeout)
		defer cancel()

		return h(ctx, r)
	}
}

// newConsumer собирает консьюмера для одного топика из базового конфига.
//
// Группа общая на топик - консьюмеры пула делят между собой его партиции
func (w *inboxWorker) newConsumer(topic string, idx int, h consumer.Handler, dlq consumer.DLQ) (*consumer.Consumer, error) {
	cfg := w.cfg.Consumer
	cfg.Topics = []string{topic}
	cfg.DLQ = dlq
	cfg.ClientID = fmt.Sprintf("%s-%s-%d", cfg.ClientID, topic, idx)

	c, err := consumer.NewConsumer(cfg, h)
	if err != nil {
		return nil, fmt.Errorf("inbox: %s: %w", topic, err)
	}

	return c, nil
}

// run блокируется, пока не отменят ctx: каждый консьюмер читает в своей горутине
func (w *inboxWorker) run(ctx context.Context) {
	var wg sync.WaitGroup

	for _, c := range w.consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Run(ctx)
		}()
	}

	wg.Wait()

	w.log.LogAttrs(ctx, slog.LevelInfo, "inbox: stopped", slog.Any("reason", ctx.Err()))
}

// closeAll закрывает консьюмеров. Вызывать после возврата из run
func (w *inboxWorker) closeAll() {
	for _, c := range w.consumers {
		c.Close()
	}

	w.consumers = nil
}
