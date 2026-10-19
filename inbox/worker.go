package inbox

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/trepach-tech/ecst-go/internal/consumer"
	"github.com/trepach-tech/ecst-go/internal/producer"
)

// Worker обратывает входящие сообщения из kafka
type Worker struct {
	log         *slog.Logger
	dlqProducer *producer.Producer

	// Для каждого топика создаётся отдельный [consumer.Consumer].
	//
	// Это позволяет настраивать пул независимо для каждого топика,
	// поскольку потоки событий могут существенно различаться по объёму
	// и стоимости обработки, в зависимости от типа событий.
	consumers []*consumer.Consumer
}

func NewWorker(cfg *Config, log *slog.Logger) (*Worker, error) {
	w := &Worker{log: log}

	var err error
	w.dlqProducer, err = producer.NewProducer(cfg.DLQProducer)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// Создание [consumer.Consumer] на каждый переданный [handlerToRegister]
	for topic, h := range cfg.TopicHandlers {

		dlqP, err := NewDLQProducer(w.dlqProducer, cfg.DLQTopic(topic))
		if err != nil {
			w.Stop()
			return nil, fmt.Errorf("failed to create dlq inbox producer for topic: %s: %w", topic, err)
		}

		c, err := w.newConsumer(cfg.ConsumerConfigTemplate, topic, cfg.WithTimeout(h), dlqP)
		if err != nil {
			w.Stop()
			return nil, fmt.Errorf("failed to create consumer for topic: %s: %w", topic, err)
		}

		w.consumers = append(w.consumers, c)

		log.LogAttrs(context.Background(), slog.LevelInfo, "inbox: topic registered",
			slog.String("topic", topic),
		)
	}

	return w, nil
}

// newConsumer собирает консьюмера для конкретного топика.
func (w *Worker) newConsumer(
	cfg consumer.Config,
	topic string,
	handler consumer.Handler,
	deadLetterSender consumer.DeadLetterSender,
) (*consumer.Consumer, error) {
	cfg.Log = w.log
	cfg.ClientID = fmt.Sprintf("%s-%s", cfg.ClientID, topic)
	cfg.Topic = topic
	cfg.ProcessorConfig.Handler = handler
	cfg.ProcessorConfig.DeadLetterSender = deadLetterSender

	c, err := consumer.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Run блокируется, пока не отменят ctx: каждый консьюмер читает в своей горутине
func (w *Worker) Run(ctx context.Context) {
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

// Stop останаливает inbowWorker, останавливает консьюмеров и dlq producer.
func (w *Worker) Stop() {
	for _, c := range w.consumers {
		c.Close()
	}
	// TODO: context timeout for close
	w.dlqProducer.Close(context.Background())

	w.consumers = nil
}
