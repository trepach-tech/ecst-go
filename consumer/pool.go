package consumer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Ключ worker - на каждую партицию в топике отдельный воркер
type tp struct {
	t string
	p int32
}

type pconsumer struct {
	// Клиент, чтоб каждый воркер мог коммитить самостоятельно
	cl *kgo.Client

	log *slog.Logger

	// Ретраи с бэкоффом и отправка в DLQ - те же, что у [Consumer]
	proc *processor

	// Идентификатор воркера
	topic     string
	partition int32

	// Сигнал "завершаться"
	quit chan struct{}

	// Сигнал "завершился"
	done chan struct{}

	// Сигнал "встал на необработанной записи"
	failed chan struct{}

	// Причина остановки, читать после <-failed
	err error

	// Отправляют туда p.Records = []*kgo.Record
	recs chan []*kgo.Record
}

// Маршрутизатор воркеров
type splitConsume struct {
	log  *slog.Logger
	proc *processor

	// Меняется только в callback'ах ребаланса, а они не бегают
	// параллельно с раздачей батчей (kgo.BlockRebalanceOnPoll),
	// поэтому мьютекс не нужен
	consumers map[tp]*pconsumer
}

func newSplitConsume(cfg Config, handler Handler, log *slog.Logger) *splitConsume {
	if log == nil {
		log = slog.Default()
	}

	return &splitConsume{
		log:       log,
		proc:      newProcessor(cfg, handler, log),
		consumers: make(map[tp]*pconsumer),
	}
}

// ctx - клиентский, отменяется только на закрытии клиента:
// по его отмене обрываются ретраи и ожидания бэкоффа
func (pc *pconsumer) consume(ctx context.Context) {
	defer close(pc.done)
	for {
		select {
		case <-pc.quit:
			return
		case recs := <-pc.recs:
			for _, rec := range recs {
				// Партицию отобрали - бросаем батч недоделанным.
				// Он не коммитится и перечитается новым владельцем,
				select {
				case <-pc.quit:
					return
				default:
				}

				// Запись не удалось ни обработать, ни положить в DLQ.
				// Воркер встает: батч не коммитим, он перечитается после
				// ребаланса или рестарта. Коммитить дальше нельзя -
				// CommitRecords двигает офсет на максимум из батча
				// и перепрыгнул бы эту запись
				if err := pc.proc.process(ctx, rec); err != nil {
					// Консьюмера закрывают: партиция не залипла,
					// и шуметь в логах не о чем
					if ctx.Err() != nil {
						return
					}

					pc.err = err
					close(pc.failed)

					pc.log.LogAttrs(ctx, slog.LevelError, "consumer: worker stopped",
						slog.String("topic", pc.topic),
						slog.Int("partition", int(pc.partition)),
						slog.Int64("offset", rec.Offset),
						slog.Any("error", err),
					)

					return
				}
			}

			// Записи уже обработаны, а хендлер идемпотентен: незакоммиченный
			// батч просто приедет повторно. Ошибка ожидаема, если партицию
			// отобрали на ребалансе
			if err := pc.commit(recs); err != nil {
				pc.log.LogAttrs(ctx, slog.LevelError, "consumer: commit failed",
					slog.String("topic", pc.topic),
					slog.Int("partition", int(pc.partition)),
					slog.Any("error", err),
				)
			}
		}

	}
}

func (pc *pconsumer) commit(recs []*kgo.Record) error {
	// Контекст отвязан от отмены: батч уже обработан, и если не дать себя
	// закоммитить на shutdown, эти записи приедут повторно после перезапуска
	ctx, cancel := context.WithTimeout(context.Background(), pc.proc.cfg.CommitTimeout)
	defer cancel()
	return pc.cl.CommitRecords(ctx, recs...)
}

// dispatch отдает батч воркеру его партиции.
//
// Вызывать из цикла поллинга: fetches.EachPartition(func(p kgo.FetchTopicPartition) {
// s.dispatch(ctx, cl, p) })
func (s *splitConsume) dispatch(ctx context.Context, cl *kgo.Client, p kgo.FetchTopicPartition) {
	pc, ok := s.consumers[tp{p.Topic, p.Partition}]
	if !ok {
		// Партицию отобрали на ребалансе, читать ее больше не наше дело
		return
	}

	// Проверяем отдельно и первым делом: в select из двух готовых вариантов
	// Go выбирает случайный, и пока в recs есть место, батч с тем же успехом
	// уехал бы мертвому воркеру - партиция встала бы на паузу через несколько
	// поллингов, когда забьется буфер, и до тех пор молча
	select {
	case <-pc.failed:
		s.pause(ctx, cl, pc)
		return
	default:
	}

	select {
	case pc.recs <- p.Records:

	// Воркер встал, пока мы ждали места в recs. Без этой ветки отправка
	// висела бы вечно и утащила за собой весь цикл поллинга
	case <-pc.failed:
		s.pause(ctx, cl, pc)
	}
}

// pause останавливает чтение партиции, на которой встал воркер.
//
// Офсет по ней уже не двинется, но брокер продолжает ее отдавать -
// без паузы мы бы качали данные прямо в мусор. Записи из буфера
// отбрасываются и перечитаются после Resume
func (s *splitConsume) pause(ctx context.Context, cl *kgo.Client, pc *pconsumer) {
	cl.PauseFetchPartitions(map[string][]int32{pc.topic: {pc.partition}})

	// ERROR, а не WARN: сама собой партиция не поедет - ребаланс
	// запускает только смена состава группы, а мы для брокера живы
	s.log.LogAttrs(ctx, slog.LevelError, "consumer: partition paused",
		slog.String("topic", pc.topic),
		slog.Int("partition", int(pc.partition)),
		slog.Any("cause", pc.err),
	)
}

func (s *splitConsume) assigned(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
	// Партиция могла остаться на паузе с прошлого владения: тогда на ней встал
	// воркер. Пауза живет в клиенте и ребаланс ее не снимает, а воркер теперь
	// новый - иначе он вечно ждал бы записи, которые клиент не фетчит.
	// Для непаузных партиций это no-op
	cl.ResumeFetchPartitions(assigned)

	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &pconsumer{
				cl:        cl,
				log:       s.log,
				proc:      s.proc,
				topic:     topic,
				partition: partition,

				quit:   make(chan struct{}),
				done:   make(chan struct{}),
				failed: make(chan struct{}),
				recs:   make(chan []*kgo.Record, 5),
			}
			s.consumers[tp{topic, partition}] = pc
			go pc.consume(ctx)
		}
	}
}

// lost гасит воркеров партиций, которые больше не наши, и ждет, пока они
// доработают текущий батч.
//
// Вешается и на revoked, и на lost: разница только в том, пройдет ли коммит.
// При штатном отзыве (revoked) партиция еще наша и коммит успеет,
// при потере (lost) - нет, и батч перечитает новый владелец
func (s *splitConsume) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := tp{topic, partition}

			// Воркер обязан быть: assigned отрабатывает до фетча новых
			// партиций, а lost - до того, как поллинг разрешат снова.
			// Проверка на случай, если это когда-нибудь перестанет быть
			// правдой: паника из callback'а ребаланса убьет процесс
			pc, ok := s.consumers[tp]
			if !ok {
				continue
			}

			delete(s.consumers, tp)
			close(pc.quit)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

// poll блокируется и раздает записи воркерам, пока не отменят ctx
// или не закроют клиент.
//
// Сам ничего не обрабатывает и не коммитит: этим заняты воркеры,
// поэтому цикл не задерживает ребаланс
func (s *splitConsume) poll(ctx context.Context, cl *kgo.Client) {
	for !s.pollOnce(ctx, cl) {
	}
}

// pollOnce - одна итерация: poll -> раздача батчей воркерам -> AllowRebalance.
//
// Вынесено в отдельную функцию ради defer: пока не вызван AllowRebalance,
// ребаланс не начнется (см. kgo.BlockRebalanceOnPoll), поэтому партиции
// не уедут к другому консьюмеру, пока мы раздаем батчи
func (s *splitConsume) pollOnce(ctx context.Context, cl *kgo.Client) (stop bool) {
	// PollRecords, а не PollFetches: при BlockRebalanceOnPoll размер батча
	// надо ограничивать, чтоб воркеры разобрали его быстрее, чем истечет
	// RebalanceTimeout. Верхняя граница на всех: батч может целиком
	// оказаться из одной партиции
	fetches := cl.PollRecords(ctx, s.proc.cfg.MaxPollRecords)

	// Poll регистрирует поллера и блокирует ребаланс, даже если вернул ошибку,
	// поэтому разрешаем ребаланс на любом выходе
	defer cl.AllowRebalance()

	if fetches.IsClientClosed() || ctx.Err() != nil {
		return true
	}

	// Ошибки фетча не фатальны: клиент сам переподключится и перечитает
	fetches.EachError(func(topic string, partition int32, err error) {
		s.log.LogAttrs(ctx, slog.LevelError, "consumer: fetch failed",
			slog.String("topic", topic),
			slog.Int("partition", int(partition)),
			slog.Any("error", err),
		)
	})

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		s.dispatch(ctx, cl, p)
	})

	return false
}
