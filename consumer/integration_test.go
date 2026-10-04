package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/trepach-tech/ecst-go/backoff"
)

// Тесты пула требуют живого брокера: проверять параллельность воркеров,
// ребаланс и паузу партиций на моках бессмысленно - это все поведение Kafka.
//
// Поднять брокер: docker compose up -d
// Запустить:     ECST_TEST_BROKERS=localhost:9092 go test ./consumer/ -run Pool
//
// Без переменной окружения тесты пропускаются, чтоб go test ./... оставался
// зеленым на машине без брокера
const brokersEnv = "ECST_TEST_BROKERS"

func testBrokers(t *testing.T) []string {
	t.Helper()

	brokers := os.Getenv(brokersEnv)
	if brokers == "" {
		t.Skipf("%s is not set, skipping (see the comment in integration_test.go)", brokersEnv)
	}

	// Логи franz-go в тестах только мешают
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	return []string{brokers}
}

// testTopic создает топик с нужным числом партиций и удаляет его после теста.
// Имя уникально, чтоб тесты не наступали друг другу на офсеты
func testTopic(t *testing.T, brokers []string, partitions int32) string {
	t.Helper()

	topic := fmt.Sprintf("ecst-test-%s-%d", t.Name(), time.Now().UnixNano())

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(admin.Close)

	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = partitions
	ct.ReplicationFactor = 1
	create.Topics = append(create.Topics, ct)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := create.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for _, topicResp := range resp.Topics {
		if topicResp.ErrorCode != 0 {
			t.Fatalf("create topic %s: error code %d", topicResp.Topic, topicResp.ErrorCode)
		}
	}

	t.Cleanup(func() {
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = []string{topic}
		dt := kmsg.NewDeleteTopicsRequestTopic()
		dt.Topic = &topic
		del.Topics = append(del.Topics, dt)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		_, _ = del.RequestWith(ctx, admin)
	})

	return topic
}

// produce пишет записи в конкретные партиции: ручной партишенер убирает
// зависимость от хеширования ключей, иначе не проверить поведение
// отдельно взятой партиции
func produce(t *testing.T, brokers []string, recs ...*kgo.Record) {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func rec(topic string, partition int32, key, value string) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       []byte(key),
		Value:     []byte(value),
	}
}

// testConfig - конфиг с быстрыми таймаутами: ждать дефолтных пауз в тестах незачем
func testConfig(brokers []string, topic, group string, dlq DLQ) Config {
	cfg := DefaultConfig(brokers...)
	cfg.Group = group
	cfg.Topics = []string{topic}
	cfg.DLQ = dlq
	cfg.HandlerMaxAttempts = 1
	cfg.DLQMaxAttempts = 1
	cfg.Backoff = backoff.Config{Min: time.Millisecond, Max: time.Millisecond, Factor: 1, Jitter: 0}
	cfg.FetchMaxWait = 50 * time.Millisecond

	return cfg
}

// runConsumer запускает консьюмера в фоне и гасит его по завершении теста
func runConsumer(t *testing.T, cfg Config, handler Handler) *Consumer {
	t.Helper()

	c, err := NewConsumer(cfg, handler)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		c.Close()
		<-done
	})

	return c
}

// Партиции должны обрабатываться параллельно: на каждую свой воркер.
//
// Хендлер не возвращается, пока в нем не окажутся все четыре партиции разом.
// Одна горутина на всех такой барьер не прошла бы - тест повис бы до таймаута
func TestPoolProcessesPartitionsInParallel(t *testing.T) {
	brokers := testBrokers(t)

	const partitions = 4
	topic := testTopic(t, brokers, partitions)

	recs := make([]*kgo.Record, 0, partitions)
	for p := range int32(partitions) {
		recs = append(recs, rec(topic, p, fmt.Sprintf("key-%d", p), "value"))
	}
	produce(t, brokers, recs...)

	var (
		mu      sync.Mutex
		inside  = map[int32]bool{}
		all     = make(chan struct{})
		once    sync.Once
		release = make(chan struct{})
	)

	handler := func(ctx context.Context, r *kgo.Record) error {
		mu.Lock()
		inside[r.Partition] = true
		got := len(inside)
		mu.Unlock()

		if got == partitions {
			once.Do(func() { close(all) })
		}

		// Держим воркера, пока остальные не подтянутся
		select {
		case <-all:
		case <-release:
		case <-ctx.Done():
		}

		return nil
	}

	runConsumer(t, testConfig(brokers, topic, t.Name(), &fakeDLQ{}), handler)

	select {
	case <-all:
	case <-time.After(45 * time.Second):
		close(release)

		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("партиции обрабатывались не параллельно: одновременно в хендлере было %d из %d",
			len(inside), partitions)
	}
}

// Внутри партиции порядок записей должен сохраняться: воркер у партиции один
// и идет по батчу последовательно
func TestPoolKeepsOrderWithinPartition(t *testing.T) {
	brokers := testBrokers(t)

	const (
		partitions = 2
		perPart    = 20
	)
	topic := testTopic(t, brokers, partitions)

	var recs []*kgo.Record
	for p := range int32(partitions) {
		for i := range perPart {
			recs = append(recs, rec(topic, p, fmt.Sprintf("key-%d", i), fmt.Sprintf("%d", i)))
		}
	}
	produce(t, brokers, recs...)

	var (
		mu     sync.Mutex
		last   = map[int32]int64{}
		seen   int
		done   = make(chan struct{})
		closed bool
	)

	handler := func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()

		if prev, ok := last[r.Partition]; ok && r.Offset <= prev {
			t.Errorf("партиция %d: офсет %d пришел после %d", r.Partition, r.Offset, prev)
		}
		last[r.Partition] = r.Offset

		if seen++; seen == partitions*perPart && !closed {
			closed = true
			close(done)
		}

		return nil
	}

	runConsumer(t, testConfig(brokers, topic, t.Name(), &fakeDLQ{}), handler)

	select {
	case <-done:
	case <-time.After(45 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("получено %d записей из %d", seen, partitions*perPart)
	}
}

// Запись, которую хендлер не осилил, уезжает в DLQ, а чтение едет дальше
func TestPoolSendsFailedRecordToDLQ(t *testing.T) {
	brokers := testBrokers(t)

	topic := testTopic(t, brokers, 1)
	produce(t, brokers,
		rec(topic, 0, "ok-1", "good"),
		rec(topic, 0, "bad", "poison"),
		rec(topic, 0, "ok-2", "good"),
	)

	dlq := &fakeDLQ{}
	var (
		mu   sync.Mutex
		good []string
		done = make(chan struct{})
		once sync.Once
	)

	handler := func(_ context.Context, r *kgo.Record) error {
		if string(r.Value) == "poison" {
			return errors.New("boom")
		}

		mu.Lock()
		good = append(good, string(r.Key))
		got := len(good)
		mu.Unlock()

		if got == 2 {
			once.Do(func() { close(done) })
		}

		return nil
	}

	runConsumer(t, testConfig(brokers, topic, t.Name(), dlq), handler)

	select {
	case <-done:
	case <-time.After(45 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("обработано %v, ожидались обе хорошие записи", good)
	}

	if calls := dlq.load(); calls != 1 {
		t.Fatalf("dlq calls = %d, want 1", calls)
	}
}

// Если запись не удалось ни обработать, ни положить в DLQ, встает только
// ее партиция. Остальные продолжают читаться
func TestPoolStallsOnlyBrokenPartition(t *testing.T) {
	brokers := testBrokers(t)

	const partitions = 2
	topic := testTopic(t, brokers, partitions)

	// Партиция 0 отравлена с первой же записи, партиция 1 здорова
	produce(t, brokers,
		rec(topic, 0, "poison", "poison"),
		rec(topic, 0, "after-poison-1", "good"),
		rec(topic, 0, "after-poison-2", "good"),
		rec(topic, 1, "healthy-1", "good"),
		rec(topic, 1, "healthy-2", "good"),
		rec(topic, 1, "healthy-3", "good"),
	)

	var (
		mu      sync.Mutex
		byPart  = map[int32][]string{}
		healthy = make(chan struct{})
		once    sync.Once
	)

	handler := func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		byPart[r.Partition] = append(byPart[r.Partition], string(r.Key))
		got := len(byPart[1])
		mu.Unlock()

		if got == 3 {
			once.Do(func() { close(healthy) })
		}

		if string(r.Value) == "poison" {
			return errors.New("boom")
		}

		return nil
	}

	// DLQ недоступна - деть отравленную запись некуда
	dlq := &fakeDLQ{err: errors.New("dlq is down")}

	runConsumer(t, testConfig(brokers, topic, t.Name(), dlq), handler)

	select {
	case <-healthy:
	case <-time.After(45 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("здоровая партиция не дочиталась: %v", byPart)
	}

	// Даем залипшей партиции время проявить себя, если она все-таки едет
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Воркер встал на первой записи, до следующих дело не дошло
	if got := byPart[0]; len(got) != 1 || got[0] != "poison" {
		t.Fatalf("партиция 0 должна была встать на отравленной записи, обработано: %v", got)
	}
	if got := len(byPart[1]); got != 3 {
		t.Fatalf("партиция 1: обработано %d записей из 3", got)
	}
}

// При подключении второго консьюмера партиции разъезжаются,
// и записи не теряются
func TestPoolRebalancesBetweenConsumers(t *testing.T) {
	brokers := testBrokers(t)

	const partitions = 4
	topic := testTopic(t, brokers, partitions)
	group := t.Name()

	var (
		mu     sync.Mutex
		owners = map[string]map[int32]bool{} // консьюмер -> его партиции
		keys   = map[string]bool{}           // что вообще обработано
	)

	handler := func(name string) Handler {
		return func(_ context.Context, r *kgo.Record) error {
			mu.Lock()
			defer mu.Unlock()

			if owners[name] == nil {
				owners[name] = map[int32]bool{}
			}
			owners[name][r.Partition] = true
			keys[string(r.Key)] = true

			return nil
		}
	}

	// Первый консьюмер забирает все четыре партиции
	runConsumer(t, testConfig(brokers, topic, group, &fakeDLQ{}), handler("a"))

	produce(t, brokers, batch(topic, partitions, "round-1")...)
	waitFor(t, 45*time.Second, "первый консьюмер не забрал все партиции", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(owners["a"]) == partitions
	})

	// Второй входит в ту же группу - часть партиций должна уехать к нему
	runConsumer(t, testConfig(brokers, topic, group, &fakeDLQ{}), handler("b"))

	waitFor(t, 60*time.Second, "партиции не разъехались на второго консьюмера", func() bool {
		// Пишем непрерывно: иначе после ребаланса второму может
		// просто нечего будет читать
		produce(t, brokers, batch(topic, partitions, "round-2")...)

		mu.Lock()
		defer mu.Unlock()
		return len(owners["b"]) > 0
	})

	mu.Lock()
	defer mu.Unlock()

	// Ни одна запись первого раунда не потерялась на ребалансе
	for p := range int32(partitions) {
		key := fmt.Sprintf("round-1-%d", p)
		if !keys[key] {
			t.Errorf("запись %s потеряна", key)
		}
	}
}

func batch(topic string, partitions int32, round string) []*kgo.Record {
	recs := make([]*kgo.Record, 0, partitions)
	for p := range partitions {
		recs = append(recs, rec(topic, p, fmt.Sprintf("%s-%d", round, p), "good"))
	}

	return recs
}

func waitFor(t *testing.T, limit time.Duration, msg string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatal(msg)
}
