package ecst

import (
	"context"
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
	"github.com/trepach-tech/ecst-go/consumer"
	"github.com/trepach-tech/ecst-go/envelope"
)

// Тесты требуют живого брокера: весь смысл модуля - в том, что события
// доезжают из таблицы в хендлер через настоящую Kafka, а на моках проверять
// там нечего.
//
// Поднять брокер: docker compose up -d
// Запустить:     ECST_TEST_BROKERS=localhost:9092 go test ./ecst/ -run Integration
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

	// Логи franz-go и самого модуля в тестах только мешают
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	return []string{brokers}
}

// testTopics создает топики и удаляет их после теста.
// Имена уникальны, чтоб тесты не наступали друг другу на офсеты
func testTopics(t *testing.T, brokers []string, partitions int32, names ...string) []string {
	t.Helper()

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(admin.Close)

	suffix := time.Now().UnixNano()
	topics := make([]string, 0, len(names))

	create := kmsg.NewPtrCreateTopicsRequest()

	for _, name := range names {
		topic := fmt.Sprintf("ecst-test-%s-%s-%d", t.Name(), name, suffix)
		topics = append(topics, topic)

		ct := kmsg.NewCreateTopicsRequestTopic()
		ct.Topic = topic
		ct.NumPartitions = partitions
		ct.ReplicationFactor = 1
		create.Topics = append(create.Topics, ct)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := create.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("create topics: %v", err)
	}
	for _, topicResp := range resp.Topics {
		if topicResp.ErrorCode != 0 {
			t.Fatalf("create topic %s: error code %d", topicResp.Topic, topicResp.ErrorCode)
		}
	}

	t.Cleanup(func() {
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = topics

		for i := range topics {
			dt := kmsg.NewDeleteTopicsRequestTopic()
			dt.Topic = &topics[i]
			del.Topics = append(del.Topics, dt)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		_, _ = del.RequestWith(ctx, admin)
	})

	return topics
}

// roundStore - outbox-таблица в памяти: строки не удаляются, а помечаются,
// как в настоящей таблице с sent_at / failed_at
type roundStore struct {
	mu   sync.Mutex
	rows []storeRow
}

type storeRow struct {
	msg    OutboxMessage
	sent   bool
	failed bool
	cause  error
}

func newRoundStore(msgs ...OutboxMessage) *roundStore {
	s := &roundStore{rows: make([]storeRow, 0, len(msgs))}
	for _, m := range msgs {
		s.rows = append(s.rows, storeRow{msg: m})
	}

	return s
}

func (s *roundStore) Fetch(_ context.Context, limit int) ([]OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := make([]OutboxMessage, 0, limit)

	for _, r := range s.rows {
		if r.sent || r.failed || len(msgs) == limit {
			continue
		}

		msgs = append(msgs, r.msg)
	}

	return msgs, nil
}

func (s *roundStore) MarkSent(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.rows {
		for _, id := range ids {
			if s.rows[i].msg.ID == id {
				s.rows[i].sent = true
			}
		}
	}

	return nil
}

func (s *roundStore) MarkFailed(_ context.Context, id string, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.rows {
		if s.rows[i].msg.ID == id {
			s.rows[i].failed = true
			s.rows[i].cause = cause
		}
	}

	return nil
}

// state отдает состояние строки: отправлена, забракована и почему
func (s *roundStore) state(id string) (sent, failed bool, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.rows {
		if r.msg.ID == id {
			return r.sent, r.failed, r.cause
		}
	}

	return false, false, nil
}

// testOutbox - outbox с короткими паузами: ждать секундного PollInterval
// и пятнадцатисекундной доставки в тестах незачем
func testOutbox(store OutboxStore, brokers []string) *OutboxConfig {
	cfg := DefaultOutboxConfig(store, brokers...)
	cfg.PollInterval = 50 * time.Millisecond
	cfg.StoreMaxAttempts = 1
	cfg.Backoff = fastBackoff()
	cfg.Producer.RecordDeliveryTimeout = 3 * time.Second

	return cfg
}

// testInbox - inbox с короткими паузами и одним консьюмером на топик
func testInbox(group string, brokers []string, regs ...Registration) *InboxConfig {
	cfg := DefaultInboxConfig(group, brokers...)
	cfg.Consumer.HandlerMaxAttempts = 1
	cfg.Consumer.DLQMaxAttempts = 1
	cfg.Consumer.Backoff = fastBackoff()
	cfg.Consumer.FetchMaxWait = 50 * time.Millisecond
	cfg.HandlerTimeout = 10 * time.Second
	cfg.Register(regs...)

	return cfg
}

func fastBackoff() backoff.Config {
	return backoff.Config{Min: time.Millisecond, Max: time.Millisecond, Factor: 1}
}

// runService поднимает сервис в фоне и гасит его по завершении теста
func runService(t *testing.T, cfg Config) *Service {
	t.Helper()

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		svc.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		svc.Close(context.Background())
	})

	return svc
}

func outboxMessage(t *testing.T, id, topic string, version int64) OutboxMessage {
	t.Helper()

	e := envelope.New("order", "order-1", version, envelope.OpUpdate, &order{Sum: 100 * int(version)}).
		WithSource("orders-service", "v1").
		WithTraceID(fmt.Sprintf("trace-%d", version))

	msg, err := NewOutboxMessage(id, topic, e)
	if err != nil {
		t.Fatalf("new outbox message: %v", err)
	}

	return msg
}

// Полный круг ECST: событие лежит в таблице, outbox публикует его в Kafka,
// inbox отдает его хендлеру уже разобранным конвертом
func TestIntegrationOutboxToInbox(t *testing.T) {
	brokers := testBrokers(t)
	topic := testTopics(t, brokers, 1, "orders")[0]

	const events = 5

	msgs := make([]OutboxMessage, 0, events)
	for i := 1; i <= events; i++ {
		msgs = append(msgs, outboxMessage(t, fmt.Sprintf("row-%d", i), topic, int64(i)))
	}

	store := newRoundStore(msgs...)
	got := make(chan envelope.Envelope[order], events)

	runService(t, Config{
		Outbox: testOutbox(store, brokers),
		Inbox: testInbox(t.Name(), brokers, Topic(topic, 1,
			func(_ context.Context, e envelope.Envelope[order]) error {
				got <- e

				return nil
			},
		)),
	})

	// Версии должны приехать по порядку: ключ записи - EntityID,
	// поэтому все события сущности лежат в одной партиции
	for want := int64(1); want <= events; want++ {
		select {
		case e := <-got:
			if e.Version != want {
				t.Fatalf("version = %d, want %d", e.Version, want)
			}
			if e.EntityID != "order-1" {
				t.Fatalf("entity id = %q, want %q", e.EntityID, "order-1")
			}
			if e.Payload == nil || e.Payload.Sum != 100*int(want) {
				t.Fatalf("payload = %+v, want sum %d", e.Payload, 100*int(want))
			}
			if e.Source.Service != "orders-service" || e.TraceID != fmt.Sprintf("trace-%d", want) {
				t.Fatalf("envelope metadata lost: %+v", e)
			}
		case <-time.After(45 * time.Second):
			t.Fatalf("событие версии %d не доехало до хендлера", want)
		}
	}

	// Строка помечается отправленной только после подтверждения брокером
	waitFor(t, 15*time.Second, "строки не отмечены отправленными", func() bool {
		for i := 1; i <= events; i++ {
			sent, _, _ := store.state(fmt.Sprintf("row-%d", i))
			if !sent {
				return false
			}
		}

		return true
	})
}

// Строку, которая не собирается в запись, публиковать некуда: она уезжает
// в MarkFailed, а батч едет дальше
func TestIntegrationOutboxMarksBrokenRowFailed(t *testing.T) {
	brokers := testBrokers(t)
	topic := testTopics(t, brokers, 1, "orders")[0]

	// У битой строки нет топика: конверт валиден, а публиковать некуда
	broken := outboxMessage(t, "row-broken", topic, 1)
	broken.Topic = ""

	good := outboxMessage(t, "row-good", topic, 2)

	store := newRoundStore(broken, good)
	got := make(chan envelope.Envelope[order], 1)

	runService(t, Config{
		Outbox: testOutbox(store, brokers),
		Inbox: testInbox(t.Name(), brokers, Topic(topic, 1,
			func(_ context.Context, e envelope.Envelope[order]) error {
				got <- e

				return nil
			},
		)),
	})

	select {
	case e := <-got:
		// Битая строка не остановила здоровую
		if e.Version != 2 {
			t.Fatalf("version = %d, want 2", e.Version)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("здоровое событие не доехало до хендлера")
	}

	waitFor(t, 15*time.Second, "битая строка не помечена failed", func() bool {
		_, failed, cause := store.state("row-broken")

		return failed && cause != nil
	})

	if sent, _, _ := store.state("row-broken"); sent {
		t.Fatal("битая строка помечена отправленной: событие потерялось бы молча")
	}
}

// Событие, которое не разбирается, уезжает в DLQ-топик и не встает
// поперек партиции: следующие события обрабатываются как обычно
func TestIntegrationInboxSendsBrokenEventToDLQ(t *testing.T) {
	brokers := testBrokers(t)

	topics := testTopics(t, brokers, 1, "orders", "orders.dlq")
	topic, dlqTopic := topics[0], topics[1]

	inbox := testInbox(t.Name(), brokers, Topic(topic, 1,
		func(_ context.Context, e envelope.Envelope[order]) error {
			return nil
		},
	))
	// Имя DLQ-топика по умолчанию - topic + ".dlq", но имена в тестах
	// уникальны, поэтому указываем явно
	inbox.DLQTopic = func(string) string { return dlqTopic }

	store := newRoundStore(outboxMessage(t, "row-1", topic, 1))

	// Битое событие пишем напрямую, минуя outbox: он валидный конверт
	// в таблицу и не пустит
	produceRaw(t, brokers, &kgo.Record{Topic: topic, Key: []byte("order-1"), Value: []byte("not an envelope")})

	runService(t, Config{
		Outbox: testOutbox(store, brokers),
		Inbox:  inbox,
	})

	// В DLQ приехала та самая запись, с метаданными происхождения
	rec := readOne(t, brokers, dlqTopic, 45*time.Second)

	if string(rec.Value) != "not an envelope" {
		t.Fatalf("dlq value = %q, want %q", rec.Value, "not an envelope")
	}

	headers := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}

	if headers[consumer.HeaderDLQTopic] != topic {
		t.Errorf("%s = %q, want %q", consumer.HeaderDLQTopic, headers[consumer.HeaderDLQTopic], topic)
	}
	if headers[consumer.HeaderDLQError] == "" {
		t.Errorf("%s is empty, want the reason the record failed", consumer.HeaderDLQError)
	}

	// Партиция не встала: событие из outbox доехало после битого
	waitFor(t, 30*time.Second, "партиция встала на битом событии", func() bool {
		sent, _, _ := store.state("row-1")

		return sent
	})
}

// produceRaw пишет запись мимо модуля: так в топик попадает то,
// что модуль сам бы не выпустил
func produceRaw(t *testing.T, brokers []string, recs ...*kgo.Record) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
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

// readOne ждет первую запись в топике
func readOne(t *testing.T, brokers []string, topic string, limit time.Duration) *kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()

	for {
		fetches := cl.PollRecords(ctx, 1)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("poll %s: %v", topic, err)
		}

		if recs := fetches.Records(); len(recs) > 0 {
			return recs[0]
		}
	}
}

func waitFor(t *testing.T, limit time.Duration, msg string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal(msg)
}

// Строка, которую брокер не принял, не тянет за собой остальные:
// доехавшие отмечаются отправленными и повторно не публикуются.
//
// Раньше батч сводился к первой ошибке, и одна такая строка заставляла
// перепубликовывать весь батч - дубли у потребителя на каждом заходе
func TestIntegrationOutboxUndeliveredRowDoesNotBlockBatch(t *testing.T) {
	brokers := testBrokers(t)
	topic := testTopics(t, brokers, 1, "orders")[0]

	// Топик, которого нет: запись в него не доедет, но это преходящая
	// причина - топик может появиться, поэтому строка остается в таблице
	missing := outboxMessage(t, "row-missing", topic, 1)
	missing.Topic = topic + "-does-not-exist"

	good := outboxMessage(t, "row-good", topic, 2)

	store := newRoundStore(missing, good)
	got := make(chan envelope.Envelope[order], 8)

	runService(t, Config{
		Outbox: testOutbox(store, brokers),
		Inbox: testInbox(t.Name(), brokers, Topic(topic, 1,
			func(_ context.Context, e envelope.Envelope[order]) error {
				got <- e

				return nil
			},
		)),
	})

	select {
	case e := <-got:
		if e.Version != 2 {
			t.Fatalf("version = %d, want 2", e.Version)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("здоровое событие не доехало до хендлера")
	}

	// Доехавшая строка отмечена, недоехавшая - нет: она поедет снова
	waitFor(t, 15*time.Second, "доехавшая строка не отмечена отправленной", func() bool {
		sent, _, _ := store.state("row-good")

		return sent
	})

	if sent, failed, _ := store.state("row-missing"); sent || failed {
		t.Fatalf("недоехавшая строка: sent = %v, failed = %v, want false, false", sent, failed)
	}

	// Событие не приезжает повторно: его строка больше не публикуется
	select {
	case e := <-got:
		t.Fatalf("событие версии %d приехало повторно", e.Version)
	case <-time.After(5 * time.Second):
	}
}
