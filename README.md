<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.png">
    <img src="docs/assets/logo.png" alt="ecst-go" width="320">
  </picture>
</p>

# ecst-go

Библиотека для Event Carried State Transfer поверх Kafka ([franz-go](https://github.com/twmb/franz-go)).

ECST — это способ отдавать изменения состояния так, чтобы получатель мог собрать свою копию данных и
больше ни о чем не спрашивать источник. В событии едет **полное состояние сущности**, а не дельта, и
**версия**, по которой получатель отбрасывает то, что уже применил.

Библиотека закрывает транспортную часть: гарантированную публикацию из outbox-таблицы, параллельное
чтение с ретраями и DLQ, типизированный конверт событий.

```
бизнес-логика ──tx──> outbox-таблица ──outbox worker──> Kafka ──inbox worker──> handler ──> своя БД
```

---

## Установка

```bash
go get github.com/giicoo/ecst-go
```

Go 1.27+.

## Содержание

| Пакет | Зачем |
|---|---|
| [`ecst`](#пакет-ecst) | Готовый сервис: outbox-воркер + inbox-воркер, типизированные хендлеры |
| [`envelope`](#пакет-envelope) | Конверт события: `entity_id`, `version`, `op`, `payload`, `source` |
| [`producer`](#пакет-producer) | Обертка над `kgo` для публикации: acks, идемпотентность, батчи |
| [`consumer`](#пакет-consumer) | Чтение группой: воркер на партицию, ретраи, DLQ, пауза партиции |
| [`backoff`](#пакет-backoff) | Экспоненциальная задержка с джиттером и `Retry` |

Подробная документация — в [`docs/`](docs/):

- [Архитектура](docs/architecture.md)
- [Outbox](docs/outbox.md)
- [Inbox и обработка](docs/inbox.md)
- [Гарантии доставки и отказы](docs/reliability.md)
- [Конфигурация](docs/configuration.md)
- [Диаграммы](docs/architecture.md#диаграммы)

![компоненты и потоки](docs/diagrams/overview.svg)

---

## Быстрый старт

Поднять брокер и создать топики:

```bash
docker compose up -d
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 3
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.dlq --partitions 3
```

Топики нужны заранее: автосоздание у брокера выключено.

### Сервис, который и публикует, и потребляет

```go
type Order struct {
    Sum int `json:"sum"`
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    store := myOutboxStore() // ваша реализация ecst.OutboxStore поверх вашей БД

    inbox := ecst.DefaultInboxConfig("orders-consumer", "localhost:9092")
    inbox.Register(
        ecst.Topic("orders", 2, handleOrder), // 2 консьюмера на топик
    )

    svc, err := ecst.New(ecst.Config{
        Outbox: ecst.DefaultOutboxConfig(store, "localhost:9092"),
        Inbox:  inbox,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer svc.Close(ctx)

    svc.Run(ctx) // блокируется до отмены ctx
}

// Хендлер получает уже разобранный конверт.
// Обязан быть идемпотентным: одно и то же событие может приехать повторно
func handleOrder(ctx context.Context, e envelope.Envelope[Order]) error {
    if e.Op == envelope.OpDelete {
        return deleteOrder(ctx, e.EntityID, e.Version)
    }
    return upsertOrder(ctx, e.EntityID, e.Version, e.Payload.Sum)
}
```

Обе части опциональны: только `Outbox` — чистый производитель, только `Inbox` — чистый потребитель.

### Запись события в outbox

Событие кладется в таблицу **той же транзакцией**, что и изменение самой сущности — в этом весь смысл
outbox: состояние и факт его изменения коммитятся атомарно.

```go
e := envelope.New("order", orderID, version, envelope.OpUpdate, &Order{Sum: 100}).
    WithSource("orders-service", "v1").
    WithTraceID(traceID)

msg, err := ecst.NewOutboxMessage(rowID, "orders", e) // валидирует конверт
if err != nil {
    return err
}

return tx.InsertOutbox(ctx, msg) // в одной транзакции с UPDATE orders
```

---

## Пакет `ecst`

Собирает сервис из двух независимых частей ([`ecst.Config`](ecst/config.go)):

**`Outbox`** — воркер, который циклом забирает строки из вашей таблицы (`OutboxStore.Fetch`),
публикует их синхронно и отмечает отправленными (`MarkSent`) **только после ack брокера**. Строки,
которые брокер отверг окончательно (слишком большое сообщение, отказ авторизации, битый конверт),
уезжают в `MarkFailed` — повтор их не вылечит. Остальные остаются неотмеченными и приедут в следующем
заходе.

**`Inbox`** — на каждый зарегистрированный топик поднимается свой пул консьюмеров со своей DLQ.
Размер пула настраивается на топик: у разных потоков событий разный объем и разная цена обработки.

Регистрация хендлеров:

```go
inbox.Register(
    ecst.Topic("orders", 2, handleOrder),       // конверт разбирается за вас
    ecst.Topic("users", 0, handleUser),         // 0 — взять InboxConfig.PoolSize
    ecst.RawTopic("debezium.public.x", 1, raw), // сырая запись: чужой формат, CDC, legacy
)
```

Топик, партиция и офсет в конверте не лежат — их достают из `ctx`, когда нужны:

```go
if r, ok := ecst.RecordFrom(ctx); ok {
    slog.Info("applied", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
}
```

## Пакет `envelope`

```go
type Envelope[T any] struct {
    EntityType string    // "order"
    EntityID   string    // ключ записи в Kafka → порядок событий сущности
    Version    int64     // > 0, растет с каждым изменением
    Op         Op        // c | u | d | r
    Payload    *T        // полное состояние; у Delete отсутствует
    Timestamp  time.Time
    TraceID    string
    Source     Source    // кто отправил и по какой схеме
}
```

`Validate()` возвращает **все** проблемы разом и вызывается автоматически при `Encode`, `Decode`,
`ToRaw`, `FromRaw` — битый конверт не доедет ни до таблицы, ни до брокера, ни до хендлера.

`envelope.Raw` (псевдоним `Envelope[json.RawMessage]`) — конверт с неразобранным payload. Нужен там,
где тип payload еще неизвестен: в outbox-таблице лежат события разных сущностей. `Decode`
с `json.RawMessage` отдает именно его, а `FromRaw[T]` доразбирает payload, когда тип стал известен.

## Пакет `producer`

| Метод | Когда |
|---|---|
| `Produce` | fire-and-forget, результат только в лог. Контекст отвязан от отмены, чтоб shutdown не обрубил отправку |
| `ProduceSync` | нельзя идти дальше, пока запись не сохранена (DLQ) |
| `ProduceSyncResults` | нужна судьба **каждой** записи батча (outbox: отмечать отправленными только доехавшие) |
| `Permanent(err)` | запись не доедет и на повторе: `MESSAGE_TOO_LARGE`, `INVALID_RECORD`, отказ авторизации |

По умолчанию `acks=all` + идемпотентная запись. `AcksLeader`/`AcksNone` выключают идемпотентность
(требование Kafka) — быстрее, но запись теряется при падении лидера.

## Пакет `consumer`

Чтение группой, по горутине на партицию. Цикл поллинга только раздает батчи воркерам и потому не
задерживает ребаланс; воркер сам обрабатывает свои записи и сам коммитит свой батч.

```go
cfg := consumer.DefaultConfig("localhost:9092")
cfg.Group = "orders-consumer"
cfg.Topics = []string{"orders"}
cfg.DLQ = dlq // обязательна

c, err := consumer.NewConsumer(cfg, handler)
defer c.Close()
c.Run(ctx)
```

Ошибка хендлера → ретраи с бэкоффом (`HandlerMaxAttempts`) → DLQ (`DLQMaxAttempts`) → коммит.
`consumer.ErrPermanent` в цепочке ошибки обрывает ретраи сразу:

```go
if err := json.Unmarshal(r.Value, &v); err != nil {
    return fmt.Errorf("parse: %w: %w", err, consumer.ErrPermanent)
}
```

`KafkaDLQ` добавляет к записи заголовки `dlq_source_topic`, `dlq_source_partition`, `dlq_source_offset`,
`dlq_error`, `dlq_time` — по ним сообщение находят и переигрывают.

## Пакет `backoff`

```go
backoff.Retry{
    Config:   backoff.Default(), // 250ms → ×2 → потолок 5s, full jitter
    Attempts: 3,
    OnRetry:  func(attempt int, err error) { slog.Warn("retry", "attempt", attempt, "error", err) },
}.Do(ctx, op)
```

`backoff.ErrPermanent` в цепочке ошибки обрывает повторы сам, без всякой настройки —
`consumer.ErrPermanent` это он же. Поле `Permanent` нужно только для чужих ошибок, которые про
сентинел не знают (например `producer.Permanent` для окончательных отказов брокера).

Джиттер нужен, чтоб инстансы, упавшие на одной и той же ошибке, не пошли ретраить одновременно.

---

## Гарантии

**At-least-once в обе стороны.** Офсет коммитится только после того, как запись обработана или
положена в DLQ; строка outbox отмечается только после ack брокера. Падение между ack и `MarkSent`
даст дубль — поэтому **хендлер обязан быть идемпотентным**.

Способ быть идемпотентным — условный upsert по версии:

```sql
INSERT INTO orders (id, sum, version, deleted)
VALUES ($1, $2, $3, false)
ON CONFLICT (id) DO UPDATE
SET sum = EXCLUDED.sum, version = EXCLUDED.version, deleted = EXCLUDED.deleted
WHERE orders.version < EXCLUDED.version;
```

Проверка версии и запись атомарны — гонок между двумя воркерами нет. `0 rows` означает дубль или
устаревшее событие: это не ошибка, офсет надо коммитить.

Delete — это tombstone (`deleted = true` + версия), а не `DELETE FROM`: иначе старое событие,
приехавшее повторно, воскресит удаленную запись.

| Правило | Зачем |
|---|---|
| `key = entity_id` | порядок событий сущности (одна партиция) |
| `version` в каждом событии | получатель отбрасывает то, что уже применил |
| запись в outbox в транзакции с изменением | состояние и событие коммитятся атомарно |
| `FOR UPDATE SKIP LOCKED` в `Fetch` | несколько инстансов не опубликуют одно и то же |
| `MarkSent` только после ack | событие не теряется |
| коммит только после ack от DLQ | сообщение не теряется |
| в DLQ только то, что не лечится повтором | при сбое БД поток не сливается в DLQ |
| пауза партиции, если и DLQ недоступна | офсет не перепрыгивает необработанную запись |

Подробнее — [docs/reliability.md](docs/reliability.md).

## Примеры

| Пример | Что показывает |
|---|---|
| [`example/basic`](example/basic) | продюсер + консьюмер на сырых записях; битая запись уезжает в DLQ и не останавливает поток |
| [`example/ecst`](example/ecst) | полный цикл: outbox-таблица → Kafka → типизированные хендлеры |

Автосоздание топиков у брокера выключено — примерам нужно заданное число партиций,
поэтому топики создаются заранее (команды есть в шапке каждого примера):

```bash
docker compose up -d

for t in orders orders.dlq users users.dlq; do
  docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 3
done

go run ./example/ecst
```

## Диаграммы

Полный путь события со всеми ветками отказов:

![retry publish / retry consume](docs/diagrams/publish-consume.svg)

Исходники и команда рендера — в [docs/architecture.md](docs/architecture.md#диаграммы).

## Лицензия

[MIT](LICENSE)
