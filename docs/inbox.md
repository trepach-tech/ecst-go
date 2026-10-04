# Inbox и обработка

## Регистрация

```go
inbox := ecst.DefaultInboxConfig("orders-consumer", "localhost:9092")

inbox.Register(
    ecst.Topic("orders", 2, handleOrder),
    ecst.Topic("users", 0, handleUser),        // 0 → InboxConfig.PoolSize
    ecst.RawTopic("cdc.public.legacy", 1, raw),
)
```

`Topic[T]` разбирает конверт за хендлер; `RawTopic` отдает запись как есть — для топиков, в которых
лежит не `envelope.Envelope`: чужой сервис, legacy-формат, CDC от Debezium.

Повторная регистрация топика перетирает предыдущую. Тип payload параметризует только сборку
регистрации, а не inbox: в одном inbox спокойно живут топики с разными payload.

На каждый топик поднимается пул из `poolSize` консьюмеров в **одной группе** — они делят между собой
партиции топика. Больше числа партиций смысла не имеет: лишние простаивают.

У каждого топика своя DLQ — по умолчанию `<topic>.dlq`. Переопределяется:

```go
inbox.DLQTopic = func(topic string) string { return "dlq." + topic }
```

## Хендлер

```go
type Handler[T any] func(ctx context.Context, e envelope.Envelope[T]) error
```

Три требования:

**Идемпотентность.** Одно и то же событие может приехать повторно (at-least-once). `Version` для
этого и нужна — по ней отбрасывают уже примененное.

**Безопасность для конкурентного вызова.** Хендлер вызывается по горутине на партицию. В пределах
одной партиции события идут строго по порядку — а значит, и события одной сущности, раз ключ это
`EntityID`.

**Уважение к `ctx`.** На хендлер стоит `HandlerTimeout`, но сам по себе таймаут его не прервет:
обязателен `ctx` в запросах к БД. Зависший хендлер держит свою партицию, а заодно и ребаланс.

Топик, партиция и офсет достаются из `ctx`:

```go
if r, ok := ecst.RecordFrom(ctx); ok {
    slog.Info("applied", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
}
```

## Применение события

Канонический способ — условный upsert по версии:

```go
func handleOrder(ctx context.Context, e envelope.Envelope[Order]) error {
    if e.Op == envelope.OpDelete {
        // tombstone, а не DELETE: иначе старое событие воскресит запись
        _, err := db.ExecContext(ctx, `
            INSERT INTO orders (id, version, deleted) VALUES ($1, $2, true)
            ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, deleted = true
            WHERE orders.version < EXCLUDED.version`,
            e.EntityID, e.Version)
        return err
    }

    _, err := db.ExecContext(ctx, `
        INSERT INTO orders (id, sum, version, deleted) VALUES ($1, $2, $3, false)
        ON CONFLICT (id) DO UPDATE
        SET sum = EXCLUDED.sum, version = EXCLUDED.version, deleted = false
        WHERE orders.version < EXCLUDED.version`,
        e.EntityID, e.Payload.Sum, e.Version)
    return err
}
```

Проверка версии и запись атомарны — гонок между воркерами нет. `0 rows` — это дубль или устаревшее
событие: **не ошибка**, возвращать `nil` и коммитить.

## Что происходит с ошибкой хендлера

```
handler error
  ├─ errors.Is(err, consumer.ErrPermanent) ──> DLQ сразу, без ретраев
  └─ иначе ──> ретраи (HandlerMaxAttempts) с бэкоффом
                 ├─ прошло ──> commit
                 └─ не прошло ──> DLQ (DLQMaxAttempts)
                                    ├─ записана ──> commit
                                    └─ нет ──> воркер встает,
                                               партиция на паузе, без commit
```

Разделять ошибки обязательно: битый JSON ретраить бессмысленно, а сбой БД отправлять в DLQ —
значит слить в нее весь поток, пока база лежит.

```go
// не лечится повтором
if err := json.Unmarshal(r.Value, &v); err != nil {
    return fmt.Errorf("parse: %w: %w", err, consumer.ErrPermanent)
}
if errors.Is(err, errConstraintViolation) {
    return fmt.Errorf("apply: %w: %w", err, consumer.ErrPermanent)
}

// лечится повтором — отдаем как есть
return fmt.Errorf("db: %w", err)
```

`ecst.Topic` уже заворачивает в `ErrPermanent` все ошибки разбора: битый JSON, конверт без
обязательных полей, payload чужой схемы.

## Остановка партиции

Если запись не удалось ни обработать, ни положить в DLQ, воркер встает: батч не коммитится.
Коммитить дальше нельзя — `CommitRecords` двигает офсет на максимум из батча и перепрыгнул бы эту
запись, то есть потерял бы ее.

Цикл поллинга замечает вставшего воркера и вызывает `PauseFetchPartitions`: офсет по партиции уже
не двинется, а брокер продолжает ее отдавать — без паузы данные качались бы прямо в мусор.

В лог уходит `ERROR` `consumer: partition paused`. Сама собой партиция не поедет: ребаланс запускает
только смена состава группы, а для брокера консьюмер жив (heartbeat идет из клиента). Лечится
рестартом инстанса или починкой DLQ. **Это событие стоит мониторить.**

Остальные партиции при этом читаются как ни в чем не бывало.

## Ребаланс

`BlockRebalanceOnPoll` + `AllowRebalance` после раздачи батчей: ребаланс не начнется, пока мы
раздаем записи, — партиции не уедут к другому консьюмеру посреди раздачи.

При отзыве партиции (`revoked`/`lost`) воркер получает `quit`, бросает текущий батч недоделанным и
завершается; `lost` дожидается всех таких воркеров. Брошенный батч не коммитится и перечитается
новым владельцем.

Разница между `revoked` и `lost` только в том, пройдет ли коммит: при штатном отзыве партиция еще
наша и воркер успеет закоммитить доработанное, при потере — нет.

`RebalanceTimeout` должен быть больше времени обработки одного батча, иначе группа выпадет в
бесконечный ребаланс. Регулировать через `MaxPollRecords` — это же размер батча между коммитами.

## DLQ

```go
dlq, err := consumer.NewKafkaDLQ(producer, "orders.dlq")
```

Отправка синхронная: вернуться раньше, чем брокер подтвердил запись, нельзя — консьюмер по возврату
сразу коммитит офсет исходной записи.

К записи добавляются заголовки происхождения:

| Заголовок | Значение |
|---|---|
| `dlq_source_topic` | исходный топик |
| `dlq_source_partition` | исходная партиция |
| `dlq_source_offset` | исходный офсет |
| `dlq_error` | текст ошибки |
| `dlq_time` | момент отправки, RFC3339 |

Исходные заголовки записи сохраняются — `entity_type` и `trace_id` остаются на месте.

Интерфейс свой, если DLQ нужна не в Kafka:

```go
type DLQ interface {
    Send(ctx context.Context, r *kgo.Record, cause error) error
}
```

`Send` возвращается без ошибки **только когда запись действительно сохранена**: сразу после этого
консьюмер коммитит офсет и запись больше не приедет.

DLQ закрывает тот, кто ее создал — консьюмер этого не делает.
