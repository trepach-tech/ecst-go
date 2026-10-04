# Outbox

Outbox-воркер перекладывает строки из вашей таблицы в Kafka. Таблицу и доступ к ней пишете вы —
только ваше приложение знает свою схему и свою БД.

## Контракт `OutboxStore`

```go
type OutboxStore interface {
    Fetch(ctx context.Context, limit int) ([]OutboxMessage, error)
    MarkSent(ctx context.Context, ids []string) error
    MarkFailed(ctx context.Context, id string, cause error) error
}
```

### `Fetch`

Отдает до `limit` неотправленных сообщений **в порядке их записи** — порядок событий одной сущности
должен сохраняться.

Обязан блокировать выбранные строки, иначе несколько инстансов сервиса опубликуют одни и те же
события:

```sql
SELECT id, topic, payload
FROM outbox
WHERE sent_at IS NULL AND failed_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED;
```

### `MarkSent`

Вызывается **только после подтверждения записи брокером**. Если упадет — сообщения уедут в Kafka
повторно, поэтому потребитель обязан быть идемпотентным.

Ретраи здесь особенно важны: незакоммиченная отметка — это дубли на стороне потребителя, а не просто
отложенный цикл. Поэтому контекст отметки отвязан от отмены (`context.WithoutCancel`): shutdown
не должен оставить доехавшие строки неотмеченными.

### `MarkFailed`

Уводит с дороги строку, которую не вылечит повтор: битый конверт, пустой топик, `MESSAGE_TOO_LARGE`,
отказ авторизации.

Обязателен. Без него битая строка вечно возвращалась бы из `Fetch`, а отметить ее отправленной
нельзя — событие потерялось бы молча. Реализуют флагом `failed_at` + `cause`, отдельной
таблицей-DLQ или счетчиком попыток; главное, чтоб `Fetch` такие строки больше не отдавал.

## Схема таблицы

```sql
CREATE TABLE outbox (
    id         bigserial PRIMARY KEY,
    topic      text        NOT NULL,
    entity_id  text        NOT NULL,
    payload    jsonb       NOT NULL,  -- envelope.Raw целиком
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz,
    failed_at  timestamptz,
    cause      text
);

CREATE INDEX outbox_pending ON outbox (id)
    WHERE sent_at IS NULL AND failed_at IS NULL;
```

Частичный индекс важен: таблица растет, а разбирать надо только непубликованный хвост.

## Запись события

Той же транзакцией, что и изменение сущности:

```go
func (s *Service) UpdateOrder(ctx context.Context, id string, sum int) error {
    return s.db.InTx(ctx, func(tx *sql.Tx) error {
        version, err := bumpOrder(ctx, tx, id, sum) // UPDATE ... RETURNING version
        if err != nil {
            return err
        }

        e := envelope.New("order", id, version, envelope.OpUpdate, &Order{Sum: sum}).
            WithSource("orders-service", "v1").
            WithTraceID(trace.FromContext(ctx))

        msg, err := ecst.NewOutboxMessage(uuid.NewString(), "orders", e)
        if err != nil {
            return err // битый конверт до таблицы не доедет
        }

        return insertOutbox(ctx, tx, msg)
    })
}
```

`ecst.NewOutboxMessage` валидирует конверт на месте: лучше поймать проблему здесь, чем в каждом
потребителе.

Delete едет без payload — состояния у удаленной сущности нет:

```go
e := envelope.New[Order]("order", id, version, envelope.OpDelete, nil).
    WithSource("orders-service", "v1")
```

## Цикл воркера

Один заход (`BatchTimeout` на весь заход, вместе с ретраями к БД):

1. **`Fetch`** с ретраями (`StoreMaxAttempts`, бэкофф). Пусто — пауза `PollInterval`.
2. **Сборка записей.** Ключ записи — `EntityID`: так все события одной сущности ложатся в одну
   партицию и приезжают потребителю в порядке версий. Заголовки — `entity_type` и `trace_id`:
   по ним запись фильтруют и трассируют, не декодируя payload. Строка, из которой запись не
   собирается, уезжает в `MarkFailed` и батч не останавливает.
3. **`ProduceSyncResults`** — синхронно и с результатом по каждой записи. Сводить батч к первой
   ошибке нельзя: это значило бы перепубликовывать доехавшие.
4. **Разбор результатов.** Ack → в список на `MarkSent`. `producer.Permanent` → `MarkFailed`.
   Остальное (таймаут, недоступный брокер, неизвестный пока топик) → строка остается неотмеченной
   и приедет в следующем заходе.
5. **`MarkSent`** с ретраями, контекст отвязан от отмены.

Если батч набрался целиком (`len == BatchSize`), следующий заход идет **без паузы** — в таблице
скорее всего есть еще.

Ошибка захода воркера не роняет: строки остались неотмеченными и приедут в следующем заходе.

## Несколько инстансов

`FOR UPDATE SKIP LOCKED` в `Fetch` — единственное, что нужно: инстансы разбирают непересекающиеся
куски хвоста. Порядок внутри сущности при этом сохраняется, если события одной сущности пишутся
последовательно (каждое после коммита предыдущего) — что и происходит, когда сущность меняется
под блокировкой своей строки.

## Публикация мимо таблицы

```go
p := svc.OutboxProducer() // nil, если Outbox не подключен
p.Produce(ctx, &kgo.Record{Topic: "ops", Value: payload})
```

Нужно для служебных топиков и тестов. Для доменных событий — только через таблицу: иначе теряется
атомарность, ради которой outbox и существует.
