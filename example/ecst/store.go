package main

import (
	"context"
	"log/slog"
	"slices"
	"sync"

	"github.com/trepach-tech/ecst-go/ecst"
)

// memStore - реализация [ecst.OutboxStore] в памяти: хватает, чтоб увидеть
// весь цикл, но не переживает рестарт.
//
// В боевом коде это таблица с колонками id, topic, payload, sent_at, failed_at
// и Fetch вида
//
//	SELECT ... WHERE sent_at IS NULL AND failed_at IS NULL
//	ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED
//
// FOR UPDATE SKIP LOCKED обязателен: без него несколько инстансов сервиса
// опубликуют одни и те же события
type memStore struct {
	mu   sync.Mutex
	rows []row
}

// row - строка таблицы. Отправленные и битые строки не удаляются, а помечаются:
// так видно, что с событием стало
type row struct {
	msg    ecst.OutboxMessage
	sent   bool
	failed bool
}

func newMemStore() *memStore {
	return &memStore{}
}

// Add изображает вставку события в таблицу в транзакции бизнес-логики
func (s *memStore) Add(msg ecst.OutboxMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rows = append(s.rows, row{msg: msg})
}

// Fetch отдает неотправленные строки в порядке вставки: порядок событий
// одной сущности должен сохраняться
func (s *memStore) Fetch(_ context.Context, limit int) ([]ecst.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := make([]ecst.OutboxMessage, 0, limit)

	for _, r := range s.rows {
		if r.sent || r.failed {
			continue
		}

		if len(msgs) == limit {
			break
		}

		msgs = append(msgs, r.msg)
	}

	return msgs, nil
}

// MarkSent вызывается только после подтверждения записи брокером
func (s *memStore) MarkSent(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.rows {
		if slices.Contains(ids, s.rows[i].msg.ID) {
			s.rows[i].sent = true
		}
	}

	return nil
}

// MarkFailed уводит с дороги строку, которую не вылечит ретрай.
//
// Без этой отметки она вечно возвращалась бы из Fetch, а отметить ее
// отправленной нельзя - событие потерялось бы молча
func (s *memStore) MarkFailed(_ context.Context, id string, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.rows {
		if s.rows[i].msg.ID == id {
			s.rows[i].failed = true
		}
	}

	slog.Error("outbox row failed", "id", id, "cause", cause)

	return nil
}
