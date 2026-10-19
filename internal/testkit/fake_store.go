package testkit

import (
	"context"
	"sync"

	"github.com/trepach-tech/ecst-go/outbox"
)

// TestStore - [OutboxStore], который считает вызовы и умеет падать
type TestStore struct {
	mu sync.Mutex

	msgs []outbox.OutboxMessage

	fetchErr      error
	markSentErr   error
	markFailedErr error

	fetchCalls      int
	markSentCalls   int
	markFailedCalls int

	sentIDs    []string
	failedIDs  []string
	failCauses []error
}

func (f *TestStore) Fetch(_ context.Context, limit int) ([]outbox.OutboxMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.fetchCalls++

	if f.fetchErr != nil {
		return nil, f.fetchErr
	}

	if limit > len(f.msgs) {
		limit = len(f.msgs)
	}

	return f.msgs[:limit], nil
}

func (f *TestStore) MarkSent(_ context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.markSentCalls++

	if f.markSentErr != nil {
		return f.markSentErr
	}

	f.sentIDs = append(f.sentIDs, ids...)

	return nil
}

func (f *TestStore) MarkFailed(_ context.Context, id string, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.markFailedCalls++

	if f.markFailedErr != nil {
		return f.markFailedErr
	}

	f.failedIDs = append(f.failedIDs, id)
	f.failCauses = append(f.failCauses, cause)

	return nil
}
