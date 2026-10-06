package backoff

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// fast - бэкофф, на котором тесты не спят: проверяется логика повторов,
// а не длительность задержки
func fast() Config {
	return Config{Min: time.Microsecond, Max: time.Microsecond, Factor: 1}
}

// failing возвращает операцию, которая падает первые n раз,
// и счетчик ее вызовов
func failing(n int, err error) (op func() error, calls *int) {
	calls = new(int)

	return func() error {
		*calls++

		if *calls <= n {
			return err
		}

		return nil
	}, calls
}

func TestRetryFirstAttempt(t *testing.T) {
	op, calls := failing(0, errors.New("boom"))

	r := Retry{Config: fast(), Attempts: 3}
	if err := r.Do(context.Background(), op); err != nil {
		t.Fatalf("do: %v", err)
	}

	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	op, calls := failing(2, errors.New("boom"))

	var retries int

	r := Retry{
		Config:   fast(),
		Attempts: 3,
		OnRetry:  func(int, error) { retries++ },
	}

	if err := r.Do(context.Background(), op); err != nil {
		t.Fatalf("do: %v", err)
	}

	if *calls != 3 {
		t.Fatalf("calls = %d, want 3", *calls)
	}

	// Перед последней, удачной попыткой было две задержки
	if retries != 2 {
		t.Fatalf("OnRetry calls = %d, want 2", retries)
	}
}

// Попытки кончились: наружу уезжает ошибка последней
func TestRetryExhausted(t *testing.T) {
	want := errors.New("boom")
	op, calls := failing(10, want)

	var attempts []int

	r := Retry{
		Config:   fast(),
		Attempts: 3,
		OnRetry:  func(attempt int, _ error) { attempts = append(attempts, attempt) },
	}

	err := r.Do(context.Background(), op)
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}

	if *calls != 3 {
		t.Fatalf("calls = %d, want 3", *calls)
	}

	// После последней попытки ждать уже нечего
	if len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("OnRetry attempts = %v, want [1 2]", attempts)
	}
}

// Attempts = 1 - это "без повторов"
func TestRetrySingleAttempt(t *testing.T) {
	want := errors.New("boom")
	op, calls := failing(10, want)

	var retried bool

	r := Retry{
		Config:   fast(),
		Attempts: 1,
		OnRetry:  func(int, error) { retried = true },
	}

	if err := r.Do(context.Background(), op); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}

	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}

	if retried {
		t.Fatal("OnRetry called with Attempts = 1")
	}
}

// [ErrPermanent] в цепочке обрывает повторы без всякой настройки
func TestRetryErrPermanent(t *testing.T) {
	op, calls := failing(10, fmt.Errorf("parse: %w", ErrPermanent))

	var retried bool

	r := Retry{
		Config:   fast(),
		Attempts: 5,
		OnRetry:  func(int, error) { retried = true },
	}

	if err := r.Do(context.Background(), op); !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want to wrap ErrPermanent", err)
	}

	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}

	if retried {
		t.Fatal("OnRetry called on ErrPermanent")
	}
}

// [Retry.Permanent] классифицирует чужие ошибки, которые про сентинел не знают
func TestRetryPermanent(t *testing.T) {
	errPermanent := errors.New("permanent")
	op, calls := failing(10, errPermanent)

	r := Retry{
		Config:    fast(),
		Attempts:  5,
		Permanent: func(err error) bool { return errors.Is(err, errPermanent) },
	}

	if err := r.Do(context.Background(), op); !errors.Is(err, errPermanent) {
		t.Fatalf("err = %v, want %v", err, errPermanent)
	}

	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}
}

// Отмена во время ожидания: наружу едут обе причины - и почему упало,
// и почему не повторяли
func TestRetryCanceled(t *testing.T) {
	want := errors.New("boom")

	ctx, cancel := context.WithCancel(context.Background())

	var calls int

	op := func() error {
		calls++
		cancel()

		return want
	}

	r := Retry{Config: Config{Min: time.Hour, Max: time.Hour, Factor: 1}, Attempts: 5}

	err := r.Do(ctx, op)
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want to wrap %v", err, want)
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want to wrap context.Canceled", err)
	}

	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// Забытый Attempts не должен превращаться в "op не вызывать":
// молча вернуть nil, не сделав работу, хуже любой ошибки
func TestRetryZeroAttempts(t *testing.T) {
	want := errors.New("boom")
	op, calls := failing(10, want)

	if err := (Retry{Config: fast()}).Do(context.Background(), op); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}

	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}
}
