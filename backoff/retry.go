package backoff

import (
	"context"
	"errors"
	"time"
)

// ErrPermanent - ошибка, которую бессмысленно повторять: битый формат,
// нарушение констрейнта, чужая схема.
//
// Вызывающий оборачивает ее (fmt.Errorf("parse: %w", backoff.ErrPermanent)),
// и [Retry.Do] обрывает повторы сразу, не тратя попытки и паузы
var ErrPermanent = errors.New("permanent error")

// Retry - повтор операции с задержкой между попытками.
//
// Сам по себе повтор без задержки бесполезен: все попытки выгорают
// за миллисекунды, пока лежит база или соседний сервис
type Retry struct {
	// Пауза между попытками
	Backoff Backoff

	// Permanent определяет, является ли ошибка окончательной и не требующей повторной попытки,
	// помимо ошибок, соответствующих ErrPermanent.
	// Используется для классификации ошибок сторонних библиотек, которые не используют ErrPermanent.
	//
	// Если nil, окончательными считаются только ошибки, соответствующие ErrPermanent.
	Permanent func(error) bool
}

// Do повторяет op, пока операция не выполнится или не закончатся попытки.
//
// Возвращает ошибку последней попытки. Прерывает повторы при ErrPermanent,
// при ошибке, для которой Retry.Permanent возвращает true, или при отмене ctx.
// Если ctx отменён во время ожидания, возвращает исходную ошибку вместе
// с ошибкой отмены.
//
// onRetry вызывается перед ожиданием следующей попытки после ошибки.
// attempt — номер неудачной попытки.
// Если nil, callback не вызывается.
func (r Retry) Do(
	ctx context.Context,
	attempts int,
	op func() error,
	onRetry func(attempt int, err error),
) error {
	var err error

	attempts = r.attempts(attempts)

	for attempt := 1; attempt <= attempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}

		if attempt == attempts || r.permanent(err) {
			break
		}

		if onRetry != nil {
			onRetry(attempt, err)
		}

		if waitErr := r.wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}

	return err
}

// wait ждет [Backoff.Delay] или отмену ctx
func (r Retry) wait(ctx context.Context, attempt int) error {
	t := time.NewTimer(r.Backoff.Delay(attempt))
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (r Retry) attempts(attempts int) int {
	// Нулевой Attempts - это забытое поле, а не "не вызывать op":
	// молча вернуть nil, не сделав работу, хуже любой ошибки
	return max(attempts, 1)
}

// permanent - стоит ли вообще повторять эту ошибку
func (r Retry) permanent(err error) bool {
	return errors.Is(err, ErrPermanent) || (r.Permanent != nil && r.Permanent(err))
}
