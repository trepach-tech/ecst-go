package backoff

import (
	"context"
	"errors"
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
	// Задержка между попытками
	Config

	// Сколько раз пытаться всего. 1 и меньше - без повторов
	Attempts int

	// Ошибки, на которых повторять бессмысленно, помимо [ErrPermanent]:
	// им классифицируют чужие ошибки, которые про этот сентинел не знают -
	// например окончательный отказ брокера.
	//
	// nil - повторяется все, кроме [ErrPermanent]
	Permanent func(error) bool

	// Вызывается перед каждой задержкой: место для лога.
	// attempt - номер только что провалившейся попытки
	//
	// nil - молча
	OnRetry func(attempt int, err error)
}

// Do повторяет op, пока она не пройдет или не кончатся попытки.
//
// Возвращает ошибку последней попытки. Прерывается сразу на [ErrPermanent],
// на [Retry.Permanent] и на отмене ctx: отмена во время ожидания - штатная остановка,
// и она приезжает вместе с исходной ошибкой
func (r Retry) Do(ctx context.Context, op func() error) error {
	var err error

	// Нулевой Attempts - это забытое поле, а не "не вызывать op":
	// молча вернуть nil, не сделав работу, хуже любой ошибки
	attempts := max(r.Attempts, 1)

	for attempt := 1; attempt <= attempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}

		if attempt == attempts || r.permanent(err) {
			break
		}

		if r.OnRetry != nil {
			r.OnRetry(attempt, err)
		}

		if waitErr := r.wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}

	return err
}

// permanent - стоит ли вообще повторять эту ошибку
func (r Retry) permanent(err error) bool {
	return errors.Is(err, ErrPermanent) || (r.Permanent != nil && r.Permanent(err))
}
