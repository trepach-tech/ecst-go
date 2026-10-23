package backoff

import "errors"

// ErrPermanent - ошибка, которую бессмысленно повторять: битый формат,
// нарушение констрейнта, чужая схема.
//
// Вызывающий оборачивает ее (fmt.Errorf("parse: %w", backoff.ErrPermanent)),
// и [Retry.Do] обрывает повторы сразу, не тратя попытки и паузы
var ErrPermanent = errors.New("permanent error")
