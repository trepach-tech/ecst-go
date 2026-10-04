// Package backoff - экспоненциальная задержка между повторными попытками с джиттером.
package backoff

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

type Config struct {
	// Задержка перед первой повторной попыткой
	Min time.Duration

	// Потолок задержки: дальше она не растет
	Max time.Duration

	// Во сколько раз растет задержка с каждой попыткой
	Factor float64

	// Какую долю задержки размывать случайно: [0, 1].
	// 0 - джиттера нет, 1 - full jitter (задержка от 0 до расчетной).
	//
	// Джиттер нужен, чтоб инстансы, упавшие на одной и той же ошибке,
	// не пошли ретраить одновременно
	Jitter float64
}

// Default возвращает бэкофф с разумными значениями по умолчанию
func Default() Config {
	return Config{
		Min:    250 * time.Millisecond,
		Max:    5 * time.Second,
		Factor: 2,
		Jitter: 1,
	}
}

// Validate проверяет конфиг и возвращает все найденные проблемы разом
func (c Config) Validate() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("backoff: "+msg)) }

	if c.Min <= 0 {
		add("Min must be > 0")
	}
	if c.Max <= 0 {
		add("Max must be > 0")
	}
	if c.Min > 0 && c.Max > 0 && c.Max < c.Min {
		add("Max must be >= Min")
	}
	if c.Factor < 1 {
		add("Factor must be >= 1")
	}
	if c.Jitter < 0 || c.Jitter > 1 {
		add("Jitter must be in [0, 1]")
	}

	return errors.Join(errs...)
}

// Delay - задержка перед повторной попыткой attempt (первая повторная - 1)
func (c Config) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := float64(c.Min) * math.Pow(c.Factor, float64(attempt-1))

	// Pow на больших attempt уходит в +Inf, поэтому режем до потолка явно
	if math.IsInf(d, 1) || d > float64(c.Max) {
		d = float64(c.Max)
	}

	d -= d * c.Jitter * rand.Float64()

	return time.Duration(d)
}

// wait ждет [Config.Delay] или отмену ctx
func (c Config) wait(ctx context.Context, attempt int) error {
	t := time.NewTimer(c.Delay(attempt))
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
