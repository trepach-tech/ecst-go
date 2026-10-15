package backoff

import (
	"testing"
	"time"
)

// Без джиттера задержка предсказуема: растет в Factor раз и упирается в Max
func TestDelay(t *testing.T) {
	c := Backoff{Min: 100 * time.Millisecond, Max: time.Second, Factor: 2}

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second, // потолок
		time.Second,
	}

	for i, w := range want {
		attempt := i + 1

		if got := c.Delay(attempt); got != w {
			t.Errorf("Delay(%d) = %s, want %s", attempt, got, w)
		}
	}
}

// Попытка меньше первой - та же задержка, что у первой:
// вызывающий не должен думать про нулевой и отрицательный attempt
func TestDelayAttemptBelowFirst(t *testing.T) {
	c := Backoff{Min: 100 * time.Millisecond, Max: time.Second, Factor: 2}

	for _, attempt := range []int{-1, 0} {
		if got := c.Delay(attempt); got != c.Min {
			t.Errorf("Delay(%d) = %s, want %s", attempt, got, c.Min)
		}
	}
}

// Pow на большом attempt уходит в +Inf: задержка все равно не должна
// превысить потолок и стать отрицательной
func TestDelayHugeAttempt(t *testing.T) {
	c := Backoff{Min: time.Second, Max: 5 * time.Second, Factor: 2}

	if got := c.Delay(10_000); got != c.Max {
		t.Fatalf("Delay(10000) = %s, want %s", got, c.Max)
	}
}

// Full jitter размывает задержку в [0, расчетная]
func TestDelayJitter(t *testing.T) {
	c := Backoff{Min: time.Second, Max: time.Second, Factor: 2, Jitter: 1}

	var distinct int
	prev := c.Delay(1)

	for range 100 {
		got := c.Delay(1)

		if got < 0 || got > time.Second {
			t.Fatalf("Delay = %s, want in [0s, 1s]", got)
		}

		if got != prev {
			distinct++
		}

		prev = got
	}

	// Джиттер, который всегда дает одно и то же, не джиттер
	if distinct == 0 {
		t.Fatal("jitter produced a constant delay")
	}
}

func TestConfigValidate(t *testing.T) {
	valid := func() Backoff {
		return Backoff{Min: time.Second, Max: 5 * time.Second, Factor: 2, Jitter: 0.5}
	}

	if err := valid().Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := map[string]func(c *Backoff){
		"zero min":       func(c *Backoff) { c.Min = 0 },
		"zero max":       func(c *Backoff) { c.Max = 0 },
		"max below min":  func(c *Backoff) { c.Max = c.Min - 1 },
		"factor below 1": func(c *Backoff) { c.Factor = 0.5 },
		"jitter below 0": func(c *Backoff) { c.Jitter = -0.1 },
		"jitter above 1": func(c *Backoff) { c.Jitter = 1.1 },
	}

	for name, brk := range tests {
		t.Run(name, func(t *testing.T) {
			c := valid()
			brk(&c)

			if err := c.Validate(); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestDefaultConfigIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config: %v", err)
	}
}
