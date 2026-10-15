package ecst

import (
	"errors"

	"github.com/trepach-tech/ecst-go/inbox"
	"github.com/trepach-tech/ecst-go/outbox"
)

// Config собирает ECST-сервис из двух независимых частей.
//
// Обе опциональны: nil - соответствующий воркер не поднимается.
// Сервис только с Outbox - производитель событий, только с Inbox - потребитель,
// оба сразу - типовой сервис, который и читает чужие события, и публикует свои
type Config struct {
	// Забирает записи из outbox-таблицы и публикует их в Kafka
	Outbox *outbox.Config

	// Поднимает консьюмера на каждый зарегистрированный топик
	Inbox *inbox.Config
}

func (c Config) validate() error {
	var errs []error

	if c.Outbox == nil && c.Inbox == nil {
		errs = append(errs, errors.New("ecst: at least one of Outbox/Inbox must be configured"))
	}
	if c.Outbox != nil {
		errs = append(errs, c.Outbox.Validate())
	}
	if c.Inbox != nil {
		errs = append(errs, c.Inbox.Validate())
	}

	return errors.Join(errs...)
}
