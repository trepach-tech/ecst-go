package ecst

import "github.com/trepach-tech/ecst-go/backoff"

var ErrPermanent func() error = func() error { return backoff.ErrPermanent }
