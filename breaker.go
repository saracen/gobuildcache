package main

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"gocloud.dev/gcerrors"
)

// maxConsecutiveFailures is how many remote operations in a row can fail
// before the bucket is given up on.
const maxConsecutiveFailures = 5

// breaker stops using the bucket once it's clearly not working, for example
// missing or invalid credentials, or no network. Otherwise every lookup of
// every build pays for (and logs) a failing request. The local cache keeps
// working either way.
type breaker struct {
	failures atomic.Int64
	tripped  atomic.Bool
	once     sync.Once
}

func (b *breaker) allow() bool {
	return !b.tripped.Load()
}

// record notes the result of a remote operation. Not found is a normal
// answer from a working bucket, so counts as success.
func (b *breaker) record(err error) {
	code := gcerrors.Code(err)
	if err == nil || code == gcerrors.NotFound {
		b.failures.Store(0)
		return
	}

	if code == gcerrors.PermissionDenied || b.failures.Add(1) >= maxConsecutiveFailures {
		b.once.Do(func() {
			b.tripped.Store(true)
			slog.Warn("remote cache disabled for the rest of this process, continuing with the local cache", "err", err)
		})
	}
}
