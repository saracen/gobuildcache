package main

import (
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

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

	// marker is where the bucket being off is shared with other processes
	// using the same local cache, since the go command starts one per
	// invocation and each would otherwise find out for itself: tripping
	// writes it, and it turns the bucket off in processes that start using
	// it within remoteDisabledTTL. Empty to not share it.
	marker string
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
		b.trip(err)
	}
}

// trip turns the bucket off for the rest of this process, and shares that
// through the marker.
func (b *breaker) trip(err error) {
	b.once.Do(func() {
		b.tripped.Store(true)
		slog.Warn("remote cache disabled for the rest of this process, continuing with the local cache", "err", err)

		if b.marker == "" {
			return
		}
		if err := os.WriteFile(b.marker, []byte(err.Error()+"\n"), 0o600); err != nil {
			slog.Warn("writing remote disabled marker", "err", err)
		}
	})
}

// disable turns the bucket off for the rest of this process because another
// process found it unusable when it last wrote the marker. It leaves the
// marker as it is, so that it expires.
func (b *breaker) disable(since time.Time, reason string) {
	b.once.Do(func() {
		b.tripped.Store(true)
		slog.Warn("remote cache disabled, as another process using this local cache found it unusable; continuing with the local cache", "since", since.Format(time.RFC3339), "err", reason)
	})
}
