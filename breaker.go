package main

import (
	"context"
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

	// off is canceled when the breaker trips, ending calls still in flight;
	// see stopped.
	offOnce sync.Once
	off     context.Context
	cancel  context.CancelFunc

	// marker is where the bucket being unreachable is shared with other
	// processes using the same local cache and bucket (see remoteIdentity),
	// since the go command starts one per invocation and each would
	// otherwise find out for itself:
	// tripUnreachable writes it, and it turns the bucket off in processes
	// that start using it within remoteDisabledTTL, or that are using it
	// when it's written (see checkMarker). Empty to not share it.
	marker        string
	markerChecked atomic.Int64 // UnixNano
}

func (b *breaker) allow() bool {
	return !b.tripped.Load()
}

// stopped returns a context canceled once the bucket is turned off, so
// that calls in flight then, such as a download the SDK is retrying until
// its timeout, don't hold up the go command.
func (b *breaker) stopped() context.Context {
	b.offOnce.Do(func() {
		b.off, b.cancel = context.WithCancel(context.Background())
	})
	return b.off
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

// trip turns the bucket off for the rest of this process. The failures that
// trip it take a process at most a few quick calls, or one attempt, to find,
// and come from a bucket or token service that answers, so they aren't
// shared: sharing them would turn the bucket off for the rest of a CI job
// over something as brief as a token service failing for a couple of
// seconds.
func (b *breaker) trip(err error) {
	b.turnOff(err, false)
}

// tripUnreachable turns the bucket off for the rest of this process because
// it couldn't be reached, and shares that through the marker: finding out
// takes up to a call's timeout, which the job's other go commands needn't
// pay again.
func (b *breaker) tripUnreachable(err error) {
	b.turnOff(err, true)
}

func (b *breaker) turnOff(err error, share bool) {
	b.once.Do(func() {
		b.tripped.Store(true)
		b.stopped()
		b.cancel()
		slog.Warn("remote cache disabled for the rest of this process, continuing with the local cache", "err", err)

		if !share || b.marker == "" {
			return
		}
		if err := os.WriteFile(b.marker, []byte(err.Error()+"\n"), 0o600); err != nil {
			slog.Warn("writing remote disabled marker", "err", err)
		}
	})
}

// disable turns the bucket off for the rest of this process because another
// process found it unreachable when it last wrote the marker. It leaves the
// marker as it is, so that it expires.
func (b *breaker) disable(since time.Time, reason string) {
	b.once.Do(func() {
		b.tripped.Store(true)
		b.stopped()
		b.cancel()
		slog.Warn("remote cache disabled, as another process using this local cache found it unreachable; continuing with the local cache", "since", since.Format(time.RFC3339), "err", reason)
	})
}
