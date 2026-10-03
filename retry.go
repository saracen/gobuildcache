package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"gocloud.dev/gcerrors"
)

// Every call to the bucket goes through withRetry, which bounds how long it
// can take and retries errors that might be transient, the same way whichever
// provider is behind the bucket.
//
// The bound matters more than the retries: nothing else limits a call, and
// some provider SDKs retry internally until their context ends. A call that
// never returns would keep the go command from exiting, since it waits for
// this process, and the breaker only sees errors once a call returns.
var (
	// maxAttempts is how many times a call is made before giving up. SDKs
	// that retry internally do so within each attempt.
	maxAttempts = 3

	// metadataTimeout bounds each attempt at a lookup or small write, and
	// transferTimeout each attempt at moving an output.
	metadataTimeout = 30 * time.Second
	transferTimeout = 5 * time.Minute

	// retryDelay is the wait before the first retry, doubled for each one
	// after, with jitter.
	retryDelay = 250 * time.Millisecond
)

// retryable reports whether err might succeed if tried again.
//
// gocloud maps few provider errors to specific codes: a GCS 503, and most S3
// and Azure server errors and throttling, come back as Unknown. So Unknown is
// retried, which also means some permanent errors are, a bounded number of
// times. Errors that can't change on retry are not.
func retryable(err error) bool {
	switch gcerrors.Code(err) {
	case gcerrors.Unknown, gcerrors.Internal, gcerrors.ResourceExhausted, gcerrors.DeadlineExceeded:
		return true
	}
	return false
}

// withRetry calls op, each attempt with its own timeout, until it succeeds,
// fails with an error that isn't retryable, or has been tried maxAttempts
// times. op must be safe to call again after a failure.
func (b *Bucket) withRetry(ctx context.Context, timeout time.Duration, op func(context.Context) error) error {
	delay := retryDelay

	b.stats.RemoteCalls.Add(1)
	inFlight := b.stats.remoteInFlight.Add(1)
	defer b.stats.remoteInFlight.Add(-1)
	for {
		peak := b.stats.RemotePeakInFlight.Load()
		if inFlight <= peak || b.stats.RemotePeakInFlight.CompareAndSwap(peak, inFlight) {
			break
		}
	}

	start := time.Now()
	defer func() {
		took := time.Since(start).Milliseconds()
		b.stats.RemoteWaitMillis.Add(took)
		for {
			slowest := b.stats.RemoteSlowestMillis.Load()
			if took <= slowest || b.stats.RemoteSlowestMillis.CompareAndSwap(slowest, took) {
				break
			}
		}
	}()

	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		err := op(attemptCtx)
		cancel()

		if err == nil || !retryable(err) || attempt >= maxAttempts || ctx.Err() != nil {
			return err
		}

		b.stats.Retries.Add(1)
		wait := delay/2 + rand.N(delay)
		slog.Debug("retrying", "attempt", attempt, "wait", wait, "err", err)

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		delay *= 2
	}
}
