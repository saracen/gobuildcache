package main

import (
	"context"
	"errors"
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
// times. Errors that can't change on retry are not, nor ones that say
// nothing about the bucket.
func retryable(err error) bool {
	if inconclusive(err) {
		return false
	}
	switch gcerrors.Code(err) {
	case gcerrors.Unknown, gcerrors.Internal, gcerrors.ResourceExhausted, gcerrors.DeadlineExceeded:
		return true
	}
	return false
}

// withRetry calls op, each attempt with its own timeout, until it succeeds,
// fails with an error that isn't retryable, or has been tried maxAttempts
// times. op must be safe to call again after a failure. It records the
// result for the breaker (see breaker.record), so that every call counts
// towards it the same way.
//
// An attempt that can't reach the bucket turns it off at once (see
// unreachable and attemptTrace), rather than being retried: SDKs that
// retry connection errors already did so within the attempt, some until its
// timeout, so every call in flight would otherwise take several timeouts to
// fail, and the breaker several such calls to trip. Turning the bucket off
// also ends the attempts still in flight. So does an attempt the SDK spent
// retrying answers, such as 503s, until its timeout, for this process only
// (see retriedAnswers).
func (b *Bucket) withRetry(ctx context.Context, timeout time.Duration, op func(context.Context) error) error {
	return b.call(ctx, timeout, callOptions{}, op)
}

// uploadWithRetry is withRetry for op uploading an output, each attempt
// bounded by transferTimeout. An attempt that sent its request to the
// bucket reached it, answered or not (see attemptTrace.reached).
func (b *Bucket) uploadWithRetry(ctx context.Context, op func(context.Context) error) error {
	return b.call(ctx, transferTimeout, callOptions{sending: true}, op)
}

// callOptions are how call makes a call, when it isn't as withRetry does.
type callOptions struct {
	// sending takes an attempt that sent its request as reaching the bucket.
	sending bool

	// unrecorded leaves the result out of the breaker, for a call whose
	// failure doesn't show whether the bucket works, nor its success that it
	// does (see refresh).
	unrecorded bool
}

// call is withRetry, with opts.
func (b *Bucket) call(ctx context.Context, timeout time.Duration, opts callOptions, op func(context.Context) error) error {
	err := b.retry(ctx, timeout, opts.sending, op)
	if !opts.unrecorded && !inconclusive(err) {
		b.remote.record(err)
	}
	return err
}

// errSuperseded is a write op chose not to make, because this process has
// put the key since (see refresh).
var errSuperseded = errors.New("superseded by a put")

// inconclusive reports whether err says nothing about whether the bucket
// works, so isn't retried, nor counted towards the breaker.
func inconclusive(err error) bool {
	return errors.Is(err, errSuperseded)
}

// retry is call's loop, taking an attempt that sent its request as reaching
// the bucket if sending.
func (b *Bucket) retry(ctx context.Context, timeout time.Duration, sending bool, op func(context.Context) error) error {
	delay := retryDelay

	done := b.stats.remoteCall()
	defer done()

	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		attemptCtx, trace := traceAttempt(attemptCtx)
		stop := context.AfterFunc(b.remote.stopped(), cancel)
		err := op(attemptCtx)
		stop()
		cancel()

		if ctx.Err() == nil && !inconclusive(err) {
			switch {
			case !trace.reached(sending) && unreachable(err):
				b.remote.tripUnreachable(err)
			case retriedAnswers(err):
				b.remote.trip(err)
			}
		}
		if err == nil || !retryable(err) || attempt >= maxAttempts || ctx.Err() != nil || !b.remote.allow() {
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
