package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"gocloud.dev/gcerrors"
	"google.golang.org/api/googleapi"
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
	// transferTimeout each attempt at moving an output, however steadily it's
	// moving. A transfer still moving at transferTimeout isn't tried again,
	// as it would take as long again.
	metadataTimeout = 30 * time.Second
	transferTimeout = 30 * time.Minute

	// transferIdleTimeout ends an attempt at moving an output once none of it
	// has moved for this long, which is what keeps a stuck transfer from
	// taking transferTimeout. A download is moving as long as its bytes
	// arrive, and is tried again if it stops. An upload only shows it's moving
	// while the SDK reads what it sends, so this is long enough for an SDK to
	// send a buffered chunk, 16 MiB for GCS, over a slow link.
	transferIdleTimeout = 2 * time.Minute

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

// downloadWithRetry is withRetry for op downloading an output, each attempt
// bounded by transferTimeout and transferIdleTimeout. op reads the output
// through movingReader, which is how the attempt knows it's moving.
func (b *Bucket) downloadWithRetry(ctx context.Context, op func(context.Context) error) error {
	return b.call(ctx, transferTimeout, callOptions{transfer: true}, op)
}

// uploadWithRetry is downloadWithRetry for op uploading an output, which it
// reads through movingReader too. An attempt that sent its request to the
// bucket reached it, answered or not (see attemptTrace.reached).
func (b *Bucket) uploadWithRetry(ctx context.Context, op func(context.Context) error) error {
	return b.call(ctx, transferTimeout, callOptions{sending: true, transfer: true}, op)
}

// callOptions are how call makes a call, when it isn't as withRetry does.
type callOptions struct {
	// sending takes an attempt that sent its request as reaching the bucket.
	sending bool

	// transfer bounds each attempt by transferIdleTimeout too, and doesn't
	// try again after one still moving at its timeout, nor an upload that
	// went out.
	transfer bool

	// unrecorded leaves the result out of the breaker, for a call whose
	// failure doesn't show whether the bucket works, nor its success that it
	// does (see refresh).
	unrecorded bool

	// once makes the call once, for one whose caller tries something else
	// when it fails (see refresh).
	once bool
}

// call is withRetry, with opts.
func (b *Bucket) call(ctx context.Context, timeout time.Duration, opts callOptions, op func(context.Context) error) error {
	err := b.retry(ctx, timeout, opts, op)
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
	var l *diskError
	return errors.Is(err, errSuperseded) || errors.As(err, &l)
}

// diskError is an error from the local disk during a call to the bucket,
// such as running out of space for a download. gocloud reports it as
// Unknown, which would otherwise be retried, downloading the output again
// each time, and counted towards the breaker, turning off a bucket that
// works over a full disk.
type diskError struct{ err error }

func (e *diskError) Error() string { return e.err.Error() }
func (e *diskError) Unwrap() error { return e.err }

// onDisk marks err, if any, as the local disk's.
func onDisk(err error) error {
	if err == nil {
		return nil
	}
	return &diskError{err}
}

// diskWriter marks the errors of writing to a local file.
type diskWriter struct{ w io.Writer }

func (l diskWriter) Write(p []byte) (int, error) {
	n, err := l.w.Write(p)
	return n, onDisk(err)
}

// diskReader marks the errors of reading a local file, other than its end.
type diskReader struct{ r io.Reader }

func (l diskReader) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	if err == io.EOF {
		return n, err
	}
	return n, onDisk(err)
}

// retry is call's loop.
func (b *Bucket) retry(ctx context.Context, timeout time.Duration, opts callOptions, op func(context.Context) error) error {
	delay := retryDelay

	done := b.stats.remoteCall()
	defer done()

	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		attemptCtx, trace := traceAttempt(attemptCtx)
		var watch *transferWatch
		if opts.transfer {
			attemptCtx, watch = watchTransfer(attemptCtx, cancel, transferIdleTimeout)
		}
		stop := context.AfterFunc(b.remote.stopped(), cancel)
		err := op(attemptCtx)
		stop()
		timedOut := errors.Is(attemptCtx.Err(), context.DeadlineExceeded)
		cancel()
		stalled := watch != nil && watch.stop() && err != nil
		if stalled {
			err = &stalledError{idle: transferIdleTimeout, err: err}
		}

		if ctx.Err() == nil && !inconclusive(err) {
			switch {
			case !trace.reached(opts.sending) && unreachable(err):
				b.remote.tripUnreachable(err)
			case retriedAnswers(err):
				b.remote.trip(err)
			}
		}
		if err == nil || !retryable(err) || attempt >= maxAttempts || opts.once || ctx.Err() != nil || !b.remote.allow() {
			return err
		}
		// A transfer still moving at its timeout would take as long again. An
		// upload's moving can't be told once the SDK has read what it sends:
		// it may still be going out over a slow link, so one that went out
		// isn't tried again either.
		if opts.transfer && trace.reached(opts.sending) && (timedOut || stalled && opts.sending) {
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

// transferWatch ends an attempt at a transfer once none of it has moved for
// its idle timeout, which movingReader tells it.
type transferWatch struct {
	last    atomic.Int64 // UnixNano of when bytes last moved
	stalled atomic.Bool
	done    chan struct{}
}

type transferWatchKey struct{}

// watchTransfer returns ctx with a transferWatch, which calls cancel once
// nothing has moved for idle.
func watchTransfer(ctx context.Context, cancel context.CancelFunc, idle time.Duration) (context.Context, *transferWatch) {
	w := &transferWatch{done: make(chan struct{})}
	w.moved()

	go func() {
		tick := time.NewTicker(idle / 8)
		defer tick.Stop()
		for {
			select {
			case <-w.done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				if time.Since(time.Unix(0, w.last.Load())) >= idle {
					w.stalled.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	return context.WithValue(ctx, transferWatchKey{}, w), w
}

func (w *transferWatch) moved() {
	w.last.Store(time.Now().UnixNano())
}

// stop stops watching, and reports whether the transfer stalled.
func (w *transferWatch) stop() bool {
	close(w.done)
	return w.stalled.Load()
}

// movingReader tells the transfer watching ctx, if any, whenever bytes are
// read through it.
type movingReader struct {
	ctx context.Context
	r   io.Reader
}

func (m movingReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 {
		if w, ok := m.ctx.Value(transferWatchKey{}).(*transferWatch); ok {
			w.moved()
		}
	}
	return n, err
}

// stalledError is a transfer ended because none of it moved for idle. It's a
// deadline, as one that ran out of time, rather than the cancellation that
// ended it, and keeps any answer the SDK was retrying, as the deadline would
// have (see retriedAnswers).
type stalledError struct {
	idle time.Duration
	err  error
}

func (e *stalledError) Error() string {
	return fmt.Sprintf("nothing moved for %v: %v", e.idle, e.err)
}

func (e *stalledError) Unwrap() []error {
	errs := []error{context.DeadlineExceeded}
	var apiErr *googleapi.Error
	if errors.As(e.err, &apiErr) {
		errs = append(errs, apiErr)
	}
	return errs
}
