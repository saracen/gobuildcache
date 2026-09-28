package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"gocloud.dev/gcerrors"
)

const (
	// remoteDisabledFile, in the local cache directory, is the breaker's
	// marker: see breaker.marker.
	remoteDisabledFile = "remote-disabled"

	// remoteDisabledTTL is how long a process that found the bucket
	// unusable turns it off for the others sharing its local cache. Long
	// enough to cover the rest of a typical CI job, short enough that a
	// long-lived local cache soon tries the bucket again.
	remoteDisabledTTL = 10 * time.Minute

	// probeKey is looked up to check the bucket is usable. Nothing writes
	// it, so a working bucket answers not found.
	probeKey = "gobuildcache-probe"
)

// startupTimeout bounds the check of the bucket before a process first uses
// it. Some provider SDKs retry connection errors until their context ends,
// and can't get through a black hole anyway, so without a bound here an
// unreachable bucket or token service costs every go command minutes of
// retrying before the breaker trips.
//
// A working bucket answers in well under a second, including exchanging
// credentials for a token, but a resolver resends a lost DNS query only
// after its timeout, 5s by default for Go's resolver and glibc's, and the
// check can need two hosts resolved in turn, the token service's and the
// bucket's. So the bound leaves room for a lost query for each: tripping
// on one would turn a working bucket off for the whole job, whereas in a
// real outage the marker means only the first go command pays the bound.
var startupTimeout = 15 * time.Second

// useRemote reports whether to use the bucket, first checking it's usable if
// the process hasn't yet (see checkRemote). Calls made while the check runs
// wait for it.
func (b *Bucket) useRemote() bool {
	if b.remote.marker != "" {
		b.startup.Do(b.checkRemote)
		b.remote.checkMarker()
	}
	return b.remote.allow()
}

// markerCheckInterval is how often a process using the bucket looks for a
// marker another process wrote after it started, which saves its later calls
// each paying an attempt's timeout to find the bucket unreachable too. Only
// calls about to use the bucket look, so a stat this often costs little.
var markerCheckInterval = time.Second

// checkMarker turns the bucket off if the marker has been written since the
// process last looked, at most every markerCheckInterval.
func (b *breaker) checkMarker() {
	if !b.allow() {
		return
	}
	now := time.Now().UnixNano()
	last := b.markerChecked.Load()
	if now-last < int64(markerCheckInterval) || !b.markerChecked.CompareAndSwap(last, now) {
		return
	}
	if since, reason, ok := readRemoteDisabled(b.marker); ok {
		b.disable(since, reason)
	}
}

// checkRemote turns the bucket off if another process sharing the local cache
// recently found it unreachable, and otherwise looks up probeKey, turning it
// off if that fails to connect, resolve or answer within startupTimeout.
//
// Any other answer is left to the process's own calls, which count towards
// the breaker as usual: looking up a key nothing writes only shows whether
// the bucket can be reached. How a bucket answers for a missing key depends
// on more than whether it's usable; S3 answers 403 to callers without
// s3:ListBucket, which gocloud reports as permission denied.
func (b *Bucket) checkRemote() {
	if since, reason, ok := readRemoteDisabled(b.remote.marker); ok {
		b.remote.disable(since, reason)
		return
	}

	if err := b.probe(); unreachable(err) {
		b.remote.tripUnreachable(fmt.Errorf("checking bucket: %w", err))
	}
}

// probe looks up probeKey, giving up after startupTimeout.
func (b *Bucket) probe() error {
	done := b.stats.remoteCall()
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	// Not every SDK call returns when its context ends: a GCS call waiting
	// on a token exchange waits for the exchange, whose own timeout is
	// longer. So the lookup is left to finish on its own after that.
	result := make(chan error, 1)
	go func() {
		_, err := b.bucket.Exists(ctx, probeKey)
		result <- err
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("no answer within %v: %w", startupTimeout, ctx.Err())
	}
}

// unreachable reports whether err means the bucket, or the service that
// issues its credentials, couldn't be reached at all: a failure to connect
// or resolve, or no answer in time. Failures on a connection that was made,
// such as a reset part way through a transfer, don't count, nor does a call
// being canceled, even if what it was retrying was a failure to connect.
// Errors that SDKs flatten into text, such as a failed token exchange,
// aren't recognised either, and count towards the breaker as other errors
// do.
func unreachable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || gcerrors.Code(err) == gcerrors.Canceled {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || gcerrors.Code(err) == gcerrors.DeadlineExceeded {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) && (opErr.Op == "dial" || opErr.Op == "proxyconnect") {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// readRemoteDisabled returns when the marker was written and why, if it was
// written within remoteDisabledTTL.
func readRemoteDisabled(marker string) (time.Time, string, bool) {
	fi, err := os.Stat(marker)
	if err != nil || time.Since(fi.ModTime()) >= remoteDisabledTTL {
		return time.Time{}, "", false
	}

	// only for the log, so a partly written reason doesn't matter
	reason, _ := os.ReadFile(marker)
	return fi.ModTime(), string(bytes.TrimSpace(reason)), true
}
