package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gocloud.dev/gcerrors"
)

const (
	// remoteDisabledFile, in the local cache directory, names the breaker's
	// markers: see breaker.marker and remoteDisabledMarker.
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

// remoteDisabledMarker returns the path of the marker in dir for the bucket
// that remote identifies (see remoteIdentity).
func remoteDisabledMarker(dir, remote string) string {
	sum := sha256.Sum256([]byte(remote))
	return filepath.Join(dir, remoteDisabledFile+"-"+hex.EncodeToString(sum[:8]))
}

// remoteIdentity identifies the bucket at bucketURL under prefix, and how this
// process reaches it, to key its remote-disabled marker. Processes sharing a
// local cache can use different buckets, or the same bucket through different
// endpoints, proxies or sources of credentials, and one of them being
// unreachable says nothing about the others. For example, a go command
// started without the credentials file that -env maps in asks the metadata
// server for a token, which off GCE never answers.
//
// So it's the URL, including parameters such as endpoint and anonymous, the
// prefix, and the environment variables that the providers' clients and Go's
// proxy settings read to decide where requests go and where credentials come
// from, as -env left them. Other variables, such as those the go command sets
// for each invocation, don't count, so a job's go commands still share it.
func remoteIdentity(bucketURL, prefix string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%q %q", bucketURL, prefix)

	env := os.Environ()
	sort.Strings(env)
	for _, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		if value = reachEnv(name, value); value != "" {
			fmt.Fprintf(&b, " %q=%q", name, value)
		}
	}
	return b.String()
}

// reachEnv returns what counts towards remoteIdentity of the environment
// variable name set to value, or "" for none. The clients treat an empty
// variable as unset. What a secret is doesn't change what's reached, only
// whether one is set, and a proxy's credentials don't change where requests
// go, so neither goes into the marker's name.
func reachEnv(name, value string) string {
	upper := strings.ToUpper(name)
	switch {
	case value == "":
		return ""
	case upper == "HTTP_PROXY" || upper == "HTTPS_PROXY":
		if u, err := url.Parse(value); err == nil && u.User != nil {
			u.User = nil
			return u.String()
		}
		return value
	case upper == "NO_PROXY" || upper == "STORAGE_EMULATOR_HOST":
		return value
	case !strings.HasPrefix(upper, "GOOGLE_") && !strings.HasPrefix(upper, "GCE_") &&
		!strings.HasPrefix(upper, "AWS_") && !strings.HasPrefix(upper, "AZURE_"):
		return ""
	}
	for _, secret := range []string{"SECRET", "TOKEN", "KEY", "PASSWORD", "CONNECTION_STRING", "SAS"} {
		if strings.Contains(upper, secret) {
			return "set"
		}
	}
	return value
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
