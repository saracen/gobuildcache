package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gocloud.dev/gcerrors"
	"google.golang.org/api/googleapi"
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

	// probeGrace is how long the startup check waits, once startupTimeout
	// is up, for the lookup's own error, which shows whether the bucket
	// answered: the GCS client returns the last answer it retried as soon as
	// its context ends, unless it's waiting on a token exchange.
	probeGrace = 100 * time.Millisecond
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
// off if that fails to connect, resolve or answer within startupTimeout. If
// the bucket answered, but with errors the SDK retried until then, it's
// turned off for this process only (see retriedAnswers).
//
// Only the GCS client keeps the answer it was retrying when its context
// ends, so the lookup is traced as withRetry's attempts are (see
// attemptTrace): an S3 or Azure bucket answering 503s for longer than the
// bound ends with the same deadline error as one that never answered. A
// lookup of a missing key that the bucket answered, and that ran out of
// time anyway, was retrying answers.
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

	trace, err := b.probe()
	reached := trace.reached(false)
	switch {
	case !reached && unreachable(err):
		b.remote.tripUnreachable(fmt.Errorf("checking bucket: %w", err))
	case retriedAnswers(err), reached && errors.Is(err, context.DeadlineExceeded):
		b.remote.trip(fmt.Errorf("checking bucket: %w", err))
	}
}

// probe looks up probeKey, giving up after startupTimeout, and returns the
// lookup's trace with its result.
func (b *Bucket) probe() (*attemptTrace, error) {
	done := b.stats.remoteCall()
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	ctx, trace := traceAttempt(ctx)

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
		return trace, err
	case <-ctx.Done():
	}

	select {
	case err := <-result:
		return trace, err
	case <-time.After(probeGrace):
		return trace, fmt.Errorf("no answer within %v: %w", startupTimeout, ctx.Err())
	}
}

// unreachable reports whether err means the bucket, or the service that
// issues its credentials, couldn't be reached at all: a failure to connect
// or resolve, or no answer in time. Failures on a connection that was made,
// such as a reset part way through a transfer, don't count, nor does a call
// being canceled, even if what it was retrying was a failure to connect.
// Errors that SDKs flatten into text, such as a failed token exchange,
// aren't recognised either, and count towards the breaker as other errors
// do. Nor is running out of time retrying answers (see retriedAnswers).
//
// A deadline doesn't show whether it came before or after the bucket was
// reached, so withRetry and the startup check also check the requests made
// (see attemptTrace).
func unreachable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || gcerrors.Code(err) == gcerrors.Canceled {
		return false
	}
	if answered(err) {
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

// attemptTrace notes how far an attempt's HTTP requests got with each host
// they went to, for withRetry and the startup check to tell whether the
// attempt reached the bucket.
//
// A call that reached the bucket did so whatever it failed with after: a
// healthy transfer too slow to finish within transferTimeout ends with the
// same deadline error as a call nothing reached, and taking it as
// unreachability would turn the bucket off for every process sharing the
// marker.
//
// Only the host of the attempt's latest request counts. The S3 and Azure
// SDKs fetch credentials with the call's context, before asking the bucket,
// so a credentials service answering doesn't show that the bucket can be
// reached, and one that doesn't answer leaves the attempt unreachable, as
// the bucket not answering does. The GCS client's token requests don't
// carry the call's context, so they aren't traced at all.
//
// Each event is taken as the latest request's: an SDK makes an attempt's
// requests one after another, or in parallel only to the bucket.
type attemptTrace struct {
	mu       sync.Mutex
	host     string
	sent     map[string]bool
	answered map[string]bool
}

// traceAttempt returns ctx with a trace of the HTTP requests made with it,
// recorded in the attemptTrace it returns.
func traceAttempt(ctx context.Context) (context.Context, *attemptTrace) {
	t := &attemptTrace{sent: map[string]bool{}, answered: map[string]bool{}}
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		// Called for every request, before a connection is dialed or
		// reused, over HTTP/1 and HTTP/2 alike.
		GetConn: func(hostPort string) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.host = hostPort
		},
		WroteHeaders: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.sent[t.host] = true
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.answered[t.host] = true
		},
	})
	return ctx, t
}

// reached reports whether the host of the attempt's latest request answered
// any of the attempt's requests or, if sending, was sent one.
//
// An upload the SDK sends in one request, as each of them does below its
// chunk or part size, may only be answered once it's all sent, and bytes the
// transport has written can still be queued behind a slow link. So sending
// counts an upload whose request went out on a connection as reached, even
// if nothing answered: it may well be moving bytes when time runs out. A
// server that accepts connections and never reads is taken as reached too,
// but the call made before each upload, a lookup of the output or a copy of
// the entry, finds a bucket like that. A request without a body is sent at
// once, so for other calls only an answer counts.
func (t *attemptTrace) reached(sending bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.answered[t.host] || sending && t.sent[t.host]
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

// answered reports whether err holds an answer from the bucket's service.
//
// Of the SDKs, only the GCS client keeps the answer it was retrying when its
// context ends. The S3 and Azure clients retry a few times, with short waits,
// and return the answer, or on a context ending return only its error, which
// can't be told from a call nothing answered.
func answered(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr)
}

// retriedAnswers reports whether err is from an SDK that retried answers
// until its context ended, such as the GCS client retrying 503s or 429s.
//
// The bucket answered, so this isn't unreachability, which is shared with
// the job's other go commands: the client waits up to 30s between tries, so
// a few seconds of 503s can outlast an attempt, and sharing that would turn
// the bucket off for 10 minutes. But the process gives up on the bucket
// rather than retrying, since each attempt would take its whole timeout.
func retriedAnswers(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) && answered(err)
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
