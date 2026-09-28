package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcp"
)

// quickStartup bounds the startup check tightly for a test.
func quickStartup(t *testing.T, timeout time.Duration) {
	t.Helper()
	saved := startupTimeout
	t.Cleanup(func() { startupTimeout = saved })
	startupTimeout = timeout
}

// scriptedGCS answers Cloud Storage requests with respond, counting them.
type scriptedGCS struct {
	requests atomic.Int64
	probes   atomic.Int64
	respond  func(r *http.Request) (*http.Response, error)
}

func (f *scriptedGCS) RoundTrip(r *http.Request) (*http.Response, error) {
	f.requests.Add(1)
	if strings.Contains(r.URL.Path, probeKey) {
		f.probes.Add(1)
	}
	if r.Body != nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	return f.respond(r)
}

func gcsStatus(r *http.Request, code int) (*http.Response, error) {
	return &http.Response{
		StatusCode: code,
		Status:     fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"error":{"code":%d,"message":"%s"}}`, code, http.StatusText(code)))),
		Request:    r,
	}, nil
}

func notFound(r *http.Request) (*http.Response, error) { return gcsStatus(r, http.StatusNotFound) }

// newCheckedBucket returns a bucket backed by transport that checks it at
// startup, sharing the result through dir as serve does.
func newCheckedBucket(t *testing.T, transport http.RoundTripper, dir string) *Bucket {
	t.Helper()

	underlying, err := gcsblob.OpenBucket(context.Background(), gcp.NewAnonymousHTTPClient(transport), "bucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })
	return checkedBucket(t, underlying, dir)
}

// checkedBucket wraps underlying as serve does, checking it at startup and
// sharing the result through dir.
func checkedBucket(t *testing.T, underlying *blob.Bucket, dir string) *Bucket {
	t.Helper()

	for _, sub := range []string{actionDir, outputDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	b := &Bucket{disk: &Disk{cacheDir: dir}, bucket: underlying}
	b.remote.marker = filepath.Join(dir, remoteDisabledFile)
	b.Start(context.Background())
	t.Cleanup(b.Close)
	return b
}

var someAction = strings.Repeat("a", 64)

func TestStartup_UnreachableBucketIsTurnedOffAtOnce(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	noHost := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "storage.googleapis.com", IsNotFound: true}}

	// The subtests run in random order, so each has its own channel, closed
	// when it ends: with one shared channel, "never answers" answers at once
	// unless it runs first.
	tests := map[string]func(release <-chan struct{}) (*http.Response, error){
		// retried by the GCS client until its context ends
		"connection refused": func(<-chan struct{}) (*http.Response, error) { return nil, refused },
		"no such host":       func(<-chan struct{}) (*http.Response, error) { return nil, noHost },
		// like a token exchange that never answers, which the request's
		// context doesn't end; given up on after a while so that a check
		// that waits for it fails rather than hangs
		"never answers": func(release <-chan struct{}) (*http.Response, error) {
			select {
			case <-release:
			case <-time.After(5 * time.Second):
			}
			return nil, refused
		},
	}

	for name, respond := range tests {
		t.Run(name, func(t *testing.T) {
			quickStartup(t, 200*time.Millisecond)
			dir := t.TempDir()
			release := make(chan struct{})
			transport := &scriptedGCS{respond: func(*http.Request) (*http.Response, error) { return respond(release) }}
			b := newCheckedBucket(t, transport, dir)
			// before closing the bucket, which waits for calls
			t.Cleanup(func() { close(release) })

			start := time.Now()
			outputID, _, err := b.OutputIDFromAction(context.Background(), someAction)
			if outputID != "" || err != nil {
				t.Errorf("lookup = %q, %v; want a miss", outputID, err)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("lookup took %v", took)
			}
			if b.remote.allow() {
				t.Error("bucket still in use")
			}
			if reason, err := os.ReadFile(b.remote.marker); err != nil || len(reason) == 0 {
				t.Errorf("marker: %q, %v; want the reason", reason, err)
			}

			requests := transport.requests.Load()
			if _, _, err := b.OutputIDFromAction(context.Background(), strings.Repeat("c", 64)); err != nil {
				t.Error(err)
			}
			if got := transport.requests.Load(); got != requests {
				t.Errorf("made %d more requests after turning the bucket off", got-requests)
			}
		})
	}
}

// A GCS call waiting on a token exchange waits for the exchange, whatever
// the call's own deadline, so the startup check has to give up on it itself
// rather than wait for the token client's timeout.
func TestStartup_TokenExchangeThatNeverAnswers(t *testing.T) {
	quickStartup(t, 200*time.Millisecond)
	// the token client's timeout, which is taken when the bucket is opened
	fastRetries(t, 5*time.Second)

	sts := newSilentListener(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeExternalAccount(t, "http://"+sts.Addr().String()+"/v1/token"))
	t.Setenv("STORAGE_EMULATOR_HOST", "")

	underlying, err := openBucket(context.Background(), "gs://bucket")
	if err != nil {
		t.Fatal(err)
	}
	b := checkedBucket(t, underlying, t.TempDir())

	start := time.Now()
	if outputID, _, err := b.OutputIDFromAction(context.Background(), someAction); outputID != "" || err != nil {
		t.Errorf("lookup = %q, %v; want a miss", outputID, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("lookup took %v, want about the startup timeout", took)
	}
	if sts.accepted.Load() == 0 {
		t.Error("no token exchange was attempted")
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}
	if _, err := os.Stat(b.remote.marker); err != nil {
		t.Errorf("marker: %v", err)
	}
}

func TestStartup_MarkerTurnsTheBucketOffForOtherProcesses(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, remoteDisabledFile)
	if err := os.WriteFile(marker, []byte("checking bucket: dial tcp: connection refused\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	written := time.Now().Add(-remoteDisabledTTL / 2)
	if err := os.Chtimes(marker, written, written); err != nil {
		t.Fatal(err)
	}

	transport := &scriptedGCS{respond: notFound}
	b := newCheckedBucket(t, transport, dir)

	if outputID, _, err := b.OutputIDFromAction(context.Background(), someAction); outputID != "" || err != nil {
		t.Errorf("lookup = %q, %v; want a miss", outputID, err)
	}
	if got := transport.requests.Load(); got != 0 {
		t.Errorf("made %d requests", got)
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}

	// rewriting it would keep it from ever expiring while processes run
	if fi, err := os.Stat(marker); err != nil || !fi.ModTime().Equal(written) {
		t.Errorf("marker rewritten: %v, %v", fi.ModTime(), err)
	}
}

func TestStartup_ExpiredMarkerIsIgnored(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, remoteDisabledFile)
	if err := os.WriteFile(marker, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-remoteDisabledTTL - time.Minute)
	if err := os.Chtimes(marker, old, old); err != nil {
		t.Fatal(err)
	}

	transport := &scriptedGCS{respond: notFound}
	b := newCheckedBucket(t, transport, dir)

	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Error(err)
	}
	if !b.remote.allow() {
		t.Error("bucket turned off")
	}
	if got := transport.probes.Load(); got != 1 {
		t.Errorf("probes = %d, want 1", got)
	}
	if got := transport.requests.Load(); got != 2 {
		t.Errorf("requests = %d, want the probe and the lookup", got)
	}
}

func TestStartup_CallsWaitForOneCheck(t *testing.T) {
	var inProbe atomic.Int64
	transport := &scriptedGCS{respond: func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, probeKey) {
			inProbe.Add(1)
			defer inProbe.Add(-1)
			time.Sleep(50 * time.Millisecond)
		} else if inProbe.Load() != 0 {
			return nil, errors.New("lookup made during the check")
		}
		return notFound(r)
	}}
	b := newCheckedBucket(t, transport, t.TempDir())

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			if _, _, err := b.OutputIDFromAction(context.Background(), fmt.Sprintf("%064x", i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if got := transport.probes.Load(); got != 1 {
		t.Errorf("probes = %d, want 1", got)
	}
	if got := b.stats.RemoteCalls.Load(); got != 11 {
		t.Errorf("remote calls = %d, want the probe and 10 lookups", got)
	}
}

// Errors that answer, such as an unauthorized anonymous caller, aren't
// unreachability. The check leaves them to the process's own calls, which
// count towards the breaker as usual.
func TestStartup_OtherErrorsAreLeftToTheBreaker(t *testing.T) {
	fastRetries(t, time.Second)
	transport := &scriptedGCS{respond: func(r *http.Request) (*http.Response, error) {
		return gcsStatus(r, http.StatusUnauthorized)
	}}
	b := newCheckedBucket(t, transport, t.TempDir())

	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err == nil {
		t.Error("lookup succeeded")
	}
	if !b.remote.allow() {
		t.Error("bucket turned off after 1 failure")
	}
	if got := b.remote.failures.Load(); got != 1 {
		t.Errorf("failures = %d, want only the lookup", got)
	}
	if got := transport.probes.Load(); got != 1 {
		t.Errorf("probes = %d, want 1", got)
	}

	for i := range maxConsecutiveFailures {
		b.OutputIDFromAction(context.Background(), fmt.Sprintf("%064x", i))
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}
	if _, err := os.Stat(b.remote.marker); err != nil {
		t.Errorf("tripping later didn't share it: %v", err)
	}
}

// Without s3:ListBucket, S3 answers a lookup of a missing key with 403,
// which gocloud reports as permission denied, so looking up a key nothing
// writes can't show whether the bucket is usable.
func TestStartup_S3WithoutListBucket(t *testing.T) {
	outputID := strings.Repeat("b", 64)
	var requests atomic.Int64
	underlying := openFakeS3(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodHead || r.URL.Path != "/bucket/"+path.Join(actionDir, someAction) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "0")
		w.Header().Set("X-Amz-Meta-Output_id", outputID)
	}))
	b := checkedBucket(t, underlying, t.TempDir())

	if got, _, err := b.OutputIDFromAction(context.Background(), someAction); got != outputID || err != nil {
		t.Errorf("lookup = %q, %v; want the entry", got, err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("requests = %d, want the check and the lookup", got)
	}
	if !b.remote.allow() {
		t.Error("bucket turned off")
	}
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("marker written: %v", err)
	}
}

// The startup check is only for processes sharing the breaker's state.
func TestStartup_NoMarkerNoCheck(t *testing.T) {
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	b := &Bucket{disk: newDisk(t), bucket: underlying}
	b.Start(context.Background())
	defer b.Close()

	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Fatal(err)
	}
	if got := b.stats.RemoteCalls.Load(); got != 1 {
		t.Errorf("remote calls = %d, want only the lookup", got)
	}
}

func TestUnreachable(t *testing.T) {
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()
	_, notFound := underlying.Attributes(context.Background(), "missing")

	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

	tests := map[string]struct {
		err  error
		want bool
	}{
		"nil":                {nil, false},
		"not found":          {notFound, false},
		"server error":       {errors.New("googleapi: Error 503: Service Unavailable"), false},
		"deadline":           {context.DeadlineExceeded, true},
		"wrapped deadline":   {fmt.Errorf("lookup: %w", context.DeadlineExceeded), true},
		"connection refused": {fmt.Errorf("Get: %w", refused), true},
		"dns":                {&net.DNSError{Err: "no such host", IsNotFound: true}, true},
		"canceled":           {context.Canceled, false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := unreachable(tc.err); got != tc.want {
				t.Errorf("unreachable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
