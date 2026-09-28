package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
	"google.golang.org/api/option"
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

// lossyDNS answers A queries for any name with 127.0.0.1, and other queries
// with no records, after dropping the first drop queries it's sent.
type lossyDNS struct {
	conn    net.PacketConn
	drop    int64
	queries atomic.Int64
}

func newLossyDNS(t *testing.T, drop int64) *lossyDNS {
	t.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	d := &lossyDNS{conn: conn, drop: drop}

	go func() {
		buf := make([]byte, 1500)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if d.queries.Add(1) <= d.drop {
				continue
			}
			if answer := dnsAnswer(buf[:n]); answer != nil {
				conn.WriteTo(answer, addr)
			}
		}
	}()
	return d
}

// dnsAnswer answers a query with one question.
func dnsAnswer(query []byte) []byte {
	if len(query) < 12 {
		return nil
	}
	// the question: the name's labels, then its type and class
	end := 12
	for end < len(query) && query[end] != 0 {
		end += int(query[end]) + 1
	}
	end += 5
	if end > len(query) {
		return nil
	}
	qtype := binary.BigEndian.Uint16(query[end-4:])

	answer := append([]byte(nil), query[:end]...)
	binary.BigEndian.PutUint16(answer[2:], 0x8180) // a response, recursion desired and available
	binary.BigEndian.PutUint16(answer[4:], 1)      // questions
	binary.BigEndian.PutUint16(answer[6:], 0)      // answers
	binary.BigEndian.PutUint16(answer[8:], 0)      // authorities
	binary.BigEndian.PutUint16(answer[10:], 0)     // additional records
	if qtype == 1 {
		binary.BigEndian.PutUint16(answer[6:], 1)
		// the question's name, type A, class IN, a TTL, and the address
		answer = append(answer, 0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 127, 0, 0, 1)
	}
	return answer
}

// A resolver resends a query it gets no answer to after its timeout, 5s by
// default for both Go's resolver and glibc's, so losing one packet makes a
// healthy bucket that long to answer. The startup check mustn't take that
// as the bucket being unreachable, which would turn it off for the job.
func TestStartup_LostDNSQueryKeepsTheBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the resolver to resend a query")
	}

	dns := newLossyDNS(t, 1)
	// not net.DefaultResolver, which other tests' calls may still be using
	dialer := &net.Dialer{Resolver: &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", dns.conn.LocalAddr().String())
		},
	}}
	transport := &http.Transport{DialContext: dialer.DialContext}
	t.Cleanup(transport.CloseIdleConnections)

	var requests atomic.Int64
	gcs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":{"code":404,"message":"Not Found"}}`)
	}))
	t.Cleanup(gcs.Close)
	_, port, err := net.SplitHostPort(gcs.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	endpoint := "http://" + net.JoinHostPort("storage.gobuildcache.test", port) + "/storage/v1/"
	underlying, err := gcsblob.OpenBucket(context.Background(), gcp.NewAnonymousHTTPClient(transport), "bucket", &gcsblob.Options{
		ClientOptions: []option.ClientOption{option.WithEndpoint(endpoint)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })
	b := checkedBucket(t, underlying, t.TempDir())

	start := time.Now()
	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Error(err)
	}
	t.Logf("lookup took %v, after %d DNS queries", time.Since(start), dns.queries.Load())

	if dns.queries.Load() <= dns.drop {
		t.Fatal("the lookup didn't resolve the bucket's host")
	}
	if !b.remote.allow() {
		t.Error("bucket turned off")
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("requests = %d, want the check and the lookup", got)
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

// A bucket that goes away after a process started using it is turned off by
// the first attempt that can't reach it, which also ends the calls still in
// flight, rather than each call retrying until its timeouts and the breaker
// waiting for five of them.
func TestBreaker_BucketGoingAwayMidJob(t *testing.T) {
	fastRetries(t, 300*time.Millisecond)
	// longer than the test waits for the download to end
	transferTimeout = time.Minute

	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	var gone atomic.Bool
	downloading := make(chan struct{})
	var downloadOnce sync.Once
	transport := &scriptedGCS{respond: func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/"+outputDir+"/") {
			// a download the GCS client retries until its context ends
			downloadOnce.Do(func() { close(downloading) })
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		if gone.Load() {
			return nil, refused
		}
		return notFound(r)
	}}
	dir := t.TempDir()
	b := newCheckedBucket(t, transport, dir)

	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Fatal(err)
	}

	download := make(chan error, 1)
	go func() {
		_, err := b.GetOutput(context.Background(), strings.Repeat("b", 64))
		download <- err
	}()
	<-downloading

	gone.Store(true)
	if _, _, err := b.OutputIDFromAction(context.Background(), strings.Repeat("c", 64)); err == nil {
		t.Error("lookup of a bucket that went away succeeded")
	}
	if got := b.stats.Retries.Load(); got != 0 {
		t.Errorf("retries = %d, want the first attempt to turn the bucket off", got)
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}
	if reason, err := os.ReadFile(b.remote.marker); err != nil || len(reason) == 0 {
		t.Errorf("marker: %q, %v; want the reason", reason, err)
	}

	select {
	case err := <-download:
		if err == nil {
			t.Error("download in flight succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("download in flight wasn't ended when the bucket was turned off")
	}

	requests := transport.requests.Load()
	if _, _, err := b.OutputIDFromAction(context.Background(), strings.Repeat("d", 64)); err != nil {
		t.Error(err)
	}
	if got := transport.requests.Load(); got != requests {
		t.Errorf("made %d more requests after turning the bucket off", got-requests)
	}
}

// A process already using the bucket when another finds it unreachable
// stops using it too, rather than each of its later calls paying an
// attempt's timeout to find out.
func TestBreaker_MarkerWrittenMidJobTurnsTheBucketOff(t *testing.T) {
	saved := markerCheckInterval
	t.Cleanup(func() { markerCheckInterval = saved })
	markerCheckInterval = 10 * time.Millisecond

	transport := &scriptedGCS{respond: notFound}
	b := newCheckedBucket(t, transport, t.TempDir())
	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Fatal(err)
	}
	if !b.remote.allow() {
		t.Fatal("bucket turned off")
	}

	if err := os.WriteFile(b.remote.marker, []byte("dial tcp: connection refused\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * markerCheckInterval)

	requests := transport.requests.Load()
	if _, _, err := b.OutputIDFromAction(context.Background(), strings.Repeat("c", 64)); err != nil {
		t.Error(err)
	}
	if got := transport.requests.Load(); got != requests {
		t.Errorf("made %d requests after another process found the bucket unreachable", got-requests)
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
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
	// a process finds these out in a few quick calls, so a brief one mustn't
	// turn the bucket off for the rest of the job
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tripping on errors that answer shared it: %v", err)
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
	reset := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	noHost := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", IsNotFound: true}}

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
		"proxy refused":      {&net.OpError{Op: "proxyconnect", Net: "tcp", Err: refused.Err}, true},
		"dns":                {&net.DNSError{Err: "no such host", IsNotFound: true}, true},
		"dial dns":           {fmt.Errorf("Get: %w", noHost), true},
		"connection reset":   {fmt.Errorf("Get: %w", reset), false},
		"canceled":           {context.Canceled, false},
		// as the GCS client reports a call canceled while it retried
		"canceled retrying refused": {fmt.Errorf("retry failed with %w; last error: %w", context.Canceled, refused), false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := unreachable(tc.err); got != tc.want {
				t.Errorf("unreachable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
