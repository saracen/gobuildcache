package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"google.golang.org/api/googleapi"
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

// testRemote identifies the bucket of checkedBucket's markers.
const testRemote = "gs://bucket"

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
	b.remote.marker = remoteDisabledMarker(dir, testRemote)
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
	marker := remoteDisabledMarker(dir, testRemote)
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

// A marker only turns off the bucket it was written for: processes sharing
// the local cache can use other buckets, or reach the same one another way.
func TestStartup_MarkerIsForItsBucket(t *testing.T) {
	quickStartup(t, 200*time.Millisecond)
	dir := t.TempDir()
	unreachable := newCheckedBucket(t, &scriptedGCS{respond: func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	}}, dir)
	unreachable.remote.marker = remoteDisabledMarker(dir, remoteIdentity("gs://unreachable", ""))
	unreachable.OutputIDFromAction(context.Background(), someAction)
	if _, err := os.Stat(unreachable.remote.marker); err != nil {
		t.Fatalf("marker: %v", err)
	}

	transport := &scriptedGCS{respond: notFound}
	b := newCheckedBucket(t, transport, dir)
	b.remote.marker = remoteDisabledMarker(dir, remoteIdentity("gs://reachable", ""))
	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Error(err)
	}
	if !b.remote.allow() {
		t.Error("bucket turned off by another bucket's marker")
	}
	if got := transport.probes.Load(); got != 1 {
		t.Errorf("probes = %d, want 1", got)
	}
}

func TestRemoteIdentity(t *testing.T) {
	// what the tests change, so the host's values don't matter
	for _, name := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "GCE_METADATA_HOST", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL", "HTTPS_PROXY", "https_proxy", "CI_JOB_ID"} {
		t.Setenv(name, "")
	}
	base := remoteIdentity("gs://bucket", "p/1/")

	tests := map[string]struct {
		url, prefix string
		env         map[string]string
		same        bool
	}{
		"same":                   {"gs://bucket", "p/1/", nil, true},
		"other bucket":           {"gs://other", "p/1/", nil, false},
		"other parameters":       {"gs://bucket?anonymous=true", "p/1/", nil, false},
		"other prefix":           {"gs://bucket", "p/2/", nil, false},
		"credentials file":       {"gs://bucket", "p/1/", map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/creds.json"}, false},
		"metadata server":        {"gs://bucket", "p/1/", map[string]string{"GCE_METADATA_HOST": "10.0.0.1"}, false},
		"endpoint":               {"gs://bucket", "p/1/", map[string]string{"AWS_ENDPOINT_URL": "http://minio:9000"}, false},
		"proxy":                  {"gs://bucket", "p/1/", map[string]string{"HTTPS_PROXY": "http://proxy:3128"}, false},
		"lowercase proxy":        {"gs://bucket", "p/1/", map[string]string{"https_proxy": "http://proxy:3128"}, false},
		"a secret":               {"gs://bucket", "p/1/", map[string]string{"AWS_SECRET_ACCESS_KEY": "one"}, false},
		"unrelated variable":     {"gs://bucket", "p/1/", map[string]string{"CI_JOB_ID": "1"}, true},
		"empty credentials file": {"gs://bucket", "p/1/", map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": ""}, true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := remoteIdentity(tc.url, tc.prefix) == base; got != tc.same {
				t.Errorf("same identity = %v, want %v", got, tc.same)
			}
		})
	}

	// only whether a secret or a proxy's credentials are set counts, and
	// they aren't written into the marker's name
	identity := func(env map[string]string) string {
		for k, v := range env {
			t.Setenv(k, v)
		}
		return remoteIdentity("gs://bucket", "p/1/")
	}
	one := identity(map[string]string{"AWS_SECRET_ACCESS_KEY": "secret-7f2c", "HTTPS_PROXY": "http://user:password-7f2c@proxy:3128"})
	two := identity(map[string]string{"AWS_SECRET_ACCESS_KEY": "secret-91ab", "HTTPS_PROXY": "http://user:password-91ab@proxy:3128"})
	if one != two {
		t.Errorf("identities differ by secrets: %q, %q", one, two)
	}
	if strings.Contains(one, "7f2c") {
		t.Errorf("identity holds a secret: %q", one)
	}
}

func TestStartup_ExpiredMarkerIsIgnored(t *testing.T) {
	dir := t.TempDir()
	marker := remoteDisabledMarker(dir, testRemote)
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

// unavailable answers every request with 503, which the GCS client retries
// until its context ends, then reports with the last answer it got.
func unavailable(r *http.Request) (*http.Response, error) {
	return gcsStatus(r, http.StatusServiceUnavailable)
}

// A bucket answering errors past the startup check's bound is answering, so
// it mustn't be taken as unreachable, which would turn it off for the whole
// job. The process that saw it gives up without sharing that.
func TestStartup_ServerErrorsPastTheBoundAreNotShared(t *testing.T) {
	quickStartup(t, 200*time.Millisecond)
	fastRetries(t, 300*time.Millisecond)

	dir := t.TempDir()
	transport := &scriptedGCS{respond: unavailable}
	b := newCheckedBucket(t, transport, dir)

	start := time.Now()
	if outputID, _, err := b.OutputIDFromAction(context.Background(), someAction); outputID != "" || err != nil {
		t.Errorf("lookup = %q, %v; want a miss", outputID, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("lookup took %v, want about the startup timeout", took)
	}
	if transport.probes.Load() == 0 {
		t.Fatal("the check made no requests")
	}
	if b.remote.allow() {
		t.Error("bucket still in use after the check spent its bound on errors")
	}
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("server errors shared as unreachability: %v", err)
	}

	// another process sharing the local cache checks for itself
	other := newCheckedBucket(t, &scriptedGCS{respond: notFound}, dir)
	if _, _, err := other.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Error(err)
	}
	if !other.remote.allow() {
		t.Error("bucket turned off for another process")
	}
}

// The same for errors past an attempt's timeout once the process is using
// the bucket, which it gives up on at the first such attempt rather than
// retrying it.
func TestBreaker_ServerErrorsPastTheAttemptTimeoutAreNotShared(t *testing.T) {
	quickStartup(t, time.Second)
	fastRetries(t, 300*time.Millisecond)

	var brownout atomic.Bool
	transport := &scriptedGCS{respond: func(r *http.Request) (*http.Response, error) {
		if brownout.Load() {
			return unavailable(r)
		}
		return notFound(r)
	}}
	b := newCheckedBucket(t, transport, t.TempDir())
	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Fatal(err)
	}

	brownout.Store(true)
	start := time.Now()
	if _, _, err := b.OutputIDFromAction(context.Background(), strings.Repeat("c", 64)); err == nil {
		t.Error("lookup of a bucket answering 503 succeeded")
	}
	if took := time.Since(start); took > 2*metadataTimeout {
		t.Errorf("lookup took %v, want about one attempt's timeout", took)
	}
	if got := b.stats.Retries.Load(); got != 0 {
		t.Errorf("retries = %d, want the first attempt to give up on the bucket", got)
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("server errors shared as unreachability: %v", err)
	}
}

// emulatedGCS opens gs://bucket through the storage emulator, served by
// handler, which answers the startup check not found, so that requests go
// through a real HTTP transport, as they do in use. It returns the bucket,
// checked at startup and sharing the result through dir.
func emulatedGCS(t *testing.T, dir string, handler http.HandlerFunc) *Bucket {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, probeKey) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":{"code":404,"message":"Not Found"}}`)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", srv.Listener.Addr().String())

	underlying, err := openBucket(context.Background(), "gs://bucket")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })
	return checkedBucket(t, underlying, dir)
}

// trickle answers with a large body sent slowly, until the client goes
// away, counting what it sends.
func trickle(sent *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(http.StatusOK)
		chunk := make([]byte, 1024)
		for {
			n, err := w.Write(chunk)
			sent.Add(int64(n))
			if err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

// readSlowly reads r's body a little at a time, counting what it reads,
// without answering, for up to d. Reads can go on from buffers after the
// client gives up, so it stops then too.
func readSlowly(r *http.Request, received *atomic.Int64, d time.Duration) {
	start := time.Now()
	buf := make([]byte, 1024)
	for time.Since(start) < d {
		n, err := r.Body.Read(buf)
		received.Add(int64(n))
		if err != nil {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// A transfer the bucket answered but that's too slow to finish within an
// attempt's timeout, such as a large output over a slow link, reached the
// bucket, so it's an ordinary failure: it mustn't turn the bucket off for
// the other processes sharing the marker.
func TestBreaker_SlowDownloadIsNotShared(t *testing.T) {
	fastRetries(t, 300*time.Millisecond)

	var sent atomic.Int64
	dir := t.TempDir()
	b := emulatedGCS(t, dir, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/"+outputDir+"/") {
			trickle(&sent)(w, r)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	if _, err := b.GetOutput(context.Background(), strings.Repeat("b", 64)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("download = %v, want it to run out of time", err)
	}
	if sent.Load() == 0 {
		t.Fatal("the download got no answer")
	}
	if got := b.stats.Retries.Load(); got != int64(maxAttempts-1) {
		t.Errorf("retries = %d, want it retried as an ordinary failure", got)
	}
	if got := b.remote.failures.Load(); got != 1 {
		t.Errorf("failures = %d, want the download counted once", got)
	}
	if !b.remote.allow() {
		t.Error("bucket turned off by a slow download")
	}
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("slow download shared as unreachability: %v", err)
	}

	// another process sharing the local cache still uses the bucket
	var requests atomic.Int64
	other := emulatedGCS(t, dir, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	})
	if _, _, err := other.OutputIDFromAction(context.Background(), someAction); err != nil {
		t.Error(err)
	}
	if !other.remote.allow() || requests.Load() == 0 {
		t.Error("bucket turned off for another process")
	}
}

// The same for an upload: a resumable upload, of an output larger than the
// GCS client's chunk, is answered when it starts, before its chunks are sent.
func TestBreaker_SlowUploadIsNotShared(t *testing.T) {
	if testing.Short() {
		t.Skip("uploads more than 16MiB")
	}
	const timeout = 300 * time.Millisecond
	fastRetries(t, timeout)

	var received atomic.Int64
	dir := t.TempDir()
	b := emulatedGCS(t, dir, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Get("uploadType") == "resumable":
			w.Header().Set("Location", "http://"+r.Host+"/upload/session")
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/upload/session":
			readSlowly(r, &received, 2*timeout)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":{"code":404,"message":"Not Found"}}`)
		}
	})

	outputID := strings.Repeat("e", 64)
	output := bytes.Repeat([]byte{'x'}, googleapi.DefaultUploadChunkSize+1)
	if err := os.WriteFile(filepath.Join(dir, outputDir, outputID), output, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := b.uploadOutput(context.Background(), outputID); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("upload = %v, want it to run out of time", err)
	}
	if received.Load() == 0 {
		t.Fatal("the upload's chunk wasn't sent")
	}
	if !b.remote.allow() {
		t.Error("bucket turned off by a slow upload")
	}
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("slow upload shared as unreachability: %v", err)
	}
}

// A transfer that times out before any answer still can't reach the bucket,
// over a real transport as with scriptedGCS.
func TestBreaker_TransferWithoutAnAnswerIsShared(t *testing.T) {
	fastRetries(t, 300*time.Millisecond)

	b := emulatedGCS(t, t.TempDir(), func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/"+outputDir+"/") {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	if _, err := b.GetOutput(context.Background(), strings.Repeat("b", 64)); err == nil {
		t.Error("download nothing answered succeeded")
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
}

// An upload the SDK sends in one request, below its chunk or part size, is
// only answered once it's all sent, so one read slowly past the attempt
// timeout, as over a slow link, got no answer. It reached the bucket all the
// same, so it's an ordinary failure too.
func TestBreaker_SlowSingleRequestUploadIsNotShared(t *testing.T) {
	const timeout = 300 * time.Millisecond

	tests := map[string]func(t *testing.T, dir string, upload http.HandlerFunc) *Bucket{
		// a multipart upload, below the 16MiB chunk
		"gcs": func(t *testing.T, dir string, upload http.HandlerFunc) *Bucket {
			return emulatedGCS(t, dir, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("uploadType") == "multipart" {
					upload(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				io.WriteString(w, `{"error":{"code":404,"message":"Not Found"}}`)
			})
		},
		// a PutObject, below the 2MiB above which it sends Expect: 100-continue
		"s3": func(t *testing.T, dir string, upload http.HandlerFunc) *Bucket {
			return checkedBucket(t, openFakeS3(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					upload(w, r)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			})), dir)
		},
	}

	for name, open := range tests {
		t.Run(name, func(t *testing.T) {
			fastRetries(t, timeout)

			var received, uploads atomic.Int64
			dir := t.TempDir()
			b := open(t, dir, func(w http.ResponseWriter, r *http.Request) {
				uploads.Add(1)
				readSlowly(r, &received, 2*timeout)
			})

			outputID := strings.Repeat("e", 64)
			if err := os.WriteFile(filepath.Join(dir, outputDir, outputID), bytes.Repeat([]byte{'x'}, 1<<20), 0o600); err != nil {
				t.Fatal(err)
			}

			if err := b.uploadOutput(context.Background(), outputID); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("upload = %v, want it to run out of time", err)
			}
			if received.Load() == 0 {
				t.Fatal("the upload wasn't sent")
			}
			if got := uploads.Load(); got != int64(maxAttempts) {
				t.Errorf("uploads = %d, want it retried as an ordinary failure", got)
			}
			if !b.remote.allow() {
				t.Error("bucket turned off by a slow upload")
			}
			if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("slow upload shared as unreachability: %v", err)
			}
		})
	}
}

// containerCredentials serves AWS credentials, expiring at expires, as a
// container credentials endpoint does, and points the AWS SDK's default
// chain at it alone. It counts the requests for them.
func containerCredentials(t *testing.T, expires time.Time) *atomic.Int64 {
	t.Helper()

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"AccessKeyId":"id","SecretAccessKey":"secret","Token":"token","Expiration":%q}`,
			expires.UTC().Format(time.RFC3339))
	}))
	t.Cleanup(srv.Close)

	none := filepath.Join(t.TempDir(), "none")
	for k, v := range map[string]string{
		"AWS_CONTAINER_CREDENTIALS_FULL_URI":     srv.URL + "/creds",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_CONFIG_FILE":                        none,
		"AWS_SHARED_CREDENTIALS_FILE":            none,
		"AWS_EC2_METADATA_DISABLED":              "true",
		"AWS_ACCESS_KEY_ID":                      "",
		"AWS_SECRET_ACCESS_KEY":                  "",
		"AWS_SESSION_TOKEN":                      "",
		"AWS_PROFILE":                            "",
		"AWS_WEB_IDENTITY_TOKEN_FILE":            "",
		"AWS_ROLE_ARN":                           "",
	} {
		t.Setenv(k, v)
	}
	return &requests
}

// openS3At opens an S3 bucket at endpoint as serve does, with the SDK's
// default credentials chain.
func openS3At(t *testing.T, endpoint string) *blob.Bucket {
	t.Helper()

	underlying, err := openBucket(context.Background(), "s3://bucket?region=us-east-1&use_path_style=true&endpoint="+url.QueryEscape(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })
	return underlying
}

// The AWS SDK fetches credentials with the call's context, and a process
// starts without any, so its startup check always fetches them. The
// credentials service answering doesn't mean the bucket can be reached.
func TestStartup_S3CredentialsAnsweringDoNotHideAnUnreachableBucket(t *testing.T) {
	quickStartup(t, 300*time.Millisecond)
	credentials := containerCredentials(t, time.Now().Add(time.Hour))
	silent := newSilentListener(t)

	b := checkedBucket(t, openS3At(t, "http://"+silent.Addr().String()), t.TempDir())

	start := time.Now()
	if b.useRemote() {
		t.Error("bucket still in use")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("startup check took %v, want it to give up at its bound", took)
	}
	if credentials.Load() == 0 || silent.accepted.Load() == 0 {
		t.Fatalf("credential requests = %d, bucket connections = %d; want both", credentials.Load(), silent.accepted.Load())
	}
	if reason, err := os.ReadFile(b.remote.marker); err != nil || len(reason) == 0 {
		t.Errorf("marker: %q, %v; want the reason", reason, err)
	}
}

// Credentials that have expired are fetched again by the call that finds
// them expired, so the same holds for a lookup, once the bucket goes away.
func TestBreaker_S3CredentialsAnsweringDoNotHideAnUnreachableBucket(t *testing.T) {
	fastRetries(t, 300*time.Millisecond)
	credentials := containerCredentials(t, time.Now().Add(-time.Minute))

	var gone atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gone.Load() {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	b := checkedBucket(t, openS3At(t, srv.URL), t.TempDir())
	if !b.useRemote() {
		t.Fatal("bucket turned off at startup")
	}
	before := credentials.Load()

	gone.Store(true)
	if _, _, err := b.OutputIDFromAction(context.Background(), someAction); err == nil {
		t.Error("lookup nothing answered succeeded")
	}
	if credentials.Load() == before {
		t.Fatal("the lookup didn't fetch credentials")
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
}

// An attempt reached the host it asked last if that host answered it, or,
// for an upload, was sent a request.
func TestTraceAttempt(t *testing.T) {
	answers := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
	}))
	t.Cleanup(answers.Close)
	slowReader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readSlowly(r, new(atomic.Int64), time.Second)
	}))
	t.Cleanup(slowReader.Close)
	silent := "http://" + newSilentListener(t).Addr().String()
	refused := httptest.NewServer(http.NotFoundHandler())
	refused.Close()

	get := func(url string) tracedRequest { return tracedRequest{http.MethodGet, url, 0} }
	put := func(url string) tracedRequest { return tracedRequest{http.MethodPut, url, 1 << 20} }

	tests := map[string]struct {
		requests       []tracedRequest
		lookup, upload bool
	}{
		"answered":                      {[]tracedRequest{get(answers.URL)}, true, true},
		"refused":                       {[]tracedRequest{get(refused.URL)}, false, false},
		"not answered":                  {[]tracedRequest{get(silent)}, false, true},
		"upload read slowly":            {[]tracedRequest{put(slowReader.URL)}, false, true},
		"upload after an answer":        {[]tracedRequest{get(slowReader.URL), put(slowReader.URL)}, true, true},
		"another host answered first":   {[]tracedRequest{get(answers.URL), get(refused.URL)}, false, false},
		"another host answered, silent": {[]tracedRequest{get(answers.URL), get(silent)}, false, true},
		"answered, then another host":   {[]tracedRequest{get(refused.URL), get(answers.URL)}, true, true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, trace := traceAttempt(context.Background())
			client := &http.Client{Transport: &http.Transport{}}
			defer client.CloseIdleConnections()

			for _, req := range tc.requests {
				ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				r, err := http.NewRequestWithContext(ctx, req.method, req.url, bytes.NewReader(make([]byte, req.size)))
				if err != nil {
					t.Fatal(err)
				}
				if resp, err := client.Do(r); err == nil {
					resp.Body.Close()
				}
				cancel()
			}

			if got := trace.reached(false); got != tc.lookup {
				t.Errorf("reached(false) = %v, want %v", got, tc.lookup)
			}
			if got := trace.reached(true); got != tc.upload {
				t.Errorf("reached(true) = %v, want %v", got, tc.upload)
			}
		})
	}
}

type tracedRequest struct {
	method, url string
	size        int
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
	unavailableErr := &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "Service Unavailable"}

	tests := map[string]struct {
		err  error
		want bool
	}{
		"nil":                {nil, false},
		"not found":          {notFound, false},
		"server error":       {unavailableErr, false},
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
		// and one that ran out of time retrying answers
		"deadline retrying 503": {fmt.Errorf("retry failed with %w; last error: %w", context.DeadlineExceeded, unavailableErr), false},
		"deadline retrying 429": {fmt.Errorf("retry failed with %w; last error: %w", context.DeadlineExceeded, &googleapi.Error{Code: http.StatusTooManyRequests}), false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := unreachable(tc.err); got != tc.want {
				t.Errorf("unreachable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetriedAnswers(t *testing.T) {
	unavailableErr := &googleapi.Error{Code: http.StatusServiceUnavailable}
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

	tests := map[string]struct {
		err  error
		want bool
	}{
		"nil":                    {nil, false},
		"server error":           {unavailableErr, false},
		"deadline":               {context.DeadlineExceeded, false},
		"deadline retrying 503":  {fmt.Errorf("retry failed with %w; last error: %w", context.DeadlineExceeded, unavailableErr), true},
		"deadline retrying dial": {fmt.Errorf("retry failed with %w; last error: %w", context.DeadlineExceeded, refused), false},
		"canceled retrying 503":  {fmt.Errorf("retry failed with %w; last error: %w", context.Canceled, unavailableErr), false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := retriedAnswers(tc.err); got != tc.want {
				t.Errorf("retriedAnswers(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
