package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"google.golang.org/api/option"
)

// fakeGoogle answers token exchanges and object lookups for every request
// made through http.DefaultTransport, recording what it was sent.
type fakeGoogle struct {
	mu            sync.Mutex
	subjectTokens []string
	storageAuth   []string
	other         []string
}

const fakeAccessToken = "fake-access-token"

func (f *fakeGoogle) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	respond := func(body string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	}

	switch {
	case r.URL.Host == "sts.googleapis.com":
		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		f.subjectTokens = append(f.subjectTokens, r.PostForm.Get("subject_token"))
		return respond(`{"access_token":"` + fakeAccessToken + `","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}`)

	case r.URL.Host == "storage.googleapis.com":
		f.storageAuth = append(f.storageAuth, r.Header.Get("Authorization"))
		return respond(`{"bucket":"bucket","name":"key","metadata":{"output_id":"` + strings.Repeat("b", 64) + `"}}`)
	}

	f.other = append(f.other, r.URL.String())
	return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: io.ErrUnexpectedEOF}
}

// writeExternalAccount writes external_account credentials exchanging the
// subject token "ci-id-token" at tokenURL, as workload identity federation
// from a CI job's ID token does, and returns their path.
func writeExternalAccount(t *testing.T, tokenURL string) string {
	t.Helper()
	dir := t.TempDir()

	subjectToken := filepath.Join(dir, "id_token")
	if err := os.WriteFile(subjectToken, []byte("ci-id-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	creds, err := json.Marshal(map[string]any{
		"type":               "external_account",
		"audience":           "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/pool/providers/provider",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          tokenURL,
		"credential_source":  map[string]string{"file": subjectToken},
	})
	if err != nil {
		t.Fatal(err)
	}
	credsFile := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(credsFile, creds, 0o600); err != nil {
		t.Fatal(err)
	}
	return credsFile
}

// silentListener accepts connections and never answers on them, like a
// service that's overloaded or wedged. It counts what it accepts.
type silentListener struct {
	net.Listener
	accepted atomic.Int64
}

func newSilentListener(t *testing.T) *silentListener {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &silentListener{Listener: l}

	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			s.accepted.Add(1)
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			conn.Close()
		}
	})
	return s
}

func TestOpenBucket_GCSDoesNotWaitForTheMetadataServer(t *testing.T) {
	credsFile := writeExternalAccount(t, "https://sts.googleapis.com/v1/token")

	tests := map[string]struct {
		url      string
		gcloud   bool // in gcloud's file rather than GOOGLE_APPLICATION_CREDENTIALS
		wantAuth string
		wantSTS  []string
	}{
		"anonymous":        {url: "gs://bucket?anonymous=true", wantAuth: ""},
		"external account": {url: "gs://bucket", wantAuth: "Bearer " + fakeAccessToken, wantSTS: []string{"ci-id-token"}},
		"gcloud's file":    {url: "gs://bucket", gcloud: true, wantAuth: "Bearer " + fakeAccessToken, wantSTS: []string{"ci-id-token"}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// An address nothing answers makes the metadata client think
			// it's on GCE, and wait out its retries on every lookup, as in
			// a job container on a GCE VM that can't reach it.
			t.Setenv("GCE_METADATA_HOST", "10.255.255.1")
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)
			t.Setenv("STORAGE_EMULATOR_HOST", "")
			if tc.gcloud {
				home := t.TempDir()
				t.Setenv("HOME", home)
				t.Setenv("APPDATA", home)
				dir := filepath.Join(home, ".config", "gcloud")
				if runtime.GOOS == "windows" {
					dir = filepath.Join(home, "gcloud")
				}
				creds, err := os.ReadFile(credsFile)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "application_default_credentials.json"), creds, 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
			}

			fake := &fakeGoogle{}
			transport := http.DefaultTransport
			http.DefaultTransport = fake
			t.Cleanup(func() { http.DefaultTransport = transport })

			ctx := context.Background()
			start := time.Now()
			bucket, err := openBucket(ctx, tc.url)
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("opening the bucket took %v", took)
			}

			start = time.Now()
			if _, err := bucket.Attributes(ctx, "action/"+strings.Repeat("a", 64)); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("first lookup took %v", took)
			}

			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.storageAuth) != 1 || fake.storageAuth[0] != tc.wantAuth {
				t.Errorf("storage requests' Authorization = %q, want one with %q", fake.storageAuth, tc.wantAuth)
			}
			if strings.Join(fake.subjectTokens, ",") != strings.Join(tc.wantSTS, ",") {
				t.Errorf("token exchanges' subject tokens = %q, want %q", fake.subjectTokens, tc.wantSTS)
			}
			if len(fake.other) != 0 {
				t.Errorf("unexpected requests: %q", fake.other)
			}
		})
	}
}

func TestOpenBucket_OtherSchemesKeepTheirOpeners(t *testing.T) {
	dir := t.TempDir()
	bucket, err := openBucket(context.Background(), "file://"+filepath.ToSlash(dir)+"?prefix=p/1/")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	if err := bucket.WriteAll(context.Background(), "key", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "p", "1", "key")); err != nil {
		t.Errorf("prefix parameter wasn't applied: %v", err)
	}
}

func TestOpenBucket_GCSRejectsInvalidParameters(t *testing.T) {
	t.Setenv("GCE_METADATA_HOST", "10.255.255.1")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("STORAGE_EMULATOR_HOST", "")

	for _, u := range []string{"gs://bucket?anonymous=maybe", "gs://bucket?unknown=1"} {
		if bucket, err := openBucket(context.Background(), u); err == nil {
			bucket.Close()
			t.Errorf("%s: opened, want an error", u)
		}
	}
}

// Failing to start would fail every go command, so without credentials the
// bucket opens, and calls to it fail for the breaker to turn it off.
func TestOpenBucket_GCSWithoutCredentials(t *testing.T) {
	t.Setenv("GCE_METADATA_HOST", "10.255.255.1")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("STORAGE_EMULATOR_HOST", "")

	fake := &fakeGoogle{}
	transport := http.DefaultTransport
	http.DefaultTransport = fake
	t.Cleanup(func() { http.DefaultTransport = transport })

	bucket, err := openBucket(context.Background(), "gs://bucket")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := bucket.Attributes(ctx, "key"); err == nil || ctx.Err() != nil {
		t.Errorf("lookup: err = %v, ctx = %v; want a credentials error before the deadline", err, ctx.Err())
	}
	if len(fake.storageAuth) != 0 {
		t.Errorf("sent %d storage requests without credentials", len(fake.storageAuth))
	}
}

// withoutCredentialsFiles leaves Application Default Credentials nothing but
// the metadata server at host.
func withoutCredentialsFiles(t *testing.T, metadataHost string) {
	t.Helper()
	t.Setenv("GCE_METADATA_HOST", metadataHost)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("HOME", t.TempDir())
	t.Setenv("APPDATA", t.TempDir())
	t.Setenv("STORAGE_EMULATOR_HOST", "")
}

// Without a credentials file, credentials come from the metadata server.
// Opening the bucket happens before gobuildcache answers the go command, and
// nothing bounds it, so it mustn't wait on the metadata server, which job
// containers often can't reach; the startup check bounds the token request.
func TestOpenBucket_GCSMetadataCredentialsUnreachable(t *testing.T) {
	quickStartup(t, 200*time.Millisecond)
	metadata := newSilentListener(t)
	withoutCredentialsFiles(t, metadata.Addr().String())

	start := time.Now()
	underlying, err := openBucket(context.Background(), "gs://bucket")
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("opening the bucket took %v", took)
	}
	// the listener counts connections as it accepts them
	time.Sleep(50 * time.Millisecond)
	if got := metadata.accepted.Load(); got != 0 {
		t.Errorf("opening the bucket connected to the metadata server %d times", got)
	}

	b := checkedBucket(t, underlying, t.TempDir())
	start = time.Now()
	if outputID, _, err := b.OutputIDFromAction(context.Background(), someAction); outputID != "" || err != nil {
		t.Errorf("lookup = %q, %v; want a miss", outputID, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("lookup took %v, want about the startup timeout", took)
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}
}

func TestOpenBucket_GCSMetadataCredentials(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()

		w.Header().Set("Metadata-Flavor", "Google")
		if r.Header.Get("Metadata-Flavor") != "Google" || r.URL.Path != "/computeMetadata/v1/instance/service-accounts/default/token" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"access_token":"metadata-token","expires_in":3600,"token_type":"Bearer"}`)
	}))
	t.Cleanup(metadata.Close)
	withoutCredentialsFiles(t, strings.TrimPrefix(metadata.URL, "http://"))

	// the metadata client has its own transport, so only storage requests
	// come here
	fake := &fakeGoogle{}
	transport := http.DefaultTransport
	http.DefaultTransport = fake
	t.Cleanup(func() { http.DefaultTransport = transport })

	bucket, err := openBucket(context.Background(), "gs://bucket")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	if _, err := bucket.Attributes(context.Background(), "action/"+strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.storageAuth) != 1 || fake.storageAuth[0] != "Bearer metadata-token" {
		t.Errorf("storage requests' Authorization = %q, want the metadata server's token", fake.storageAuth)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 {
		t.Errorf("metadata requests = %q, want only the token", paths)
	}
}

// freezer forwards connections to backend until frozen, then forwards
// nothing more on them, and holds new ones without forwarding anything, as
// a bucket that goes away behind a black hole does. It counts what it
// accepts.
type freezer struct {
	net.Listener
	frozen   atomic.Bool
	accepted atomic.Int64
}

func newFreezer(t *testing.T, backend string) *freezer {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &freezer{Listener: l}

	done := make(chan struct{})
	var mu sync.Mutex
	var held []net.Conn
	hold := func(conn net.Conn) {
		mu.Lock()
		defer mu.Unlock()
		held = append(held, conn)
	}
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if f.frozen.Load() {
				<-done
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil || err != nil {
				return
			}
		}
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			f.accepted.Add(1)
			hold(conn)
			if f.frozen.Load() {
				continue
			}
			upstream, err := net.Dial("tcp", backend)
			if err != nil {
				conn.Close()
				continue
			}
			hold(upstream)
			go pipe(conn, upstream)
			go pipe(upstream, conn)
		}
	}()
	t.Cleanup(func() {
		close(done)
		l.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			conn.Close()
		}
	})
	return f
}

// http2GCS serves gs://bucket over HTTP/2 with handler, which is also sent
// the startup check, and returns the server, with its requests counted.
func http2GCS(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *atomic.Int64) {
	t.Helper()

	var requests atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 {
			requests.Add(1)
		}
		handler(w, r)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, &requests
}

// openGCSAt opens gs://bucket at addr, served by srv, through gcsTransport,
// checked at startup and sharing the result through dir.
func openGCSAt(t *testing.T, srv *httptest.Server, addr, dir string) *Bucket {
	t.Helper()

	transport := gcsTransport().(*http.Transport)
	transport.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	t.Cleanup(transport.CloseIdleConnections)
	underlying, err := gcsblob.OpenBucket(context.Background(), gcp.NewAnonymousHTTPClient(transport), "bucket", &gcsblob.Options{
		ClientOptions: []option.ClientOption{option.WithEndpoint("https://" + addr + "/storage/v1/")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })
	return checkedBucket(t, underlying, dir)
}

// quickPings makes gcsTransport's pings quick for a test.
func quickPings(t *testing.T) {
	t.Helper()
	saved := gcsPingTimeout
	t.Cleanup(func() { gcsPingTimeout = saved })
	gcsPingTimeout = 50 * time.Millisecond
}

// gcsUploads calls upload for multipart uploads, and answers anything else
// not found.
func gcsUploads(upload http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("uploadType") == "multipart" {
			upload(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":{"code":404,"message":"Not Found"}}`)
	}
}

// writeOutput writes a 256KiB output to dir, sent in a single request, and
// returns its ID.
func writeOutput(t *testing.T, dir string) string {
	t.Helper()
	outputID := strings.Repeat("e", 64)
	if err := os.WriteFile(filepath.Join(dir, outputDir, outputID), bytes.Repeat([]byte{'x'}, 256<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	return outputID
}

// Over HTTP/2, an upload's retry goes out on the connection the upload was
// sent on, and a request that was sent counts as reaching the bucket. So if
// the bucket goes away while the upload is sent, the connection must be
// found dead for the retry to find the bucket unreachable.
func TestGCSTransport_UploadToABucketThatGoesAwayTurnsItOff(t *testing.T) {
	quickPings(t)

	var f *freezer
	var uploads atomic.Int64
	srv, http2 := http2GCS(t, gcsUploads(func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		f.frozen.Store(true)
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	f = newFreezer(t, srv.Listener.Addr().String())

	dir := t.TempDir()
	b := openGCSAt(t, srv, f.Addr().String(), dir)
	if !b.useRemote() {
		t.Fatal("bucket turned off at startup")
	}
	fastRetries(t, 400*time.Millisecond)

	if err := b.uploadOutput(context.Background(), writeOutput(t, dir)); err == nil {
		t.Error("upload to a bucket that went away succeeded")
	}
	if http2.Load() == 0 {
		t.Fatal("the bucket wasn't reached over HTTP/2")
	}
	if uploads.Load() == 0 {
		t.Fatal("the upload wasn't sent")
	}
	if got := b.stats.Retries.Load(); got != 1 {
		t.Errorf("retries = %d, want the retry to turn the bucket off", got)
	}
	if got := f.accepted.Load(); got < 2 {
		t.Errorf("connections = %d, want the retry to dial a new one", got)
	}
	if b.remote.allow() {
		t.Error("bucket still in use")
	}
	if reason, err := os.ReadFile(b.remote.marker); err != nil || len(reason) == 0 {
		t.Errorf("marker: %q, %v; want the reason", reason, err)
	}
}

// Pings mustn't take a slow upload's connection for a dead one.
func TestGCSTransport_SlowUploadIsNotShared(t *testing.T) {
	quickPings(t)
	const timeout = 400 * time.Millisecond

	var received, uploads atomic.Int64
	srv, http2 := http2GCS(t, gcsUploads(func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		readSlowly(r, &received, 2*timeout)
	}))

	dir := t.TempDir()
	b := openGCSAt(t, srv, srv.Listener.Addr().String(), dir)
	if !b.useRemote() {
		t.Fatal("bucket turned off at startup")
	}
	fastRetries(t, timeout)

	if err := b.uploadOutput(context.Background(), writeOutput(t, dir)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("upload = %v, want it to run out of time", err)
	}
	if http2.Load() == 0 {
		t.Fatal("the bucket wasn't reached over HTTP/2")
	}
	if received.Load() == 0 {
		t.Fatal("the upload wasn't sent")
	}
	if got := uploads.Load(); got != 1 {
		t.Errorf("uploads = %d, want 1: one that went out isn't tried again", got)
	}
	if !b.remote.allow() {
		t.Error("bucket turned off by a slow upload")
	}
	if _, err := os.Stat(b.remote.marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("slow upload shared as unreachability: %v", err)
	}
}
