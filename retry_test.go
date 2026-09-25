package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcp"
)

// fastRetries makes retries quick for a test.
func fastRetries(t *testing.T, timeout time.Duration) {
	t.Helper()

	attempts, metadata, transfer, delay := maxAttempts, metadataTimeout, transferTimeout, retryDelay
	t.Cleanup(func() {
		maxAttempts, metadataTimeout, transferTimeout, retryDelay = attempts, metadata, transfer, delay
	})

	maxAttempts, metadataTimeout, transferTimeout, retryDelay = 3, timeout, timeout, time.Millisecond
}

func TestRetryable(t *testing.T) {
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()
	_, notFound := underlying.Attributes(context.Background(), "missing")

	tests := map[string]struct {
		err  error
		want bool
	}{
		"unmapped provider error": {errors.New("503 Service Unavailable"), true},
		"attempt timed out":       {context.DeadlineExceeded, true},
		"not found":               {notFound, false},
		"canceled":                {context.Canceled, false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := retryable(tc.err); got != tc.want {
				t.Errorf("retryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWithRetry_GivesUp(t *testing.T) {
	fastRetries(t, time.Second)
	b := &Bucket{}

	var calls int
	err := b.withRetry(context.Background(), time.Second, func(context.Context) error {
		calls++
		return errors.New("still broken")
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if calls != maxAttempts {
		t.Errorf("calls = %d, want %d", calls, maxAttempts)
	}
	if got := b.stats.Retries.Load(); got != int64(maxAttempts-1) {
		t.Errorf("retries = %d, want %d", got, maxAttempts-1)
	}
}

func TestWithRetry_DoesNotRetryPermanentErrors(t *testing.T) {
	fastRetries(t, time.Second)
	b := &Bucket{}

	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	var calls int
	err := b.withRetry(context.Background(), time.Second, func(ctx context.Context) error {
		calls++
		_, err := underlying.Attributes(ctx, "missing")
		return err
	})
	if err == nil || calls != 1 {
		t.Errorf("err = %v, calls = %d; want not found after 1 call", err, calls)
	}
}

func TestWithRetry_BoundsCallsThatNeverReturn(t *testing.T) {
	fastRetries(t, 20*time.Millisecond)
	b := &Bucket{}

	start := time.Now()
	err := b.withRetry(context.Background(), metadataTimeout, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want deadline exceeded", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("took %v", took)
	}
}

// fakeGCS stands in for the Cloud Storage API, failing the first failures
// requests of the given method with a 503, or all of them if failures is
// negative.
type fakeGCS struct {
	method   string
	failures atomic.Int64
	requests atomic.Int64
}

func (f *fakeGCS) RoundTrip(r *http.Request) (*http.Response, error) {
	f.requests.Add(1)
	if r.Body != nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}

	if r.Method == f.method && (f.failures.Load() < 0 || f.failures.Add(-1) >= 0) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Body: http.NoBody, Request: r}, nil
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"bucket":"bucket","name":"key","metadata":{"output_id":"` + strings.Repeat("b", 64) + `"}}`)),
		Request:    r,
	}, nil
}

func newFakeGCSBucket(t *testing.T, transport *fakeGCS) *Bucket {
	t.Helper()

	underlying, err := gcsblob.OpenBucket(context.Background(), gcp.NewAnonymousHTTPClient(transport), "bucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })

	b := &Bucket{disk: newDisk(t), bucket: underlying}
	b.Start(context.Background())
	t.Cleanup(b.Close)
	return b
}

func TestGCS_UploadRecoversFromA503(t *testing.T) {
	fastRetries(t, 5*time.Second)
	transport := &fakeGCS{method: http.MethodPost}
	transport.failures.Store(1)
	b := newFakeGCSBucket(t, transport)

	content := []byte("output")
	outputID := hashID(content)
	if err := os.WriteFile(filepath.Join(b.disk.cacheDir, outputDir, outputID), content, 0o600); err != nil {
		t.Fatal(err)
	}

	err := b.withRetry(context.Background(), transferTimeout, func(ctx context.Context) error {
		return b.bucket.Upload(ctx, "output/"+outputID, bytes.NewReader(content), &blob.WriterOptions{ContentType: "application/octet-stream"})
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if got := b.stats.Retries.Load(); got != 1 {
		t.Errorf("retries = %d, want 1", got)
	}
	if got := transport.requests.Load(); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
}

func TestGCS_UploadGivesUpOnPersistent503s(t *testing.T) {
	fastRetries(t, 5*time.Second)
	transport := &fakeGCS{method: http.MethodPost}
	transport.failures.Store(-1)
	b := newFakeGCSBucket(t, transport)

	err := b.withRetry(context.Background(), transferTimeout, func(ctx context.Context) error {
		return b.bucket.Upload(ctx, "key", bytes.NewReader(nil), &blob.WriterOptions{ContentType: "text/plain"})
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := transport.requests.Load(); got != int64(maxAttempts) {
		t.Errorf("requests = %d, want %d", got, maxAttempts)
	}
}

// The GCS client retries reads by itself until its context ends, so without
// a timeout per attempt a persistent 503 would never return.
func TestGCS_PersistentReadFailuresReturn(t *testing.T) {
	fastRetries(t, 300*time.Millisecond)
	transport := &fakeGCS{method: http.MethodGet}
	transport.failures.Store(-1)
	b := newFakeGCSBucket(t, transport)

	// so that if the lookup doesn't return, the test can still finish
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() {
		_, err := b.OutputIDFromAction(ctx, strings.Repeat("a", 64))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lookup didn't return")
	}
}

func TestWithRetry_RecordsTiming(t *testing.T) {
	fastRetries(t, time.Second)
	b := &Bucket{}

	release := make(chan struct{})
	done := make(chan struct{})
	for range 3 {
		go func() {
			defer func() { done <- struct{}{} }()
			b.withRetry(context.Background(), time.Second, func(context.Context) error {
				<-release
				time.Sleep(20 * time.Millisecond)
				return nil
			})
		}()
	}

	// wait until all three are in flight together
	for b.stats.remoteInFlight.Load() < 3 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	for range 3 {
		<-done
	}

	if got := b.stats.RemoteCalls.Load(); got != 3 {
		t.Errorf("remote calls = %d, want 3", got)
	}
	if got := b.stats.RemotePeakInFlight.Load(); got != 3 {
		t.Errorf("peak in flight = %d, want 3", got)
	}
	if got := b.stats.RemoteWaitMillis.Load(); got < 60 {
		t.Errorf("remote wait = %dms, want at least 60ms", got)
	}
	if got := b.stats.RemoteSlowestMillis.Load(); got < 20 {
		t.Errorf("slowest = %dms, want at least 20ms", got)
	}
	if got := b.stats.remoteInFlight.Load(); got != 0 {
		t.Errorf("in flight after = %d, want 0", got)
	}
}
