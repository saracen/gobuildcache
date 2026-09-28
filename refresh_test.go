package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/s3blob"
)

// newFileBucket returns a Bucket backed by a fileblob bucket, whose object
// modification times can be set with os.Chtimes.
func newFileBucket(t *testing.T, readonly bool) (*Bucket, *blob.Bucket, string) {
	t.Helper()

	dir := t.TempDir()
	underlying, err := fileblob.OpenBucket(dir, &fileblob.Options{NoTempDir: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })

	b := &Bucket{disk: newDisk(t), bucket: underlying, readonly: readonly, refreshAfter: time.Hour}
	b.Start(context.Background())
	t.Cleanup(b.Close)

	return b, underlying, dir
}

// seed puts an action link and its output in the bucket, put and last
// written at modTime.
func seed(t *testing.T, underlying *blob.Bucket, dir, actionID string, content []byte, modTime time.Time) string {
	t.Helper()
	ctx := context.Background()

	outputID := hashID(content)
	if err := underlying.WriteAll(ctx, path.Join(outputDir, outputID), content, &blob.WriterOptions{ContentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}
	if err := underlying.WriteAll(ctx, path.Join(actionDir, actionID), nil, &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": outputID, putTimeKey: strconv.FormatInt(modTime.UnixNano(), 10)},
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{path.Join(outputDir, outputID), path.Join(actionDir, actionID)} {
		if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}

	return outputID
}

func modTime(t *testing.T, underlying *blob.Bucket, key string) time.Time {
	t.Helper()

	attrs, err := underlying.Attributes(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return attrs.ModTime
}

func getThroughCacher(t *testing.T, b *Bucket, actionID string) string {
	t.Helper()
	ctx := context.Background()

	outputID, err := b.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	pathname, err := b.GetOutput(ctx, outputID)
	if err != nil || pathname == "" {
		t.Fatalf("get output: %q, %v", pathname, err)
	}
	return outputID
}

func TestRefresh_OldObjectsAreRefreshedOnHit(t *testing.T) {
	b, underlying, dir := newFileBucket(t, false)

	actionID := strings.Repeat("a", 64)
	old := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	outputID := seed(t, underlying, dir, actionID, []byte("popular"), old)

	getThroughCacher(t, b, actionID)
	b.Close()

	for _, key := range []string{path.Join(actionDir, actionID), path.Join(outputDir, outputID)} {
		if got := modTime(t, underlying, key); !got.After(old.Add(time.Hour)) {
			t.Errorf("%s not refreshed: mod time %v", key, got)
		}
	}
	if got := b.stats.Refreshes.Load(); got != 2 {
		t.Errorf("refreshes = %d, want 2", got)
	}

	// the refreshed copies are intact
	attrs, err := underlying.Attributes(context.Background(), path.Join(actionDir, actionID))
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Metadata["output_id"] != outputID || attrs.Metadata[putTimeKey] != strconv.FormatInt(old.UnixNano(), 10) {
		t.Errorf("action metadata after refresh = %v", attrs.Metadata)
	}
	if got, _ := underlying.ReadAll(context.Background(), path.Join(outputDir, outputID)); string(got) != "popular" {
		t.Errorf("output after refresh = %q", got)
	}

	// refreshing isn't putting
	if got, err := b.disk.PutTime(actionID); err != nil || !got.Equal(old) {
		t.Errorf("put time = %v, %v; want %v", got, err, old)
	}
}

// TestRefresh_S3KeepsPutTime checks that refreshing an action link on S3,
// which replaces its metadata, replaces it with the put time too.
func TestRefresh_S3KeepsPutTime(t *testing.T) {
	actionID := strings.Repeat("a", 64)
	outputID := strings.Repeat("b", 64)
	putTime := strconv.FormatInt(time.Now().Add(-48*time.Hour).UnixNano(), 10)

	var mu sync.Mutex
	var copied http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/"+path.Join(actionDir, actionID) {
			http.NotFound(w, r)
			return
		}
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("Last-Modified", time.Now().Add(-48*time.Hour).UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Amz-Meta-Output_id", outputID)
			w.Header().Set("X-Amz-Meta-Put_time", putTime)
		case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
			mu.Lock()
			copied = r.Header.Clone()
			mu.Unlock()
			io.WriteString(w, `<CopyObjectResult><ETag>"x"</ETag></CopyObjectResult>`)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "id", SecretAccessKey: "secret"}, nil
		}),
	})
	underlying, err := s3blob.OpenBucket(context.Background(), client, "bucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })

	b := &Bucket{disk: newDisk(t), bucket: underlying, refreshAfter: time.Hour}
	b.Start(context.Background())
	if got, err := b.OutputIDFromAction(context.Background(), actionID); err != nil || got != outputID {
		t.Fatalf("OutputIDFromAction = %q, %v", got, err)
	}
	b.Close()

	mu.Lock()
	defer mu.Unlock()
	if copied == nil {
		t.Fatalf("action link wasn't refreshed by copying: %d refreshes, %d errors", b.stats.Refreshes.Load(), b.stats.RefreshErrors.Load())
	}
	if got := copied.Get("X-Amz-Metadata-Directive"); got != "REPLACE" {
		t.Errorf("metadata directive = %q, want REPLACE", got)
	}
	if got := copied.Get("X-Amz-Meta-Output_id"); got != outputID {
		t.Errorf("copied output_id = %q, want %q", got, outputID)
	}
	if got := copied.Get("X-Amz-Meta-Put_time"); got != putTime {
		t.Errorf("copied put_time = %q, want %q", got, putTime)
	}
}

func TestRefresh_RecentObjectsAreLeftAlone(t *testing.T) {
	b, underlying, dir := newFileBucket(t, false)

	actionID := strings.Repeat("a", 64)
	recent := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	outputID := seed(t, underlying, dir, actionID, []byte("fresh"), recent)

	getThroughCacher(t, b, actionID)
	b.Close()

	if got := modTime(t, underlying, path.Join(outputDir, outputID)); !got.Equal(recent) {
		t.Errorf("recent output was rewritten: %v != %v", got, recent)
	}
	if got := b.stats.Refreshes.Load() + b.stats.RefreshUploads.Load(); got != 0 {
		t.Errorf("refreshes = %d, want 0", got)
	}
}

func TestRefresh_ReadonlyNeverRefreshes(t *testing.T) {
	b, underlying, dir := newFileBucket(t, true)

	actionID := strings.Repeat("a", 64)
	old := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	outputID := seed(t, underlying, dir, actionID, []byte("popular"), old)

	getThroughCacher(t, b, actionID)
	b.Close()

	if got := modTime(t, underlying, path.Join(outputDir, outputID)); !got.Equal(old) {
		t.Errorf("readonly rewrote an object: %v != %v", got, old)
	}
}

func TestRefresh_RePutOfOldOutputRefreshesIt(t *testing.T) {
	b, underlying, dir := newFileBucket(t, false)
	ctx := context.Background()

	content := []byte("rebuilt")
	old := time.Now().Add(-48 * time.Hour)
	outputID := seed(t, underlying, dir, strings.Repeat("a", 64), content, old)

	// another action produces the same output
	if _, _, err := b.PutOutput(ctx, outputID, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LinkActionToOutput(ctx, strings.Repeat("c", 64), outputID); err != nil {
		t.Fatal(err)
	}
	b.Close()

	if got := modTime(t, underlying, path.Join(outputDir, outputID)); !got.After(old.Add(time.Hour)) {
		t.Errorf("re-put output not refreshed: mod time %v", got)
	}
	if got := b.stats.Uploads.Load(); got != 0 {
		t.Errorf("uploads = %d, want 0: an existing output should be refreshed, not uploaded", got)
	}
}

func TestRefresh_FallsBackToUploading(t *testing.T) {
	b, underlying, dir := newFileBucket(t, false)
	ctx := context.Background()

	content := []byte("vanishing")
	outputID := seed(t, underlying, dir, strings.Repeat("a", 64), content, time.Now().Add(-48*time.Hour))

	local := filepath.Join(b.disk.cacheDir, outputDir, outputID)
	if err := os.WriteFile(local, content, 0o600); err != nil {
		t.Fatal(err)
	}

	// the object disappears before it's refreshed, so it can't be copied
	key := path.Join(outputDir, outputID)
	if err := underlying.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}

	if err := b.refresh(ctx, refreshJob{key: key, contentType: "application/octet-stream", local: local}); err != nil {
		t.Fatal(err)
	}

	if got, err := underlying.ReadAll(ctx, key); err != nil || string(got) != string(content) {
		t.Errorf("output after fallback = %q, %v", got, err)
	}
	if got := b.stats.RefreshUploads.Load(); got != 1 {
		t.Errorf("refresh uploads = %d, want 1", got)
	}
}
