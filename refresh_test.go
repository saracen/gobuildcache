package main

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	outputID, _, err := b.OutputIDFromAction(ctx, actionID)
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
	if _, got, err := b.disk.OutputIDFromAction(context.Background(), actionID); err != nil || !got.Equal(old) {
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
	if got, _, err := b.OutputIDFromAction(context.Background(), actionID); err != nil || got != outputID {
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

	if err := b.refresh(ctx, refreshJob{key: key, opts: outputOptions(), local: local}); err != nil {
		t.Fatal(err)
	}

	if got, err := underlying.ReadAll(ctx, key); err != nil || string(got) != string(content) {
		t.Errorf("output after fallback = %q, %v", got, err)
	}
	if got := b.stats.RefreshUploads.Load(); got != 1 {
		t.Errorf("refresh uploads = %d, want 1", got)
	}
}

// fakeS3 is an in-memory S3 bucket named "bucket", served over path-style
// URLs, with just what gobuildcache uses.
type fakeS3 struct {
	mu   sync.Mutex
	objs map[string]fakeObject

	// copyStatus fails copies with this status, and copyCode if it's set or
	// AccessDenied, if it's set, and beforeCopy is called before a copy that
	// isn't failed. copies counts the copies asked for.
	copyStatus int
	copyCode   string
	beforeCopy func()
	copies     atomic.Int64
}

type fakeObject struct {
	body     []byte
	metadata map[string]string
	modTime  time.Time
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch {
	case r.Method == http.MethodHead || r.Method == http.MethodGet:
		f.mu.Lock()
		o, ok := f.objs[key]
		f.mu.Unlock()
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			if r.Method == http.MethodGet {
				io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>no such key</Message></Error>`)
			}
			return
		}
		w.Header().Set("Last-Modified", o.modTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", strconv.Itoa(len(o.body)))
		w.Header().Set("ETag", `"x"`)
		for k, v := range o.metadata {
			w.Header().Set("X-Amz-Meta-"+k, v)
		}
		if r.Method == http.MethodGet {
			w.Write(o.body)
		}

	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		f.copies.Add(1)
		if f.copyStatus != 0 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(f.copyStatus)
			io.WriteString(w, `<Error><Code>`+cmp.Or(f.copyCode, "AccessDenied")+`</Code><Message>copy failed</Message></Error>`)
			return
		}
		if f.beforeCopy != nil {
			f.beforeCopy()
		}
		src, _ := url.PathUnescape(r.Header.Get("X-Amz-Copy-Source"))
		src = strings.TrimPrefix(strings.TrimPrefix(src, "/"), "bucket/")

		f.mu.Lock()
		o, ok := f.objs[src]
		if ok {
			o.modTime = time.Now()
			if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
				o.metadata = fakeMetadata(r.Header)
			}
			f.objs[key] = o
		}
		f.mu.Unlock()
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>no such key</Message></Error>`)
			return
		}
		io.WriteString(w, `<CopyObjectResult><ETag>"x"</ETag></CopyObjectResult>`)

	case r.Method == http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.objs[key] = fakeObject{body: body, metadata: fakeMetadata(r.Header), modTime: time.Now()}
		f.mu.Unlock()
		w.Header().Set("ETag", `"x"`)

	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func fakeMetadata(h http.Header) map[string]string {
	m := map[string]string{}
	for k, v := range h {
		if name, ok := strings.CutPrefix(strings.ToLower(k), "x-amz-meta-"); ok {
			m[name] = v[0]
		}
	}
	return m
}

func (f *fakeS3) object(key string) fakeObject {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.objs[key]
}

func openFakeS3(t *testing.T, f http.Handler) *blob.Bucket {
	t.Helper()

	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		// keeps request bodies plain, rather than with trailing checksums
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "id", SecretAccessKey: "secret"}, nil
		}),
	})
	underlying, err := s3blob.OpenBucket(context.Background(), client, "bucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { underlying.Close() })
	return underlying
}

// getOldLinkOnFakeS3 seeds an action link put two days ago and gets it,
// which queues a refresh of it. It returns the Bucket, whose queued jobs are
// run by the caller.
func getOldLinkOnFakeS3(t *testing.T, f *fakeS3, actionID string) *Bucket {
	t.Helper()

	old := time.Now().Add(-48 * time.Hour)
	content := []byte("ok  \tpkg\t0.010s\n")
	f.objs = map[string]fakeObject{
		// recent, so only the link is due a refresh
		path.Join(outputDir, hashID(content)): {body: content, modTime: time.Now()},
		path.Join(actionDir, actionID): {
			metadata: map[string]string{"output_id": hashID(content), putTimeKey: strconv.FormatInt(old.UnixNano(), 10)},
			modTime:  old,
		},
	}

	b := &Bucket{disk: newDisk(t), bucket: openFakeS3(t, f), refreshAfter: time.Hour, testExpire: time.Now()}
	b.jobs = make(chan queuedJob, 10)

	if got, _, err := b.OutputIDFromAction(context.Background(), actionID); err != nil || got != hashID(content) {
		t.Fatalf("OutputIDFromAction = %q, %v", got, err)
	}
	return b
}

// putRerun puts a new output for actionID, as the go command does when it
// reruns a test whose result it expired, returning its ID.
func putRerun(t *testing.T, b *Bucket, actionID string) string {
	t.Helper()
	ctx := context.Background()

	content := []byte("ok  \tpkg\t0.020s\n")
	if _, _, err := b.PutOutput(ctx, hashID(content), bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LinkActionToOutput(ctx, actionID, hashID(content)); err != nil {
		t.Fatal(err)
	}
	return hashID(content)
}

// TestRefresh_AfterRePutKeepsIt checks that a refresh of an action link queued
// before this process put it again doesn't write back the link the put
// replaced, whether copying or uploading it again.
func TestRefresh_AfterRePutKeepsIt(t *testing.T) {
	for _, tc := range []struct {
		name       string
		copyStatus int
	}{
		{name: "copied"},
		{name: "uploaded", copyStatus: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			actionID := strings.Repeat("a", 64)
			f := &fakeS3{copyStatus: tc.copyStatus}
			b := getOldLinkOnFakeS3(t, f, actionID)
			rerun := putRerun(t, b, actionID)
			close(b.jobs)

			// the upload first, then the refresh queued before it
			var refreshes []refreshJob
			for job := range b.jobs {
				switch {
				case job.upload != nil:
					if err := b.upload(ctx, *job.upload); err != nil {
						t.Fatal(err)
					}
				case job.refresh != nil:
					refreshes = append(refreshes, *job.refresh)
				}
			}
			if len(refreshes) != 1 {
				t.Fatalf("refreshes queued = %d, want 1", len(refreshes))
			}
			if err := b.refresh(ctx, refreshes[0]); err != nil {
				t.Fatal(err)
			}

			if got := f.object(path.Join(actionDir, actionID)).metadata["output_id"]; got != rerun {
				t.Errorf("bucket link's output = %s, want the rerun's %s", got, rerun)
			}
		})
	}
}

// TestRefresh_InFlightFinishesBeforeRePut checks that when this process puts
// an action link while refreshing it, the put's upload waits for the refresh,
// rather than landing first and being overwritten by it.
func TestRefresh_InFlightFinishesBeforeRePut(t *testing.T) {
	ctx := context.Background()
	actionID := strings.Repeat("a", 64)

	copying := make(chan struct{})
	release := make(chan struct{})
	f := &fakeS3{beforeCopy: func() {
		close(copying)
		<-release
	}}
	b := getOldLinkOnFakeS3(t, f, actionID)

	job := <-b.jobs
	if job.refresh == nil {
		t.Fatalf("queued %+v, want a refresh", job)
	}
	refreshed := make(chan error, 1)
	go func() { refreshed <- b.refresh(ctx, *job.refresh) }()
	select {
	case <-copying:
	case <-time.After(10 * time.Second):
		t.Fatal("refresh didn't copy")
	}

	rerun := putRerun(t, b, actionID)
	job = <-b.jobs
	if job.upload == nil {
		t.Fatalf("queued %+v, want an upload", job)
	}
	uploaded := make(chan error, 1)
	go func() { uploaded <- b.upload(ctx, *job.upload) }()

	// give the upload time to land, if it doesn't wait for the copy
	time.Sleep(200 * time.Millisecond)
	close(release)

	if err := <-refreshed; err != nil {
		t.Fatal(err)
	}
	if err := <-uploaded; err != nil {
		t.Fatal(err)
	}
	if got := f.object(path.Join(actionDir, actionID)).metadata["output_id"]; got != rerun {
		t.Errorf("bucket link's output = %s, want the rerun's %s", got, rerun)
	}
}

// A copy's success shows nothing about calls failing meanwhile, so it
// doesn't reset the breaker.
func TestRefresh_CopyDoesNotCountTowardsTheBreaker(t *testing.T) {
	b, underlying, dir := newFileBucket(t, false)
	ctx := context.Background()

	content := []byte("copied")
	outputID := seed(t, underlying, dir, strings.Repeat("a", 64), content, time.Now().Add(-48*time.Hour))
	b.remote.failures.Store(3)

	key := path.Join(outputDir, outputID)
	if err := b.refresh(ctx, refreshJob{key: key, opts: outputOptions()}); err != nil {
		t.Fatal(err)
	}
	if got := b.stats.Refreshes.Load(); got != 1 {
		t.Fatalf("refreshes = %d, want 1", got)
	}
	if got := b.remote.failures.Load(); got != 3 {
		t.Errorf("failures = %d, want 3", got)
	}
}

// A copy the provider refuses, as S3 refuses one of an object over 5 GB, is
// tried once, uploading being its retry, and once copies keep failing,
// refreshes upload without asking.
func TestRefresh_StopsCopyingOnceCopiesKeepFailing(t *testing.T) {
	fastRetries(t, 5*time.Second)
	f := &fakeS3{objs: map[string]fakeObject{}, copyStatus: http.StatusBadRequest, copyCode: "InvalidRequest"}
	b := &Bucket{disk: newDisk(t), bucket: openFakeS3(t, f), refreshAfter: time.Hour}
	ctx := context.Background()

	const refreshes = maxCopyFailures + 2
	for i := range refreshes {
		content := []byte(fmt.Sprintf("output %d", i))
		key := path.Join(outputDir, hashID(content))
		f.objs[key] = fakeObject{body: content, modTime: time.Now().Add(-48 * time.Hour)}
		local := filepath.Join(b.disk.cacheDir, outputDir, hashID(content))
		if err := os.WriteFile(local, content, 0o600); err != nil {
			t.Fatal(err)
		}

		if err := b.refresh(ctx, refreshJob{key: key, opts: outputOptions(), local: local}); err != nil {
			t.Fatal(err)
		}
	}

	if got := f.copies.Load(); got != maxCopyFailures {
		t.Errorf("copies = %d, want %d: one each, until they keep failing", got, maxCopyFailures)
	}
	if got := b.stats.RefreshUploads.Load(); got != refreshes {
		t.Errorf("refresh uploads = %d, want %d", got, refreshes)
	}
	if got := b.remote.failures.Load(); got != 0 {
		t.Errorf("failures = %d, want 0: refused copies say nothing about the bucket", got)
	}
}
