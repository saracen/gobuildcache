package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

func TestIsValidID(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"a", true},
		{"0123456789abcdef", true},
		{strings.Repeat("a", 128), true},
		{strings.Repeat("a", 129), false},
		{"ABC", false},
		{"abcg", false},
		{"abc/def", false},
		{"../etc/passwd", false},
		{"abc.def", false},
		{"abc def", false},
		{"abc\x00def", false},
	}
	for _, tc := range cases {
		if got := isValidID(tc.in); got != tc.want {
			t.Errorf("isValidID(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func newDisk(t *testing.T) *Disk {
	t.Helper()
	dir := t.TempDir()
	for _, sub := range []string{actionDir, outputDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return &Disk{cacheDir: dir}
}

func hashID(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func TestDiskPutOutput_StoresAndDedupes(t *testing.T) {
	d := newDisk(t)
	ctx := context.Background()

	content := []byte("hello world")
	outputID := hashID(content)

	pathname, exists, err := d.PutOutput(ctx, outputID, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected exists=false on first put")
	}
	if want := filepath.Join(d.cacheDir, outputDir, outputID); pathname != want {
		t.Errorf("pathname=%q, want %q", pathname, want)
	}

	got, err := os.ReadFile(pathname)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("on-disk content=%q, want %q", got, content)
	}

	pathname2, exists2, err := d.PutOutput(ctx, outputID, bytes.NewReader([]byte("ignored")))
	if err != nil {
		t.Fatal(err)
	}
	if !exists2 {
		t.Error("expected exists=true on duplicate put")
	}
	if pathname2 != pathname {
		t.Errorf("path differs: %q vs %q", pathname2, pathname)
	}

	// Existing file is untouched (the second put's body must not overwrite).
	got, err = os.ReadFile(pathname)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("dedupe overwrote: got %q, want %q", got, content)
	}
}

func TestDiskGetOutput(t *testing.T) {
	d := newDisk(t)
	got, err := d.GetOutput(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(d.cacheDir, outputDir, "abc"); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDiskOutputIDFromAction_Missing(t *testing.T) {
	d := newDisk(t)
	got, err := d.OutputIDFromAction(context.Background(), "nope")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestDiskOutputIDFromAction_RejectsInvalidLink(t *testing.T) {
	d := newDisk(t)
	actionID := strings.Repeat("a", 64)
	actionPath := filepath.Join(d.cacheDir, actionDir, actionID)
	if err := os.Symlink("../../etc/passwd", actionPath); err != nil {
		t.Fatal(err)
	}
	if _, err := d.OutputIDFromAction(context.Background(), actionID); err == nil {
		t.Error("expected error for invalid symlink target")
	}
}

func TestDiskLinkActionToOutput(t *testing.T) {
	d := newDisk(t)
	ctx := context.Background()

	actionID := strings.Repeat("a", 64)
	outputID := strings.Repeat("b", 64)

	exists, err := d.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected exists=false on first link")
	}

	got, err := d.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != outputID {
		t.Errorf("got %q, want %q", got, outputID)
	}

	// Idempotent for unchanged target.
	exists, err = d.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("expected exists=true on idempotent link")
	}

	// Replaces symlink when target changes.
	newOutput := strings.Repeat("c", 64)
	exists, err = d.LinkActionToOutput(ctx, actionID, newOutput)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected exists=false when target changes")
	}
	got, err = d.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != newOutput {
		t.Errorf("got %q, want %q after relink", got, newOutput)
	}
}

func newBucket(t *testing.T) (*Bucket, *blob.Bucket) {
	t.Helper()
	d := newDisk(t)
	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })
	b := &Bucket{disk: d, bucket: underlying}
	b.Start(context.Background())
	t.Cleanup(b.Close)
	return b, underlying
}

func TestBucketPutOutput_PersistsAndUploads(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	content := []byte("hello bucket")
	outputID := hashID(content)

	pathname, exists, err := b.PutOutput(ctx, outputID, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected exists=false")
	}

	// On-disk first; upload happens via worker.
	if got, err := os.ReadFile(pathname); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, content) {
		t.Errorf("disk content mismatch")
	}

	// Drain the upload queue.
	b.Close()

	got, err := underlying.ReadAll(ctx, path.Join(outputDir, outputID))
	if err != nil {
		t.Fatalf("read uploaded: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("uploaded content mismatch")
	}
}

func TestBucketGetOutput_DownloadsAndPersists(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	content := []byte("downloadable")
	outputID := hashID(content)
	if err := underlying.WriteAll(ctx, path.Join(outputDir, outputID), content, nil); err != nil {
		t.Fatal(err)
	}

	pathname, err := b.GetOutput(ctx, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(b.disk.cacheDir, outputDir, outputID); pathname != want {
		t.Errorf("pathname=%q, want %q", pathname, want)
	}
	if got, err := os.ReadFile(pathname); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, content) {
		t.Errorf("on-disk content mismatch")
	}
}

func TestBucketGetOutput_DiskCacheHit(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	content := []byte("cached")
	outputID := hashID(content)
	pathname := filepath.Join(b.disk.cacheDir, outputDir, outputID)
	if err := os.WriteFile(pathname, content, 0o600); err != nil {
		t.Fatal(err)
	}

	// Bucket is empty — if GetOutput tries to download, we'd see a NotFound miss
	// (returning ""). Disk hit avoids that.
	got, err := b.GetOutput(ctx, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if got != pathname {
		t.Errorf("got %q, want %q", got, pathname)
	}

	// Verify by uploading a different blob and confirming disk cache wins.
	if err := underlying.WriteAll(ctx, path.Join(outputDir, outputID), []byte("other"), nil); err != nil {
		t.Fatal(err)
	}
	got2, err := b.GetOutput(ctx, outputID)
	if err != nil {
		t.Fatal(err)
	}
	on, err := os.ReadFile(got2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(on, content) {
		t.Errorf("disk cache was overwritten by remote content")
	}
}

func TestBucketGetOutput_HashMismatch(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	realContent := []byte("good")
	outputID := hashID(realContent)
	if err := underlying.WriteAll(ctx, path.Join(outputDir, outputID), []byte("evil!"), nil); err != nil {
		t.Fatal(err)
	}

	_, err := b.GetOutput(ctx, outputID)
	if err == nil {
		t.Fatal("expected hash mismatch error")
	}
	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Errorf("err=%v, want hash mismatch", err)
	}

	// Corrupt content must not have been persisted.
	pathname := filepath.Join(b.disk.cacheDir, outputDir, outputID)
	if _, err := os.Stat(pathname); !errors.Is(err, fs.ErrNotExist) {
		t.Error("hash-mismatched file was persisted to disk")
	}
}

func TestBucketGetOutput_NotFound(t *testing.T) {
	ctx := context.Background()
	b, _ := newBucket(t)
	got, err := b.GetOutput(ctx, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("got %q, want empty on miss", got)
	}
}

func TestBucketOutputIDFromAction_RejectsBadMetadata(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	actionID := strings.Repeat("a", 64)
	if err := underlying.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"output_id": "../../etc/passwd"},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := b.OutputIDFromAction(ctx, actionID); err == nil {
		t.Error("expected error for invalid metadata output_id")
	}
}

func TestBucketOutputIDFromAction_FromMetadata(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	actionID := strings.Repeat("a", 64)
	outputID := strings.Repeat("b", 64)
	if err := underlying.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"output_id": outputID},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := b.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != outputID {
		t.Errorf("got %q, want %q", got, outputID)
	}

	// Disk symlink should now exist for fast subsequent lookups.
	diskGot, err := b.disk.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if diskGot != outputID {
		t.Errorf("disk got %q, want %q", diskGot, outputID)
	}
}

func TestBucketOutputIDFromAction_EmptyMetadata(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	actionID := strings.Repeat("a", 64)
	if err := underlying.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}

	got, err := b.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("got %q, want empty when metadata absent", got)
	}
}

func TestBucketOutputIDFromAction_EmptyMarkerTTL(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	actionID := strings.Repeat("a", 64)

	// First call: bucket miss writes marker.
	got, err := b.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("got %q, want empty on first miss", got)
	}
	markerPath := filepath.Join(b.disk.cacheDir, actionDir, actionID+".empty")
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("expected marker at %s: %v", markerPath, err)
	}

	// Now upload an action so the bucket would resolve. Within TTL, marker
	// suppresses the lookup.
	expected := strings.Repeat("b", 64)
	if err := underlying.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"output_id": expected},
	}); err != nil {
		t.Fatal(err)
	}
	got, err = b.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("expected marker to suppress, got %q", got)
	}

	// Backdate marker past TTL; lookup should now go through.
	past := time.Now().Add(-2 * emptyMarkerTTL)
	if err := os.Chtimes(markerPath, past, past); err != nil {
		t.Fatal(err)
	}
	got, err = b.OutputIDFromAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != expected {
		t.Errorf("got %q, want %q after TTL expiry", got, expected)
	}
}

func TestBucketLinkActionToOutput(t *testing.T) {
	ctx := context.Background()
	b, underlying := newBucket(t)

	actionID := strings.Repeat("a", 64)
	outputID := strings.Repeat("b", 64)

	exists, err := b.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected exists=false on first link")
	}

	attrs, err := underlying.Attributes(ctx, path.Join(actionDir, actionID))
	if err != nil {
		t.Fatalf("bucket Attributes: %v", err)
	}
	if attrs.Metadata["output_id"] != outputID {
		t.Errorf("metadata=%v, want output_id=%s", attrs.Metadata, outputID)
	}

	// Idempotent path skips the bucket upload but still reports exists=true.
	exists, err = b.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("expected exists=true on idempotent link")
	}
}

func TestBucketCloseIdempotent(t *testing.T) {
	b, _ := newBucket(t)
	b.Close()
	b.Close() // would panic without sync.Once
}

func TestBucketPutAfterClose(t *testing.T) {
	ctx := context.Background()
	b, _ := newBucket(t)
	b.Close()

	content := []byte("hi")
	_, _, err := b.PutOutput(ctx, hashID(content), bytes.NewReader(content))
	if err == nil {
		t.Error("expected error from PutOutput after Close")
	}
}
