package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

const (
	actionDir = "action"
	outputDir = "output"

	// How long a recorded "this action has no output in the bucket" marker
	// suppresses re-checking. Long enough to prevent hammering during a single
	// build, short enough that a freshly-uploaded entry becomes visible soon.
	emptyMarkerTTL = 10 * time.Minute
)

// isValidID reports whether s is safe to use as a cache ID embedded in a
// filesystem path. IDs we generate are lowercase hex from hex.EncodeToString;
// values arriving from untrusted sources (bucket metadata, on-disk symlink
// targets) must be rejected before they can drive path traversal.
func isValidID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

type OutputInfo struct {
	ID   string
	Path string
	Size int64
	Time int64
}

type Storage interface {
	PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error)
	GetOutput(ctx context.Context, outputID string) (string, error)

	OutputIDFromAction(ctx context.Context, actionID string) (string, error)
	LinkActionToOutput(ctx context.Context, actionID, outputID string) error
}

type Disk struct {
	cacheDir string
}

type Bucket struct {
	disk   *Disk
	bucket *blob.Bucket
	jobs   chan string
	wg     sync.WaitGroup

	closeOnce sync.Once
}

func (d *Disk) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	outputPathname := filepath.Join(d.cacheDir, outputDir, outputID)

	// do nothing if already exists
	if _, err := os.Stat(outputPathname); err == nil {
		return outputPathname, true, nil
	}

	slog.Debug("persisting to disk", "path", outputPathname)

	f, err := os.CreateTemp(d.cacheDir, "output")
	if err != nil {
		return "", false, fmt.Errorf("creating temporary output file: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		return "", false, fmt.Errorf("copying output to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", false, fmt.Errorf("flushing output to disk: %w", err)
	}

	if err := os.Rename(f.Name(), outputPathname); err != nil {
		return "", false, fmt.Errorf("renaming: %w", err)
	}

	return outputPathname, false, nil
}

func (d *Disk) GetOutput(ctx context.Context, outputID string) (string, error) {
	return filepath.Join(d.cacheDir, outputDir, outputID), nil
}

func (d *Disk) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)

	outputPathname, err := os.Readlink(actionPathname)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	outputID := filepath.Base(outputPathname)
	if !isValidID(outputID) {
		return "", fmt.Errorf("invalid output id %q in action symlink %s", outputID, actionPathname)
	}
	return outputID, nil
}

func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)
	outputPathname := filepath.Join("..", outputDir, outputID)

	// Check if existing symlink already points to the correct output
	existing, err := os.Readlink(actionPathname)
	if err == nil && existing == outputPathname {
		return true, nil
	}

	// Atomically create/replace symlink by creating at temp path then renaming
	tmpPathname := fmt.Sprintf("%s.tmp.%x", actionPathname, rand.Uint64())
	// Conceivably the temporary filename could already exist and this would
	// error, but it seems unlikely enough to not worry about.
	if err := os.Symlink(outputPathname, tmpPathname); err != nil {
		return false, err
	}
	if err := os.Rename(tmpPathname, actionPathname); err != nil {
		os.Remove(tmpPathname)
		return false, err
	}
	return false, nil
}

func (b *Bucket) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	outputID, err := b.disk.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", fmt.Errorf("output id from action (disk): %w", err)
	}

	if outputID != "" {
		slog.Debug("returning output id", "action", actionID, "output", outputID)
		return outputID, nil
	}

	// TODO: come up with a better solution for this scenario
	// If we fetch from remote storage and there's nothing there, we store an "empty" link,
	// just so that we don't keep trying to fetch this (it adds latency, only to find nothing).
	// The downside is that if at some point it does exist in remote storage, we might not
	// immediately observe that.
	cacheEmptyOutputPath := filepath.Join(b.disk.cacheDir, actionDir, actionID+".empty")
	if fi, err := os.Stat(cacheEmptyOutputPath); err == nil {
		if time.Since(fi.ModTime()) < emptyMarkerTTL {
			slog.Debug("empty found", "action", actionID, "output", outputID)
			return "", nil
		}
		slog.Debug("empty marker expired", "action", actionID)
	}

	attr, err := b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
	slog.Debug("fetched attributes", "action", actionID, "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		slog.Debug("created found", "action", actionID, "output", outputID)
		if err := os.WriteFile(cacheEmptyOutputPath, nil, 0o600); err != nil {
			slog.Warn("writing empty marker", "action", actionID, "err", err)
		}
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("attribute for %v: %w", actionID, err)
	}

	outputID = attr.Metadata["output_id"]
	if outputID == "" {
		slog.Debug("no metadata output id", "action", actionID, "output", outputID)
		return "", nil
	}
	if !isValidID(outputID) {
		return "", fmt.Errorf("invalid output_id %q in bucket metadata for action %s", outputID, actionID)
	}

	slog.Debug("linking action to output from output from action", "action", actionID, "output", outputID)
	if _, err := b.disk.LinkActionToOutput(ctx, actionID, outputID); err != nil {
		slog.Warn("linking action to output", "action", actionID, "output", outputID, "err", err)
	}

	return outputID, nil
}

func (b *Bucket) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	exists, err := b.disk.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil || exists {
		return exists, err
	}

	return false, b.bucket.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": outputID},
		ContentType: "text/plain",
	})
}

func (b *Bucket) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	pathname, exists, err := b.disk.PutOutput(ctx, outputID, r)
	if err != nil {
		return "", false, err
	}
	if exists {
		return pathname, true, nil
	}

	slog.Debug("scheduling upload", "path", pathname)
	if err := b.enqueueUpload(pathname); err != nil {
		return pathname, false, err
	}

	return pathname, false, nil
}

// enqueueUpload sends to the job channel, recovering if a concurrent Close
// has shut it down. A protocol-conformant driver issues no puts after close,
// but a malformed one would otherwise panic the process.
func (b *Bucket) enqueueUpload(pathname string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("upload queue closed: %v", r)
		}
	}()
	b.jobs <- pathname
	return nil
}

func (b *Bucket) Start(ctx context.Context) {
	// queue up to 1000
	b.jobs = make(chan string, 1000)

	// 20 workers ought to be enough for anybody
	for range 20 {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			for pathname := range b.jobs {
				f, err := os.Open(pathname)
				if err != nil {
					slog.Error("opening file for upload", "path", pathname, "err", err)
					continue
				}

				now := time.Now()
				err = b.bucket.Upload(ctx, path.Join(outputDir, filepath.Base(pathname)), f, &blob.WriterOptions{ContentType: "application/octet-stream"})
				f.Close()
				if err != nil {
					slog.Error("uploading file", "path", pathname, "err", err, "took", time.Since(now))
				} else {
					slog.Debug("uploaded file", "path", pathname, "took", time.Since(now))
				}
			}
		}()
	}
}

func (b *Bucket) Close() {
	b.closeOnce.Do(func() {
		slog.Debug("waiting for uploads...")

		now := time.Now()
		close(b.jobs)
		b.wg.Wait()

		slog.Debug("waited for uploads", "took", time.Since(now))
	})
}

func (b *Bucket) GetOutput(ctx context.Context, outputID string) (string, error) {
	slog.Debug("getting output from disk", "output", outputID)

	pathname, err := b.disk.GetOutput(ctx, outputID)
	if err != nil {
		return "", err
	}

	slog.Debug("got output from disk", "output", outputID, "path", pathname, "err", err)

	if _, err := os.Stat(pathname); err == nil {
		slog.Debug("returning pathname", "output", outputID, "path", pathname)

		return pathname, nil
	}

	slog.Debug("downloading", "output", outputID)

	rdr, err := b.bucket.NewReader(ctx, path.Join(outputDir, outputID), nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer rdr.Close()

	f, err := os.CreateTemp(b.disk.cacheDir, "output")
	if err != nil {
		return "", fmt.Errorf("creating temporary output file: %w", err)
	}
	keep := false
	defer func() {
		f.Close()
		if !keep {
			os.Remove(f.Name())
		}
	}()

	// outputID is the SHA256 of the cached content (per Go's cache protocol).
	// Hash while streaming so a poisoned bucket can't feed mismatched bytes
	// into the build.
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, h), rdr)
	if err != nil {
		return "", fmt.Errorf("downloading output: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("flushing output: %w", err)
	}

	if got := hex.EncodeToString(h.Sum(nil)); got != outputID {
		return "", fmt.Errorf("output %s hash mismatch: got %s", outputID, got)
	}

	if err := os.Rename(f.Name(), pathname); err != nil {
		return "", fmt.Errorf("renaming output: %w", err)
	}
	keep = true

	slog.Debug("downloaded to disk", "output", outputID, "size", size)

	return pathname, nil
}
