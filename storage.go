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
	jobs   chan uploadJob
	wg     sync.WaitGroup

	// outputs deduplicates output uploads within this process, so that an
	// action link is only ever published after its output is in the bucket.
	outputs sync.Map // outputID -> *outputUpload

	stats  Stats
	remote breaker

	closeOnce sync.Once
}

// uploadJob publishes an action link to the bucket once the output it points
// to has been uploaded. Publishing the link first would let another process
// observe an action whose output doesn't exist yet (or never will, if the
// upload fails), turning a would-be hit into a wasted lookup and a miss.
type uploadJob struct {
	actionID string
	outputID string
}

type outputUpload struct {
	once sync.Once
	err  error
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

	outputID, err := readActionLink(actionPathname)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	if !isValidID(outputID) {
		return "", fmt.Errorf("invalid output id %q in action link %s", outputID, actionPathname)
	}
	return outputID, nil
}

// readActionLink returns the output ID an action link refers to. Links are
// files containing the output ID; older versions used symlinks to the output,
// which aren't reliably available on Windows.
func readActionLink(pathname string) (string, error) {
	fi, err := os.Lstat(pathname)
	if err != nil {
		return "", err
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(pathname)
		if err != nil {
			return "", err
		}
		return filepath.Base(target), nil
	}

	data, err := os.ReadFile(pathname)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(data)), nil
}

func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)

	if existing, err := readActionLink(actionPathname); err == nil && existing == outputID {
		return true, nil
	}

	// Write to a temporary file and rename, so readers never see a partial link.
	f, err := os.CreateTemp(filepath.Join(d.cacheDir, actionDir), actionID+".tmp.*")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())

	_, err = f.WriteString(outputID)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return false, err
	}

	// A legacy symlink would be replaced by the rename on unix, but not on
	// Windows, so remove it first.
	if fi, err := os.Lstat(actionPathname); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		os.Remove(actionPathname)
	}

	if err := os.Rename(f.Name(), actionPathname); err != nil {
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

	if !b.remote.allow() {
		return "", nil
	}

	b.stats.RemoteLookups.Add(1)
	attr, err := b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
	b.remote.record(err)
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

	slog.Debug("scheduling upload", "action", actionID, "output", outputID)
	if err := b.enqueueUpload(uploadJob{actionID: actionID, outputID: outputID}); err != nil {
		return false, err
	}

	return false, nil
}

func (b *Bucket) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	return b.disk.PutOutput(ctx, outputID, r)
}

// enqueueUpload sends to the job channel, recovering if a concurrent Close
// has shut it down. A protocol-conformant driver issues no puts after close,
// but a malformed one would otherwise panic the process.
func (b *Bucket) enqueueUpload(job uploadJob) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("upload queue closed: %v", r)
		}
	}()
	b.jobs <- job
	return nil
}

func (b *Bucket) upload(ctx context.Context, job uploadJob) error {
	v, _ := b.outputs.LoadOrStore(job.outputID, &outputUpload{})
	u := v.(*outputUpload)
	if !b.remote.allow() {
		b.stats.UploadsSkipped.Add(1)
		return nil
	}

	u.once.Do(func() {
		u.err = b.uploadOutput(ctx, job.outputID)
	})
	if u.err != nil {
		return fmt.Errorf("uploading output %s: %w", job.outputID, u.err)
	}

	err := b.bucket.Upload(ctx, path.Join(actionDir, job.actionID), bytes.NewReader(nil), &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": job.outputID},
		ContentType: "text/plain",
	})
	b.remote.record(err)
	if err != nil {
		return fmt.Errorf("uploading action %s: %w", job.actionID, err)
	}

	return nil
}

func (b *Bucket) uploadOutput(ctx context.Context, outputID string) error {
	key := path.Join(outputDir, outputID)

	// Outputs are content addressed, so if it's already there it's identical.
	// Writers mostly produce outputs that another job has already uploaded;
	// checking first is a lot cheaper than uploading it again.
	exists, err := b.bucket.Exists(ctx, key)
	if err != nil {
		slog.Debug("checking output exists", "output", outputID, "err", err)
	}
	if exists {
		slog.Debug("output already uploaded", "output", outputID)
		b.stats.UploadsSkipped.Add(1)
		return nil
	}

	f, err := os.Open(filepath.Join(b.disk.cacheDir, outputDir, outputID))
	if err != nil {
		return err
	}
	defer f.Close()

	n := &countingReader{r: f}
	err = b.bucket.Upload(ctx, key, n, &blob.WriterOptions{ContentType: "application/octet-stream"})
	b.remote.record(err)
	if err != nil {
		return err
	}

	b.stats.Uploads.Add(1)
	b.stats.UploadBytes.Add(n.n)

	return nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (b *Bucket) Start(ctx context.Context) {
	// queue up to 1000
	b.jobs = make(chan uploadJob, 1000)

	// 20 workers ought to be enough for anybody
	for range 20 {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			for job := range b.jobs {
				now := time.Now()
				if err := b.upload(ctx, job); err != nil {
					b.stats.UploadErrors.Add(1)
					slog.Error("upload", "action", job.actionID, "output", job.outputID, "err", err, "took", time.Since(now))
				} else {
					slog.Debug("uploaded", "action", job.actionID, "output", job.outputID, "took", time.Since(now))
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

	if !b.remote.allow() {
		return "", nil
	}

	slog.Debug("downloading", "output", outputID)

	rdr, err := b.bucket.NewReader(ctx, path.Join(outputDir, outputID), nil)
	b.remote.record(err)
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

	b.stats.Downloads.Add(1)
	b.stats.DownloadBytes.Add(size)

	slog.Debug("downloaded to disk", "output", outputID, "size", size)

	return pathname, nil
}
