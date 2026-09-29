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
	"strconv"
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

	// putTimeKey is the action link metadata holding when its entry was put,
	// in Unix nanoseconds. It's on the link rather than the output because
	// outputs are content addressed and shared by every action that produces
	// the same bytes, each put at its own time.
	putTimeKey = "put_time"
)

// unknownPutTime is reported for action links uploaded without a put time,
// and with -expire-others for every entry this process didn't put.
// The go command only uses an entry's time to expire test results put before
// the last "go clean -testcache", so a time before any of those makes them
// always expire, rather than risk replaying a result that should rerun. The
// object's modification time isn't a substitute: refreshing rewrites it, so
// it can be later than a "go clean -testcache" that came after the put.
var unknownPutTime = time.Unix(0, 0)

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

	OutputIDFromAction(ctx context.Context, actionID string) (string, time.Time, error)
	LinkActionToOutput(ctx context.Context, actionID, outputID string) error
}

type Disk struct {
	cacheDir string
}

type Bucket struct {
	disk   *Disk
	bucket *blob.Bucket
	jobs   chan queuedJob
	wg     sync.WaitGroup

	// outputs deduplicates output uploads within this process, so that an
	// action link is only ever published after its output is in the bucket.
	outputs sync.Map // outputID -> *outputUpload

	stats  Stats
	remote breaker

	// startup checks the bucket before the process first uses it, when the
	// breaker has a marker; see useRemote.
	startup sync.Once

	// readonly keeps puts local, never uploading them.
	readonly bool

	// testExpire is when the go command last expired test results, or zero
	// if it hasn't; see readTestExpire.
	testExpire time.Time

	// refreshAfter is how old an object must be before a writer that uses it
	// refreshes it; zero disables refreshing. See refresh.go.
	refreshAfter time.Duration
	refreshed    sync.Map // key -> struct{}
	writes       sync.Map // key -> *keyWrites

	closeOnce sync.Once
}

// uploadJob publishes an action link to the bucket once the output it points
// to has been uploaded. Publishing the link first would let another process
// observe an action whose output doesn't exist yet (or never will, if the
// upload fails), turning a would-be hit into a wasted lookup and a miss.
type uploadJob struct {
	actionID string
	outputID string
	putTime  time.Time
}

// queuedJob is one of the kinds of work done in the background.
type queuedJob struct {
	upload  *uploadJob
	refresh *refreshJob
}

type outputUpload struct {
	once sync.Once
	err  error
}

// createTemp creates a file in dir named prefix and a random number, and
// opens it for writing. Unlike os.CreateTemp, whose files only their owner
// can read, it gives the file mode 0666 less the umask, as the go command
// does its cache's files. CI systems that restore caches for jobs running as
// another user depend on that, such as GitLab Runner's Docker executor, which
// runs jobs with umask 0000 so that whatever user a job runs as can use what
// was restored.
func createTemp(dir, prefix string) (*os.File, error) {
	for range 10000 {
		name := filepath.Join(dir, prefix+strconv.FormatUint(uint64(rand.Uint32()), 10))
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return f, err
	}
	return nil, &os.PathError{Op: "createtemp", Path: filepath.Join(dir, prefix+"*"), Err: os.ErrExist}
}

func (d *Disk) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	outputPathname := filepath.Join(d.cacheDir, outputDir, outputID)

	// do nothing if already exists
	if _, err := os.Stat(outputPathname); err == nil {
		return outputPathname, true, nil
	}

	slog.Debug("persisting to disk", "path", outputPathname)

	f, err := createTemp(d.cacheDir, "output")
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

// OutputIDFromAction returns the output ID actionID is linked to and when its
// entry was put, or "" if it isn't linked.
func (d *Disk) OutputIDFromAction(ctx context.Context, actionID string) (string, time.Time, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)

	outputID, putTime, err := readActionLink(actionPathname)
	if errors.Is(err, os.ErrNotExist) {
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, err
	}

	if !isValidID(outputID) {
		return "", time.Time{}, fmt.Errorf("invalid output id %q in action link %s", outputID, actionPathname)
	}
	return outputID, putTime, nil
}

// readActionLink returns the output ID an action link refers to and when its
// entry was put, which is the link's modification time. Links are files
// containing the output ID; older versions used symlinks to the output, which
// aren't reliably available on Windows, and didn't record put times.
func readActionLink(pathname string) (string, time.Time, error) {
	fi, err := os.Lstat(pathname)
	if err != nil {
		return "", time.Time{}, err
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(pathname)
		if err != nil {
			return "", time.Time{}, err
		}
		return filepath.Base(target), unknownPutTime, nil
	}

	// Both come from the same open file: a put renames a new link into place,
	// so reading them separately could pair one put's output with another's
	// time.
	f, err := os.Open(pathname)
	if err != nil {
		return "", time.Time{}, err
	}
	defer f.Close()

	fi, err = f.Stat()
	if err != nil {
		return "", time.Time{}, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return "", time.Time{}, err
	}
	return string(bytes.TrimSpace(data)), fi.ModTime(), nil
}

// LinkActionToOutput links actionID to outputID, recording that the entry was
// put at putTime. If actionID was already linked to outputID, it returns when
// that was put, and otherwise the zero time, but it writes the link
// regardless: the same entry put again, such as a test rerun after "go clean
// -testcache", has a new put time.
func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string, putTime time.Time) (time.Time, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)

	var previous time.Time
	if existing, existingPut, err := readActionLink(actionPathname); err == nil && existing == outputID {
		previous = existingPut
	}

	// Write to a temporary file and rename, so readers never see a partial link
	// or one with the wrong time.
	f, err := createTemp(filepath.Join(d.cacheDir, actionDir), actionID+".tmp.")
	if err != nil {
		return time.Time{}, err
	}
	defer os.Remove(f.Name())

	_, err = f.WriteString(outputID)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chtimes(f.Name(), putTime, putTime)
	}
	if err != nil {
		return time.Time{}, err
	}

	// A legacy symlink would be replaced by the rename on unix, but not on
	// Windows, so remove it first.
	if fi, err := os.Lstat(actionPathname); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		os.Remove(actionPathname)
	}

	if err := os.Rename(f.Name(), actionPathname); err != nil {
		return time.Time{}, err
	}
	return previous, nil
}

// OutputIDFromAction returns the output ID actionID is linked to and when its
// entry was put, from the local cache or else the bucket, or "" if it isn't
// linked.
func (b *Bucket) OutputIDFromAction(ctx context.Context, actionID string) (string, time.Time, error) {
	outputID, putTime, err := b.disk.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("output id from action (disk): %w", err)
	}

	if outputID != "" {
		slog.Debug("returning output id", "action", actionID, "output", outputID)
		return outputID, putTime, nil
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
			return "", time.Time{}, nil
		}
		slog.Debug("empty marker expired", "action", actionID)
	}

	if !b.useRemote() {
		return "", time.Time{}, nil
	}

	b.stats.RemoteLookups.Add(1)
	var attr *blob.Attributes
	err = b.withRetry(ctx, metadataTimeout, func(ctx context.Context) error {
		var err error
		attr, err = b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
		return err
	})
	b.remote.record(err)
	slog.Debug("fetched attributes", "action", actionID, "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		slog.Debug("created found", "action", actionID, "output", outputID)
		if err := os.WriteFile(cacheEmptyOutputPath, nil, 0o600); err != nil {
			slog.Warn("writing empty marker", "action", actionID, "err", err)
		}
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("attribute for %v: %w", actionID, err)
	}

	outputID = attr.Metadata["output_id"]
	if outputID == "" {
		slog.Debug("no metadata output id", "action", actionID, "output", outputID)
		return "", time.Time{}, nil
	}
	if !isValidID(outputID) {
		return "", time.Time{}, fmt.Errorf("invalid output_id %q in bucket metadata for action %s", outputID, actionID)
	}

	putTime = unknownPutTime
	metadata := map[string]string{"output_id": outputID}
	if v, ok := attr.Metadata[putTimeKey]; ok {
		if ns, err := strconv.ParseInt(v, 10, 64); err == nil {
			putTime = time.Unix(0, ns)
			// refreshing on S3 replaces the metadata, so it must carry this too
			metadata[putTimeKey] = v
		} else {
			slog.Debug("invalid put time", "action", actionID, "put_time", v)
		}
	}

	if b.shouldRefresh(attr.ModTime) {
		b.scheduleRefresh(refreshJob{
			key:         path.Join(actionDir, actionID),
			metadata:    metadata,
			contentType: "text/plain",
		})
	}

	slog.Debug("linking action to output from output from action", "action", actionID, "output", outputID)
	if _, err := b.disk.LinkActionToOutput(ctx, actionID, outputID, putTime); err != nil {
		slog.Warn("linking action to output", "action", actionID, "output", outputID, "err", err)
	}

	return outputID, putTime, nil
}

func (b *Bucket) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	putTime := time.Now()
	previous, err := b.disk.LinkActionToOutput(ctx, actionID, outputID, putTime)
	exists := !previous.IsZero()
	if err != nil || b.readonly {
		return exists, err
	}

	// The go command puts some entries again unchanged every time it uses
	// them, such as a test package's generated test main on every "go list
	// -test", and nothing reads their new put time. So an unchanged entry is
	// only uploaded again if its put time is one this job's go command
	// expires, as for a test rerun after "go clean -testcache": others need
	// the new put time to not expire it too.
	if exists && !previous.Before(b.testExpire) {
		return true, nil
	}

	// before queueing, so a refresh from before the put no longer writes it
	b.keyWrites(path.Join(actionDir, actionID)).put.Store(true)

	slog.Debug("scheduling upload", "action", actionID, "output", outputID)
	if err := b.enqueue(queuedJob{upload: &uploadJob{actionID: actionID, outputID: outputID, putTime: putTime}}); err != nil {
		return false, err
	}

	return exists, nil
}

func (b *Bucket) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	return b.disk.PutOutput(ctx, outputID, r)
}

// enqueue sends to the job channel, recovering if a concurrent Close has
// shut it down. A protocol-conformant driver issues no puts after close, but
// a malformed one would otherwise panic the process.
func (b *Bucket) enqueue(job queuedJob) (err error) {
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
	if !b.useRemote() {
		b.stats.UploadsSkipped.Add(1)
		return nil
	}

	u.once.Do(func() {
		u.err = b.uploadOutput(ctx, job.outputID)
	})
	if u.err != nil {
		return fmt.Errorf("uploading output %s: %w", job.outputID, u.err)
	}

	key := path.Join(actionDir, job.actionID)
	w := b.keyWrites(key)
	err := b.withRetry(ctx, metadataTimeout, func(ctx context.Context) error {
		w.mu.Lock()
		defer w.mu.Unlock()

		return b.bucket.Upload(ctx, key, bytes.NewReader(nil), &blob.WriterOptions{
			Metadata: map[string]string{
				"output_id": job.outputID,
				putTimeKey:  strconv.FormatInt(job.putTime.UnixNano(), 10),
			},
			ContentType: "text/plain",
		})
	})
	b.remote.record(err)
	if err != nil {
		return fmt.Errorf("uploading action %s: %w", job.actionID, err)
	}

	return nil
}

func (b *Bucket) uploadOutput(ctx context.Context, outputID string) error {
	key := path.Join(outputDir, outputID)

	local := filepath.Join(b.disk.cacheDir, outputDir, outputID)

	// Outputs are content addressed, so if it's already there it's identical.
	// Writers mostly produce outputs that another job has already uploaded;
	// checking first is a lot cheaper than uploading it again, and it only
	// needs refreshing if it's getting old.
	var attrs *blob.Attributes
	err := b.withRetry(ctx, metadataTimeout, func(ctx context.Context) error {
		var err error
		attrs, err = b.bucket.Attributes(ctx, key)
		return err
	})
	if err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		slog.Debug("checking output exists", "output", outputID, "err", err)
	}
	if err == nil {
		slog.Debug("output already uploaded", "output", outputID)
		b.stats.UploadsSkipped.Add(1)
		if b.shouldRefresh(attrs.ModTime) {
			b.refreshNow(ctx, refreshJob{key: key, contentType: "application/octet-stream", local: local})
		}
		return nil
	}

	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()

	n := &countingReader{r: f}
	err = b.uploadWithRetry(ctx, func(ctx context.Context) error {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		n.n = 0
		return b.bucket.Upload(ctx, key, n, &blob.WriterOptions{ContentType: "application/octet-stream"})
	})
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
	b.jobs = make(chan queuedJob, 1000)

	// 20 workers ought to be enough for anybody
	for range 20 {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			for job := range b.jobs {
				now := time.Now()
				switch {
				case job.upload != nil:
					if err := b.upload(ctx, *job.upload); err != nil {
						b.stats.UploadErrors.Add(1)
						slog.Error("upload", "action", job.upload.actionID, "output", job.upload.outputID, "err", err, "took", time.Since(now))
					} else {
						slog.Debug("uploaded", "action", job.upload.actionID, "output", job.upload.outputID, "took", time.Since(now))
					}

				case job.refresh != nil:
					if err := b.refresh(ctx, *job.refresh); err != nil {
						b.stats.RefreshErrors.Add(1)
						slog.Error("refresh", "key", job.refresh.key, "err", err, "took", time.Since(now))
					} else {
						slog.Debug("refreshed", "key", job.refresh.key, "took", time.Since(now))
					}
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

	if !b.useRemote() {
		return "", nil
	}

	slog.Debug("downloading", "output", outputID)

	f, err := createTemp(b.disk.cacheDir, "output")
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
	var size int64
	var modTime time.Time
	err = b.withRetry(ctx, transferTimeout, func(ctx context.Context) error {
		// start again from nothing if a previous attempt failed part way
		if err := f.Truncate(0); err != nil {
			return err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		h.Reset()

		rdr, err := b.bucket.NewReader(ctx, path.Join(outputDir, outputID), nil)
		if err != nil {
			return err
		}
		defer rdr.Close()

		modTime = rdr.ModTime()
		size, err = io.Copy(io.MultiWriter(f, h), rdr)
		return err
	})
	b.remote.record(err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return "", nil
	}
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

	if b.shouldRefresh(modTime) {
		b.scheduleRefresh(refreshJob{key: path.Join(outputDir, outputID), contentType: "application/octet-stream", local: pathname})
	}

	slog.Debug("downloaded to disk", "output", outputID, "size", size)

	return pathname, nil
}
