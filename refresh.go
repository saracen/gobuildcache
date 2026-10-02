package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gocloud.dev/blob"
)

// Buckets are expected to expire entries some time after they were last
// written, which is the lifecycle rule every provider supports. On its own that
// deletes entries that are used all the time along with ones that aren't, so
// writers refresh the entries they use: at most once per refreshAfter, each is
// rewritten in place, which restarts its expiry.
//
// A refresh is a copy of the object onto itself, done by the provider without
// transferring the object. If the copy fails, the object is uploaded again from
// the local cache instead.

// A refresh writes back what the object held when it was read, so for an
// action link it must not land after this process has put the link since,
// such as for a test rerun after "go clean -testcache": it would restore the
// output and put time the rerun replaced. Once an action link has been put,
// refreshes of it stop, since the put's upload restarts its expiry anyway, and
// a refresh already writing it finishes before the upload writes it.

// keyWrites orders this process's writes of one key.
type keyWrites struct {
	// mu is held while writing the key.
	mu sync.Mutex

	// put is set once this process has put the key.
	put atomic.Bool
}

func (b *Bucket) keyWrites(key string) *keyWrites {
	v, _ := b.writes.LoadOrStore(key, &keyWrites{})
	return v.(*keyWrites)
}

// refreshJob refreshes one object in the bucket.
type refreshJob struct {
	key         string
	metadata    map[string]string
	contentType string

	// local is the object's contents on disk, to upload if it can't be
	// copied. Empty for objects with no contents, such as action links.
	local string
}

// shouldRefresh reports whether an object last written at modTime is due a
// refresh.
func (b *Bucket) shouldRefresh(modTime time.Time) bool {
	return !b.readonly &&
		b.refreshAfter > 0 &&
		!modTime.IsZero() &&
		time.Since(modTime) > b.refreshAfter &&
		b.remote.allow()
}

// scheduleRefresh queues a refresh, once per key per process.
func (b *Bucket) scheduleRefresh(job refreshJob) {
	if _, loaded := b.refreshed.LoadOrStore(job.key, struct{}{}); loaded {
		return
	}

	slog.Debug("scheduling refresh", "key", job.key)
	if err := b.enqueue(queuedJob{refresh: &job}); err != nil {
		slog.Debug("scheduling refresh", "key", job.key, "err", err)
	}
}

// refreshNow refreshes straight away, once per key per process. It's for
// background workers, which can't queue more work once the queue is closing.
func (b *Bucket) refreshNow(ctx context.Context, job refreshJob) {
	if _, loaded := b.refreshed.LoadOrStore(job.key, struct{}{}); loaded {
		return
	}

	if err := b.refresh(ctx, job); err != nil {
		b.stats.RefreshErrors.Add(1)
		slog.Error("refresh", "key", job.key, "err", err)
	}
}

func (b *Bucket) refresh(ctx context.Context, job refreshJob) error {
	if !b.useRemote() {
		return nil
	}

	// write calls op with the key held, unless this process has put it.
	w := b.keyWrites(job.key)
	write := func(op func() error) error {
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.put.Load() {
			return errSuperseded
		}
		return op()
	}

	// Whether a copy works depends on the provider and the object, as well as
	// the bucket: Azure refuses to copy a blob onto itself, and S3 an object
	// over 5 GB. So the copy doesn't count towards the breaker, failing or
	// succeeding; the upload instead does.
	err := b.call(ctx, transferTimeout, callOptions{unrecorded: true}, func(ctx context.Context) error {
		return write(func() error {
			return b.bucket.Copy(ctx, job.key, job.key, &blob.CopyOptions{
				BeforeCopy: func(asFunc func(any) bool) error {
					// S3 refuses to copy an object onto itself unless
					// something about it changes, so have it replace the
					// metadata, with the same.
					var input *s3.CopyObjectInput
					if asFunc(&input) {
						input.MetadataDirective = s3types.MetadataDirectiveReplace
						input.Metadata = job.metadata
						input.ContentType = aws.String(job.contentType)
					}
					return nil
				},
			})
		})
	})
	if errors.Is(err, errSuperseded) {
		slog.Debug("refresh superseded by a put", "key", job.key)
		return nil
	}
	if err == nil {
		b.stats.Refreshes.Add(1)
		return nil
	}

	slog.Debug("refreshing by copy failed, uploading instead", "key", job.key, "err", err)

	var r io.ReadSeeker = bytes.NewReader(nil)
	if job.local != "" {
		f, err := openFile(job.local)
		if err != nil {
			return fmt.Errorf("opening local copy: %w", err)
		}
		defer f.Close()
		r = f
	}

	err = b.uploadWithRetry(ctx, func(ctx context.Context) error {
		return write(func() error {
			if _, err := r.Seek(0, io.SeekStart); err != nil {
				return err
			}
			return b.bucket.Upload(ctx, job.key, r, &blob.WriterOptions{
				Metadata:    job.metadata,
				ContentType: job.contentType,
			})
		})
	})
	if errors.Is(err, errSuperseded) {
		slog.Debug("refresh superseded by a put", "key", job.key)
		return nil
	}
	if err != nil {
		return err
	}

	b.stats.RefreshUploads.Add(1)
	return nil
}
