package main

import (
	"bytes"
	"context"
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
)

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
	defer os.RemoveAll(f.Name())
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

	return filepath.Base(outputPathname), nil
}

func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)
	outputPathname := filepath.Join("..", outputDir, outputID)

	err := os.Symlink(outputPathname, actionPathname)
	if errors.Is(err, os.ErrExist) {
		return true, nil
	}

	return false, err
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
	if _, err := os.Stat(cacheEmptyOutputPath); err == nil {
		slog.Debug("empty found", "action", actionID, "output", outputID)
		return "", nil
	}

	attr, err := b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
	slog.Debug("fetched attributes", "action", actionID, "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		slog.Debug("created found", "action", actionID, "output", outputID)
		os.WriteFile(cacheEmptyOutputPath, nil, 0o600)
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

	slog.Debug("linking action to output from output from action", "action", actionID, "output", outputID)
	b.disk.LinkActionToOutput(ctx, actionID, outputID)

	return outputID, nil
}

func (b *Bucket) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	exists, err := b.disk.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil || exists {
		return exists, err
	}

	return false, b.bucket.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": outputID},
		ContentType: "plain/text",
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
	b.jobs <- pathname

	return pathname, false, nil
}

func (b *Bucket) Start(ctx context.Context) {
	// queue up to 1000
	b.jobs = make(chan string, 1000)

	// 20 workers ought to be enough for anybody
	for i := 0; i < 20; i++ {
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
	slog.Debug("waiting for uploads...")

	now := time.Now()
	close(b.jobs)
	b.wg.Wait()

	slog.Debug("waited for uploads", "took", time.Since(now))
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

	buf := new(bytes.Buffer)
	err = b.bucket.Download(ctx, path.Join(outputDir, outputID), buf, &blob.ReaderOptions{})
	slog.Debug("downloaded", "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	slog.Debug("putting download to disk", "output", outputID, "size", buf.Len())

	pathname, _, err = b.disk.PutOutput(ctx, outputID, bytes.NewReader(buf.Bytes()))

	slog.Debug("putting download to disk done", "output", outputID, "size", buf.Len())

	return pathname, err
}
