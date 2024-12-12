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

func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string) error {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)
	outputPathname := filepath.Join("..", outputDir, outputID)

	err := os.Symlink(outputPathname, actionPathname)
	if errors.Is(err, os.ErrExist) {
		return nil
	}

	return err
}

func (b *Bucket) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	outputID, err := b.disk.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", fmt.Errorf("output id from action (disk): %w", err)
	}

	if outputID != "" {
		return outputID, nil
	}

	attr, err := b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
	if gcerrors.Code(err) == gcerrors.NotFound {
		return "", nil
	}

	return attr.Metadata["output_id"], nil
}

func (b *Bucket) LinkActionToOutput(ctx context.Context, actionID, outputID string) error {
	err := b.bucket.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": outputID},
		ContentType: "plain/text",
	})
	if err != nil {
		return err
	}

	return b.disk.LinkActionToOutput(ctx, actionID, outputID)
}

func (b *Bucket) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	pathname, exists, err := b.disk.PutOutput(ctx, outputID, r)
	if err != nil {
		return "", false, err
	}
	if exists {
		return pathname, true, nil
	}

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
					slog.Error("uploading file", "path", pathname, "err", err)
				} else {
					slog.Info("uploaded file", "path", pathname, "took", time.Since(now))
				}
			}
		}()
	}
}

func (b *Bucket) Close() {
	slog.Info("waiting for uploads...")

	now := time.Now()
	close(b.jobs)
	b.wg.Wait()

	slog.Info("waited for uploads", "took", time.Since(now))
}

func (b *Bucket) GetOutput(ctx context.Context, outputID string) (string, error) {
	pathname, err := b.disk.GetOutput(ctx, outputID)
	if err != nil {
		return "", err
	}

	if _, err := os.Stat(pathname); err == nil {
		return pathname, nil
	}

	pr, pw := io.Pipe()
	defer pw.Close()
	defer pr.Close()

	go func() {
		err := b.bucket.Download(ctx, path.Join(outputDir, outputID), pw, &blob.ReaderOptions{})
		if err != nil {
			pw.CloseWithError(err)
		}
		pw.Close()
	}()

	pathname, _, err = b.disk.PutOutput(ctx, outputID, pr)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return "", nil
	}

	return pathname, err
}
