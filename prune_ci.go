package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// pruneStarted prunes a CI job's delta, after its go commands and before a
// CI cache saves it. started is the file "gobuildcache setup" writes when it
// gives the job the delta, holding when the job started using it.
//
// Without started, the job didn't use the delta, so it's removed, and the CI
// cache has nothing to save and keeps what it had. Otherwise prune keeps what
// the job used since then, unless the job failed: a job that failed may not
// have used everything it needs, such as the tests it didn't get to, so then
// only the size limit applies.
//
// pruneDelta only removes what gobuildcache writes, so anything else a
// restored delta held, or what it couldn't remove, would be saved again. So
// if it fails, or the delta still takes more than twice opts.maxSize on disk,
// the delta is emptied, and the cache saves an empty one in place of the one
// restored. Disk usage counts whole blocks for each file, which a delta of
// small entries adds a lot to, hence twice.
func pruneStarted(dir, started string, failed bool, opts pruneOptions) int {
	data, err := os.ReadFile(started)
	if errors.Is(err, fs.ErrNotExist) {
		slog.Info("gobuildcache prune: the job didn't use the delta, so removing it", "delta_dir", dir)
		if err := os.RemoveAll(dir); err != nil {
			slog.Error("prune", "err", err)
			return 1
		}
		return 0
	}
	if err != nil {
		return emptyDelta(dir, fmt.Sprintf("couldn't read when the job started using it: %v", err))
	}

	if failed {
		slog.Info("gobuildcache prune: the job failed, so only -max-size applies")
	} else if opts.usedSince, err = parseTime(strings.TrimSpace(string(data))); err != nil {
		return emptyDelta(dir, fmt.Sprintf("couldn't read when the job started using it: %v", err))
	}

	result, err := pruneDelta(dir, opts)
	if err != nil {
		return emptyDelta(dir, fmt.Sprintf("couldn't prune it: %v", err))
	}
	reportPrune(result)

	if opts.maxSize > 0 {
		used, err := diskUsage(dir)
		if err != nil {
			return emptyDelta(dir, fmt.Sprintf("couldn't measure it: %v", err))
		}
		if used > 2*opts.maxSize {
			return emptyDelta(dir, fmt.Sprintf("it still takes %d bytes on disk, over twice -max-size", used))
		}
	}
	return 0
}

func emptyDelta(dir, why string) int {
	slog.Warn("gobuildcache prune: emptying the delta, since "+why, "delta_dir", dir)
	if err := os.RemoveAll(dir); err != nil {
		slog.Error("prune", "err", err)
		return 1
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		slog.Error("prune", "err", err)
		return 1
	}
	return 0
}

// diskUsage is what the files under root take on disk, as du counts it.
func diskUsage(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) && path == root {
			return filepath.SkipDir
		}
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += allocated(info)
		return nil
	})
	return total, err
}
