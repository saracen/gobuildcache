package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A delta directory keeps what a readonly job computed itself, apart from
// what it downloaded, so a CI system can save it and give it to later jobs
// of the same change, such as the next pipeline of a merge request. Those
// jobs trust the bucket as much as before, and the delta as much as
// whatever saved it.
//
// It's laid out like -dir, with action links and outputs written the same
// way, so go commands sharing it are as safe as ones sharing -dir. It also
// records when each entry was last used, in used/, so that pruning can keep
// only the entries the latest job used.
const usedDir = "used"

type delta struct {
	disk *Disk

	// marked is the entries whose use this process has recorded; recording
	// once per process is enough for pruning by the job's start.
	marked sync.Map // actionID -> struct{}
}

func newDelta(dir string) (*delta, error) {
	for _, sub := range []string{actionDir, outputDir, usedDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("creating delta %s dir: %w", sub, err)
		}
	}
	return &delta{disk: &Disk{cacheDir: dir}}, nil
}

// hit returns the path of actionID's output and when it was put if the delta
// has it, recording its use.
func (d *delta) hit(actionID string) (string, time.Time) {
	outputID, putTime, err := d.disk.OutputIDFromAction(context.Background(), actionID)
	if err != nil {
		slog.Warn("delta lookup", "action", actionID, "err", err)
		return "", time.Time{}
	}
	if outputID == "" {
		return "", time.Time{}
	}

	pathname := filepath.Join(d.disk.cacheDir, outputDir, outputID)
	if _, err := os.Stat(pathname); err != nil {
		return "", time.Time{}
	}

	d.markUsed(actionID)
	return pathname, putTime
}

// markUsed records that actionID was used now, unless this process already
// has. The time is the record's contents, not its modification time, so it
// doesn't matter whether whatever saves the delta keeps those.
func (d *delta) markUsed(actionID string) {
	if _, loaded := d.marked.LoadOrStore(actionID, struct{}{}); loaded {
		return
	}

	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := writeFileAtomic(filepath.Join(d.disk.cacheDir, usedDir), actionID, now); err != nil {
		d.marked.Delete(actionID)
		slog.Warn("recording delta entry use", "action", actionID, "err", err)
	}
}

// writeFileAtomic writes name in dir by renaming a temporary file into
// place, so readers never see it partly written.
func writeFileAtomic(dir, name, data string) error {
	f, err := os.CreateTemp(dir, name+".tmp.*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())

	_, err = f.WriteString(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(dir, name))
}

// readUsed returns when the entry for actionID was last used, or the zero
// time if that isn't recorded.
func readUsed(dir, actionID string) time.Time {
	data, err := os.ReadFile(filepath.Join(dir, usedDir, actionID))
	if err != nil {
		return time.Time{}
	}
	ns, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// linked reports whether actionID is linked to outputID in disk, with the
// output present, and put no earlier than testExpire.
func linked(disk *Disk, actionID, outputID string, testExpire time.Time) (string, bool) {
	existing, putTime, err := disk.OutputIDFromAction(context.Background(), actionID)
	if err != nil || existing != outputID || putTime.Before(testExpire) {
		return "", false
	}
	pathname := filepath.Join(disk.cacheDir, outputDir, outputID)
	if _, err := os.Stat(pathname); err != nil {
		return "", false
	}
	return pathname, true
}

// putDelta stores a put in the delta, returning the path of its output.
//
// The go command puts some entries again unchanged every time it uses them,
// such as a test package's generated test main on every "go list -test". An
// entry this job already has is only stored again if its put time is one
// this job's go command expires, as for a test rerun after "go clean
// -testcache", which needs the new put time; otherwise the delta would also
// hold entries from the bucket, which later jobs get from there anyway.
func (c *Cacher) putDelta(ctx context.Context, actionID, outputID string, body io.Reader) (string, error) {
	testExpire := c.bucket.testExpire
	if pathname, ok := linked(c.delta.disk, actionID, outputID, testExpire); ok {
		c.delta.markUsed(actionID)
		return pathname, nil
	}
	if pathname, ok := linked(c.disk, actionID, outputID, testExpire); ok {
		return pathname, nil
	}

	pathname, err, _ := c.flight.Do("delta-put"+outputID, func() (any, error) {
		pathname, existed, err := c.delta.disk.PutOutput(ctx, outputID, body)
		// outputs are content addressed, so only count the bytes once
		if err == nil && !existed {
			if fi, err := os.Stat(pathname); err == nil {
				c.bucket.stats.DeltaPutBytes.Add(fi.Size())
			}
		}
		return pathname, err
	})
	if err != nil {
		return "", err
	}

	if _, err := c.delta.disk.LinkActionToOutput(ctx, actionID, outputID, time.Now()); err != nil {
		return pathname.(string), fmt.Errorf("linking action to output (delta): %w", err)
	}
	c.delta.markUsed(actionID)
	c.bucket.stats.DeltaPuts.Add(1)

	return pathname.(string), nil
}

type pruneOptions struct {
	// usedSince removes entries last used before it, unless zero.
	usedSince time.Time

	// maxSize removes the least recently used entries until their outputs
	// take no more than this many bytes, unless zero.
	maxSize int64
}

type pruneResult struct {
	Kept, Removed           int
	KeptBytes, RemovedBytes int64
}

type deltaEntry struct {
	actionID string
	outputID string
	used     time.Time
}

// pruneDelta removes the entries in the delta dir that opts don't keep, and
// the outputs no kept entry links to. It must run when no go command is using
// the delta, such as after a job's go commands and before the delta is saved.
//
// Only what gobuildcache writes there is removed: entries, their use
// records, and temporary files left by processes that were killed.
func pruneDelta(dir string, opts pruneOptions) (pruneResult, error) {
	var result pruneResult

	names, err := readDirNames(filepath.Join(dir, actionDir))
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}

	var entries []deltaEntry
	for _, name := range names {
		pathname := filepath.Join(dir, actionDir, name)
		if isTempName(name) {
			os.Remove(pathname)
			continue
		}
		if !isValidID(name) {
			continue
		}

		outputID, _, err := readActionLink(pathname)
		if err != nil || !isValidID(outputID) {
			removeEntry(dir, name)
			continue
		}
		entries = append(entries, deltaEntry{actionID: name, outputID: outputID, used: readUsed(dir, name)})
	}

	sizes := map[string]int64{}
	outputs, err := readDirNames(filepath.Join(dir, outputDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	for _, name := range outputs {
		if !isValidID(name) {
			continue
		}
		if fi, err := os.Stat(filepath.Join(dir, outputDir, name)); err == nil && fi.Mode().IsRegular() {
			sizes[name] = fi.Size()
		}
	}

	// most recently used first, so the size cap removes the least
	sortEntries(entries)

	kept := map[string]struct{}{}
	var keptBytes int64
	full := false
	for _, e := range entries {
		size, ok := sizes[e.outputID]
		keep := ok && (opts.usedSince.IsZero() || !e.used.Before(opts.usedSince))
		if _, counted := kept[e.outputID]; counted {
			size = 0
		}
		// once an entry doesn't fit, every less recently used one goes too
		if keep && opts.maxSize > 0 && (full || keptBytes+size > opts.maxSize) {
			keep, full = false, true
		}

		if !keep {
			removeEntry(dir, e.actionID)
			result.Removed++
			continue
		}
		kept[e.outputID] = struct{}{}
		keptBytes += size
		result.Kept++
	}
	result.KeptBytes = keptBytes

	for outputID, size := range sizes {
		if _, ok := kept[outputID]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, outputDir, outputID)); err == nil {
			result.RemovedBytes += size
		}
	}

	// use records of entries that are gone, and temporary files
	used, err := readDirNames(filepath.Join(dir, usedDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	for _, name := range used {
		if isTempName(name) {
			os.Remove(filepath.Join(dir, usedDir, name))
			continue
		}
		if !isValidID(name) {
			continue
		}
		if _, err := os.Lstat(filepath.Join(dir, actionDir, name)); errors.Is(err, os.ErrNotExist) {
			os.Remove(filepath.Join(dir, usedDir, name))
		}
	}

	// outputs being written, named by Disk.PutOutput
	root, err := readDirNames(dir)
	if err != nil {
		return result, err
	}
	for _, name := range root {
		if isOutputTempName(name) {
			os.Remove(filepath.Join(dir, name))
		}
	}

	return result, nil
}

func removeEntry(dir, actionID string) {
	os.Remove(filepath.Join(dir, actionDir, actionID))
	os.Remove(filepath.Join(dir, usedDir, actionID))
}

func sortEntries(entries []deltaEntry) {
	slices.SortFunc(entries, func(a, b deltaEntry) int {
		if c := b.used.Compare(a.used); c != 0 {
			return c
		}
		return strings.Compare(a.actionID, b.actionID)
	})
}

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

// isTempName reports whether name is a temporary file writeFileAtomic or
// Disk.LinkActionToOutput creates: an ID, ".tmp." and a random number.
func isTempName(name string) bool {
	id, suffix, ok := strings.Cut(name, ".tmp.")
	return ok && isValidID(id) && isDigits(suffix)
}

// isOutputTempName reports whether name is a temporary file Disk.PutOutput
// creates: "output" and a random number.
func isOutputTempName(name string) bool {
	suffix, ok := strings.CutPrefix(name, "output")
	return ok && isDigits(suffix)
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseSize parses a size in bytes, optionally with a KiB, MiB or GiB
// suffix.
func parseSize(s string) (int64, error) {
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}} {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			s, mult = n, u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > (1<<63-1)/mult {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}

// parseTime parses Unix seconds or an RFC 3339 time, such as GitLab's
// CI_JOB_STARTED_AT or the output of "date +%s".
func parseTime(s string) (time.Time, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q: want Unix seconds or RFC 3339", s)
	}
	return t, nil
}
