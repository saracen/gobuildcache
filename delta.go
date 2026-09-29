package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"sync/atomic"
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
//
// Unlike -dir, it's restored by something else, which can leave an output
// damaged: GitLab's cache extraction, for one, writes each file in place and
// carries on after one fails part way. The go command only checks the size
// of most outputs it gets, and uses their bytes as they are, so a truncated
// compiled archive crashes the linker in every later job. So outputs are
// checked against their IDs before they're used; see output.
//
// It can also belong to another user, as when a CI cache restores it with
// the owner it was saved with for a job running as someone else. What the
// job can't read is missing, and what it can't write is kept in -dir, so
// that such a delta costs its benefit rather than failing the go command.
// Files are created with the go command's modes, less the umask, so that
// where a CI system relies on the umask to share caches across users, it
// works for the delta too; see createTemp.
const usedDir = "used"

type delta struct {
	disk *Disk

	// stats, if set, counts damaged outputs and failed reads and writes.
	stats *Stats

	// writable is whether puts and use records can be written; if not,
	// puts are kept in -dir and uses aren't recorded.
	writable bool

	// warned is set once a failed read or write has been logged as a
	// warning; see problem.
	warned atomic.Bool

	// marked is the entries whose use this process has recorded; recording
	// once per process is enough for pruning by the job's start.
	marked sync.Map // actionID -> struct{}

	// verified is the outputs this process has found to match their IDs, so
	// each is hashed at most once per process.
	verified sync.Map // outputID -> struct{}
}

// newDelta returns the delta in dir, creating it if needed. A delta this
// process can't write to is still read, as its outputs are checked, but
// isn't changed.
func newDelta(dir string) (*delta, error) {
	for _, sub := range []string{actionDir, outputDir, usedDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o777); err != nil {
			return nil, fmt.Errorf("creating delta %s dir: %w", sub, err)
		}
	}
	d := &delta{disk: &Disk{cacheDir: dir}, writable: true}

	// puts write temporary files in the delta dir itself, then rename them
	// into action/ and output/
	for _, sub := range []string{"", actionDir, outputDir, usedDir} {
		f, err := createTemp(filepath.Join(dir, sub), ".writable")
		if err != nil {
			slog.Warn("delta dir isn't writable, so it's only read, and puts are kept in -dir", "err", err)
			d.writable = false
			break
		}
		f.Close()
		os.Remove(f.Name())
	}
	return d, nil
}

// problem logs a failed read or write of the delta, after which the entry
// is treated as missing: the first as a warning, and the rest, which usually
// fail for the same reason, at debug level.
func (d *delta) problem(msg string, args ...any) {
	if d.stats != nil {
		d.stats.DeltaErrors.Add(1)
	}
	if d.warned.CompareAndSwap(false, true) {
		slog.Warn(msg+", so the entry is treated as missing from the delta; further delta errors are logged at debug level", args...)
		return
	}
	slog.Debug(msg, args...)
}

// hit returns the path of actionID's output and when it was put if the delta
// has it, recording its use.
func (d *delta) hit(actionID string) (string, time.Time) {
	outputID, putTime, err := d.disk.OutputIDFromAction(context.Background(), actionID)
	if err != nil {
		d.problem("delta lookup failed", "action", actionID, "err", err)
		return "", time.Time{}
	}
	if outputID == "" {
		return "", time.Time{}
	}

	pathname, ok := d.output(outputID)
	if !ok {
		return "", time.Time{}
	}

	// only once the output is known to be good, so that pruning doesn't
	// keep a damaged entry
	d.markUsed(actionID)
	return pathname, putTime
}

// output returns the path of outputID in the delta if it's a regular file
// whose contents hash to outputID. One that doesn't match or can't be read is
// removed, so that its entries are misses the go command computes and puts
// again, and so that it isn't saved again. Anything else there under an
// output's name, such as a symlink a CI cache restored, is removed too: a put
// writes a regular file.
func (d *delta) output(outputID string) (string, bool) {
	pathname := filepath.Join(d.disk.cacheDir, outputDir, outputID)

	fi, err := os.Lstat(pathname)
	if err != nil {
		return "", false
	}
	if !fi.Mode().IsRegular() {
		d.damaged(pathname, fi, "not a regular file")
		return "", false
	}
	if _, ok := d.verified.Load(outputID); ok {
		return pathname, true
	}

	f, err := os.Open(pathname)
	if errors.Is(err, os.ErrNotExist) {
		return "", false
	}
	if err != nil {
		d.damaged(pathname, fi, "can't be read: "+err.Error())
		return "", false
	}
	defer f.Close()

	// what's hashed must be what was checked, and what's removed
	opened, err := f.Stat()
	if err != nil || !os.SameFile(fi, opened) {
		return "", false
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		d.problem("reading delta output failed", "output", outputID, "err", err)
		return "", false
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != outputID {
		d.damaged(pathname, fi, "contents don't match its id")
		return "", false
	}

	d.verified.Store(outputID, struct{}{})
	return pathname, true
}

// damaged removes the damaged output at pathname, unless it has been replaced
// since it was found, as by another process putting it again.
func (d *delta) damaged(pathname string, found os.FileInfo, reason string) {
	slog.Warn("removing damaged delta output", "path", pathname, "reason", reason)
	if d.stats != nil {
		d.stats.DeltaDamaged.Add(1)
	}
	if fi, err := os.Lstat(pathname); err == nil && os.SameFile(fi, found) {
		os.Remove(pathname)
	}
}

// markUsed records that actionID was used now, unless this process already
// has or can't. The time is the record's contents, not its modification
// time, so it doesn't matter whether whatever saves the delta keeps those.
func (d *delta) markUsed(actionID string) {
	if !d.writable {
		return
	}
	if _, loaded := d.marked.LoadOrStore(actionID, struct{}{}); loaded {
		return
	}

	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := writeFileAtomic(filepath.Join(d.disk.cacheDir, usedDir), actionID, now); err != nil {
		d.marked.Delete(actionID)
		d.problem("recording delta entry use failed", "action", actionID, "err", err)
	}
}

// writeFileAtomic writes name in dir by renaming a temporary file into
// place, so readers never see it partly written.
func writeFileAtomic(dir, name, data string) error {
	f, err := createTemp(dir, name+".tmp.")
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

// putDelta stores a put in the delta, returning the path of its output. An
// error means the delta can't take it, and the put belongs in -dir instead.
//
// The go command puts some entries again unchanged every time it uses them,
// such as a test package's generated test main on every "go list -test". An
// entry this job already has is only stored again if its put time is one
// this job's go command expires, as for a test rerun after "go clean
// -testcache", which needs the new put time; otherwise the delta would also
// hold entries from the bucket, which later jobs get from there anyway.
func (c *Cacher) putDelta(ctx context.Context, actionID, outputID string, body io.Reader) (string, error) {
	testExpire := c.bucket.testExpire
	if _, ok := linked(c.delta.disk, actionID, outputID, testExpire); ok {
		if pathname, ok := c.delta.output(outputID); ok {
			c.delta.markUsed(actionID)
			return pathname, nil
		}
	}
	if pathname, ok := linked(c.disk, actionID, outputID, testExpire); ok {
		return pathname, nil
	}

	pathname, err, _ := c.flight.Do("delta-put"+outputID, func() (any, error) {
		// A damaged output is removed here, so this writes the go command's
		// bytes in its place rather than keeping it.
		if pathname, ok := c.delta.output(outputID); ok {
			return pathname, nil
		}
		pathname, existed, err := c.delta.disk.PutOutput(ctx, outputID, body)
		// Either another process put it since, or output couldn't use or
		// remove what's there, and it can't be given to the go command.
		if err == nil && existed {
			if _, ok := c.delta.output(outputID); !ok {
				return "", fmt.Errorf("delta output %s can't be used or replaced", outputID)
			}
		}
		// outputs are content addressed, so only count the bytes once
		if err == nil && !existed {
			c.delta.verified.Store(outputID, struct{}{})
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
	// usedSince removes entries last used before it, unless zero or no
	// entry was used since.
	usedSince time.Time

	// maxSize removes the least recently used entries until their outputs
	// take no more than this many bytes, unless zero.
	maxSize int64
}

type pruneResult struct {
	Kept, Removed           int
	KeptBytes, RemovedBytes int64

	// Unused is set when no entry was used since usedSince, which was then
	// ignored.
	Unused bool

	// Failed counts the files that should have been removed but couldn't
	// be, such as another user's, and Err is the first such error.
	Failed int
	Err    error
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
// If no entry was used since usedSince, it only applies maxSize: a job that
// fails before its go commands run, such as while setting up, uses nothing,
// and removing everything would leave its retry nothing to use.
//
// Only what gobuildcache writes there is removed: entries, their use
// records, temporary files left by processes that were killed, and symlinks
// under an output's name, which gets remove anyway. What can't be removed is
// counted, rather than stopping it, as a job can still use the rest.
func pruneDelta(dir string, opts pruneOptions) (pruneResult, error) {
	var result pruneResult
	remove := func(pathname string) bool {
		err := os.Remove(pathname)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return true
		}
		result.Failed++
		if result.Err == nil {
			result.Err = err
		}
		return false
	}
	removeEntry := func(actionID string) bool {
		ok := remove(filepath.Join(dir, actionDir, actionID))
		remove(filepath.Join(dir, usedDir, actionID))
		return ok
	}

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
			remove(pathname)
			continue
		}
		if !isValidID(name) {
			continue
		}

		outputID, _, err := readActionLink(pathname)
		if err != nil || !isValidID(outputID) {
			if removeEntry(name) {
				result.Removed++
			}
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
		// Anything else under an output's name isn't one, and gets are
		// misses on it; see delta.output.
		pathname := filepath.Join(dir, outputDir, name)
		if fi, err := os.Lstat(pathname); err == nil && fi.Mode().IsRegular() {
			sizes[name] = fi.Size()
		} else if err == nil && fi.Mode()&os.ModeSymlink != 0 {
			remove(pathname)
		}
	}

	usedSince := opts.usedSince
	if !usedSince.IsZero() && len(entries) > 0 && !slices.ContainsFunc(entries, func(e deltaEntry) bool {
		return !e.used.Before(usedSince)
	}) {
		usedSince = time.Time{}
		result.Unused = true
	}

	// most recently used first, so the size cap removes the least
	sortEntries(entries)

	kept := map[string]struct{}{}
	var keptBytes int64
	full := false
	for _, e := range entries {
		size, ok := sizes[e.outputID]
		keep := ok && (usedSince.IsZero() || !e.used.Before(usedSince))
		if _, counted := kept[e.outputID]; counted {
			size = 0
		}
		// once an entry doesn't fit, every less recently used one goes too
		if keep && opts.maxSize > 0 && (full || keptBytes+size > opts.maxSize) {
			keep, full = false, true
		}

		if !keep {
			if removeEntry(e.actionID) {
				result.Removed++
			}
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
		if remove(filepath.Join(dir, outputDir, outputID)) {
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
			remove(filepath.Join(dir, usedDir, name))
			continue
		}
		if !isValidID(name) {
			continue
		}
		if _, err := os.Lstat(filepath.Join(dir, actionDir, name)); errors.Is(err, os.ErrNotExist) {
			remove(filepath.Join(dir, usedDir, name))
		}
	}

	// outputs being written, named by Disk.PutOutput
	root, err := readDirNames(dir)
	if err != nil {
		return result, err
	}
	for _, name := range root {
		if isOutputTempName(name) {
			remove(filepath.Join(dir, name))
		}
	}

	return result, nil
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
