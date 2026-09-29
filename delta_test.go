package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// newDeltaProcess returns a readonly Cacher as a separate process sharing the
// local cache dir, the delta and the bucket would have. maxWait 0 turns
// claims off.
func newDeltaProcess(t *testing.T, dir, deltaDir string, underlying *blob.Bucket, maxWait time.Duration) *Cacher {
	t.Helper()

	c := newCacherIn(t, underlying, dir)
	c.bucket.readonly = true

	d, err := newDelta(deltaDir)
	if err != nil {
		t.Fatal(err)
	}
	d.stats = &c.bucket.stats
	c.delta = d

	if maxWait > 0 {
		claims, err := newClaims(dir, maxWait, &c.bucket.stats)
		if err != nil {
			t.Fatal(err)
		}
		c.claims = claims
		t.Cleanup(claims.releaseAll)
	}
	return c
}

// fillBucket puts content for actionID as a writer would, returning when it
// was put.
func fillBucket(t *testing.T, underlying *blob.Bucket, actionID, content []byte) time.Time {
	t.Helper()

	writer := newCacherOn(t, underlying)
	put(t, writer, actionID, content)
	writer.bucket.Close()

	_, putTime, err := writer.disk.OutputIDFromAction(context.Background(), hex.EncodeToString(actionID))
	if err != nil {
		t.Fatal(err)
	}
	return putTime
}

func deltaHas(t *testing.T, deltaDir string, actionID []byte) bool {
	t.Helper()

	_, err := os.Stat(filepath.Join(deltaDir, actionDir, hex.EncodeToString(actionID)))
	return err == nil
}

func bucketKeys(t *testing.T, underlying *blob.Bucket) []string {
	t.Helper()

	var keys []string
	iter := underlying.List(nil)
	for {
		obj, err := iter.Next(context.Background())
		if err == io.EOF {
			return keys
		}
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, obj.Key)
	}
}

func TestServe_DeltaOptions(t *testing.T) {
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	for _, tc := range []struct {
		name string
		opts options
		want string
	}{
		{name: "without -readonly", opts: options{}, want: "-delta-dir requires -readonly"},
		{name: "with -expire-others", opts: options{readonly: true, expireOthers: true}, want: "-delta-dir can't be used with -expire-others"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.cacheDir, tc.opts.deltaDir = t.TempDir(), t.TempDir()
			err := serve(context.Background(), underlying, tc.opts, strings.NewReader(""), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("serve: %v, want %q", err, tc.want)
			}
		})
	}
}

// TestCacher_DeltaKeepsPutsApart checks that puts go to the delta, not -dir
// or the bucket, and that later gets, by this process or another sharing the
// delta, find them there.
func TestCacher_DeltaKeepsPutsApart(t *testing.T) {
	dir, deltaDir := t.TempDir(), t.TempDir()
	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })

	c := newDeltaProcess(t, dir, deltaDir, underlying, 0)
	actionID := bytes.Repeat([]byte{0xaa}, 32)
	content := []byte("computed by the merge request")
	put(t, c, actionID, content)
	c.bucket.Close()

	if !deltaHas(t, deltaDir, actionID) {
		t.Error("put isn't in the delta")
	}
	if _, err := os.Stat(filepath.Join(dir, actionDir, hex.EncodeToString(actionID))); err == nil {
		t.Error("put is in -dir")
	}
	if keys := bucketKeys(t, underlying); len(keys) != 0 {
		t.Errorf("put reached the bucket: %v", keys)
	}
	if got := c.bucket.stats.DeltaPuts.Load(); got != 1 {
		t.Errorf("delta puts = %d, want 1", got)
	}
	if got := c.bucket.stats.DeltaPutBytes.Load(); got != int64(len(content)) {
		t.Errorf("delta put bytes = %d, want %d", got, len(content))
	}

	for name, p := range map[string]*Cacher{
		"same process":    c,
		"another process": newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0),
	} {
		pathname := get(t, p, actionID)
		if !strings.HasPrefix(pathname, deltaDir) {
			t.Errorf("%s: get = %q, want it from the delta", name, pathname)
		}
		if got := p.bucket.stats.DeltaHits.Load(); got != 1 {
			t.Errorf("%s: delta hits = %d, want 1", name, got)
		}
	}
}

// TestCacher_DeltaLookupOrder checks that the delta is looked in before -dir
// and the bucket, and that entries from the bucket are downloaded to -dir,
// not the delta.
func TestCacher_DeltaLookupOrder(t *testing.T) {
	dir, deltaDir := t.TempDir(), t.TempDir()
	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })

	inBoth := bytes.Repeat([]byte{0xaa}, 32)
	fillBucket(t, underlying, inBoth, []byte("the bucket's"))
	remoteOnly := bytes.Repeat([]byte{0xbb}, 32)
	fillBucket(t, underlying, remoteOnly, []byte("only in the bucket"))

	first := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)
	// the go command only puts after a miss, but a delta can have an entry
	// the bucket got later
	put(t, first, inBoth, []byte("the delta's"))

	c := newDeltaProcess(t, dir, deltaDir, underlying, 0)
	if data, err := os.ReadFile(get(t, c, inBoth)); err != nil || string(data) != "the delta's" {
		t.Errorf("entry in both: got %q, %v, want the delta's", data, err)
	}

	pathname := get(t, c, remoteOnly)
	if want := filepath.Join(dir, outputDir); filepath.Dir(pathname) != want {
		t.Errorf("entry from the bucket at %q, want it in %s", pathname, want)
	}
	if deltaHas(t, deltaDir, remoteOnly) {
		t.Error("entry from the bucket is in the delta")
	}
	if _, err := os.Stat(filepath.Join(deltaDir, usedDir, hex.EncodeToString(remoteOnly))); err == nil {
		t.Error("entry from the bucket has a use recorded in the delta")
	}

	// once -dir has it, the delta still comes first
	if data, err := os.ReadFile(get(t, c, inBoth)); err != nil || string(data) != "the delta's" {
		t.Errorf("entry in both, again: got %q, %v, want the delta's", data, err)
	}
}

// TestCacher_DeltaRePut checks that an entry the go command puts again
// unchanged isn't stored in the delta, unless its put time is one the job's
// go command expires, as for a test rerun after "go clean -testcache".
func TestCacher_DeltaRePut(t *testing.T) {
	ctx := context.Background()
	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })

	fromBucket := bytes.Repeat([]byte{0xaa}, 32)
	content := []byte("test main")
	putTime := fillBucket(t, underlying, fromBucket, content)

	for _, tc := range []struct {
		name       string
		testExpire time.Time
		want       bool
	}{
		{name: "not expired", want: false},
		{name: "expired", testExpire: putTime.Add(time.Nanosecond), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deltaDir := t.TempDir()
			c := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)
			c.bucket.testExpire = tc.testExpire

			get(t, c, fromBucket)
			put(t, c, fromBucket, content)
			if got := deltaHas(t, deltaDir, fromBucket); got != tc.want {
				t.Errorf("re-put of the bucket's entry in the delta = %v, want %v", got, tc.want)
			}

			_, got, err := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0).Get(ctx, &request{ActionID: fromBucket})
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == got.Equal(putTime) {
				t.Errorf("later get put at %v; the bucket's put %v, want a new put time %v", got, putTime, tc.want)
			}

			// and the same for the delta's own entries
			own := bytes.Repeat([]byte{0xbb}, 32)
			c.bucket.testExpire = time.Time{}
			put(t, c, own, content)
			_, first, _ := c.delta.disk.OutputIDFromAction(ctx, hex.EncodeToString(own))
			if tc.want {
				c.bucket.testExpire = first.Add(time.Nanosecond)
			}
			time.Sleep(10 * time.Millisecond)
			put(t, c, own, content)
			_, second, _ := c.delta.disk.OutputIDFromAction(ctx, hex.EncodeToString(own))
			if second.Equal(first) == tc.want {
				t.Errorf("re-put of the delta's entry put at %v, first at %v, want a new put time %v", second, first, tc.want)
			}
		})
	}
}

// TestCacher_DeltaPutTimes checks that entries in the delta are reported as
// put when they were, like entries in -dir.
func TestCacher_DeltaPutTimes(t *testing.T) {
	ctx := context.Background()
	deltaDir := t.TempDir()
	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })

	actionID := bytes.Repeat([]byte{0xaa}, 32)
	putter := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)
	before := time.Now()
	put(t, putter, actionID, []byte("test result"))
	after := time.Now()

	for _, tc := range []struct {
		name string
		c    *Cacher
	}{
		{name: "putting process", c: putter},
		{name: "another process", c: newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, putTime, err := tc.c.Get(ctx, &request{ActionID: actionID})
			if err != nil {
				t.Fatal(err)
			}
			if putTime.Before(before.Add(-time.Second)) || putTime.After(after) {
				t.Errorf("put at %v, want between %v and %v", putTime, before, after)
			}
		})
	}
}

// TestCacher_DeltaDamagedOutput checks that an output in a restored delta
// that isn't a regular file matching its ID, as a failed CI cache extraction
// can leave it, is a miss rather than used, that the go command putting it
// again replaces it, whether or not it got it first, and that pruning
// doesn't keep one that isn't put again.
func TestCacher_DeltaDamagedOutput(t *testing.T) {
	content := []byte("a compiled archive")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, content, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, pathname string)
	}{
		{name: "truncated", damage: func(t *testing.T, pathname string) {
			if err := os.Truncate(pathname, int64(len(content)/2)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "empty", damage: func(t *testing.T, pathname string) {
			if err := os.Truncate(pathname, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "byte changed", damage: func(t *testing.T, pathname string) {
			data := bytes.Clone(content)
			data[3] ^= 0xff
			if err := os.WriteFile(pathname, data, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", damage: func(t *testing.T, pathname string) {
			if err := os.Remove(pathname); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, pathname); err != nil {
				t.Skip("symlinks unavailable:", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actionID := bytes.Repeat([]byte{0xaa}, 32)
			outputPath := func(deltaDir string) string {
				return filepath.Join(deltaDir, outputDir, hashID(content))
			}
			damaged := func(t *testing.T) (string, *blob.Bucket) {
				t.Helper()
				underlying := memblob.OpenBucket(nil)
				t.Cleanup(func() { underlying.Close() })
				deltaDir := t.TempDir()
				put(t, newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0), actionID, content)
				tc.damage(t, outputPath(deltaDir))
				return deltaDir, underlying
			}
			isGood := func(t *testing.T, deltaDir string) {
				t.Helper()
				fi, err := os.Lstat(outputPath(deltaDir))
				if err != nil || !fi.Mode().IsRegular() {
					t.Fatalf("output after putting it again: %v, %v", fi, err)
				}
				if data, err := os.ReadFile(outputPath(deltaDir)); err != nil || !bytes.Equal(data, content) {
					t.Errorf("output after putting it again: %q, %v", data, err)
				}
			}

			t.Run("get then put", func(t *testing.T) {
				deltaDir, underlying := damaged(t)
				c := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)
				putUse := readUsed(deltaDir, hex.EncodeToString(actionID))
				time.Sleep(10 * time.Millisecond)
				if got := get(t, c, actionID); got != "" {
					t.Fatalf("get = %q, want a miss", got)
				}
				if got := c.bucket.stats.DeltaDamaged.Load(); got != 1 {
					t.Errorf("delta damaged = %d, want 1", got)
				}
				if got := readUsed(deltaDir, hex.EncodeToString(actionID)); !got.Equal(putUse) {
					t.Errorf("miss on a damaged output recorded a use at %v", got)
				}
				put(t, c, actionID, content)
				isGood(t, deltaDir)
				if pathname := get(t, newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0), actionID); pathname != outputPath(deltaDir) {
					t.Errorf("get after putting it again = %q, want the delta's", pathname)
				}
			})

			t.Run("put without a get", func(t *testing.T) {
				deltaDir, underlying := damaged(t)
				c := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)
				put(t, c, actionID, content)
				isGood(t, deltaDir)
				if got := c.bucket.stats.DeltaPuts.Load(); got != 1 {
					t.Errorf("delta puts = %d, want 1", got)
				}
			})

			t.Run("prune", func(t *testing.T) {
				deltaDir, underlying := damaged(t)
				start := time.Now()
				time.Sleep(10 * time.Millisecond)
				c := newDeltaProcess(t, t.TempDir(), deltaDir, underlying, 0)
				// another entry is used, so the job used the delta
				put(t, c, bytes.Repeat([]byte{0xbb}, 32), []byte("other"))
				get(t, c, actionID)

				result, err := pruneDelta(deltaDir, pruneOptions{usedSince: start})
				if err != nil {
					t.Fatal(err)
				}
				if result.Kept != 1 || deltaHas(t, deltaDir, actionID) {
					t.Errorf("prune kept the damaged entry: %+v", result)
				}
				if _, err := os.Lstat(outputPath(deltaDir)); err == nil {
					t.Error("prune kept the damaged output")
				}
				if data, err := os.ReadFile(outside); err != nil || !bytes.Equal(data, content) {
					t.Errorf("file a symlinked output named changed: %q, %v", data, err)
				}
			})
		})
	}
}

// TestClaims_DeltaHit checks that a process waiting on another's claim gets
// the entry it puts in the delta they share.
func TestClaims_DeltaHit(t *testing.T) {
	dir, underlying := sharedDir(t)
	deltaDir := t.TempDir()
	a := newDeltaProcess(t, dir, deltaDir, underlying, time.Minute)
	b := newDeltaProcess(t, dir, deltaDir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	if got := get(t, a, actionID); got != "" {
		t.Fatalf("first get = %q, want a miss", got)
	}
	done := awaitGet(t, b, actionID)
	put(t, a, actionID, []byte("compiled once"))

	got := receive(t, done)
	if data, err := os.ReadFile(got.path); err != nil || string(data) != "compiled once" || !strings.HasPrefix(got.path, deltaDir) {
		t.Errorf("waiting process got %q: %q, %v", got.path, data, err)
	}
	if hits := b.bucket.stats.DeltaHits.Load(); hits != 1 {
		t.Errorf("delta hits = %d, want 1", hits)
	}
}

// TestDelta_ConcurrentProcesses checks that processes sharing the delta can
// put and get the same entries at once.
func TestDelta_ConcurrentProcesses(t *testing.T) {
	dir, underlying := sharedDir(t)
	deltaDir := t.TempDir()

	const entries = 20
	var contents, outputIDs [entries][]byte
	for i := range entries {
		contents[i] = []byte(fmt.Sprintf("entry %d", i))
		outputIDs[i] = mustHex(t, hashID(contents[i]))
	}

	var wg sync.WaitGroup
	for range 4 {
		c := newDeltaProcess(t, dir, deltaDir, underlying, 0)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range entries {
				actionID := bytes.Repeat([]byte{byte(i)}, 32)
				content := contents[i]
				if _, err := c.Put(context.Background(), &request{ActionID: actionID, OutputID: outputIDs[i], Body: bytes.NewReader(content), BodySize: int64(len(content))}); err != nil {
					t.Error(err)
					return
				}
				pathname, _, err := c.Get(context.Background(), &request{ActionID: actionID})
				if err != nil {
					t.Error(err)
					return
				}
				if data, err := os.ReadFile(pathname); err != nil || !bytes.Equal(data, content) {
					t.Errorf("entry %d: got %q, %v", i, data, err)
				}
			}
		}()
	}
	wg.Wait()

	result, err := pruneDelta(deltaDir, pruneOptions{usedSince: time.Unix(0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Kept != entries || result.Removed != 0 {
		t.Errorf("prune after concurrent puts: %+v, want %d entries kept", result, entries)
	}
}

// writeDeltaEntry writes an entry to the delta in dir as a put would, with
// its use recorded at used unless that's zero, returning its action ID.
func writeDeltaEntry(t *testing.T, dir string, n byte, content string, used time.Time) string {
	t.Helper()

	d, err := newDelta(dir)
	if err != nil {
		t.Fatal(err)
	}
	actionID := strings.Repeat(fmt.Sprintf("%02x", n), 32)
	outputID := hashID([]byte(content))
	if _, _, err := d.disk.PutOutput(context.Background(), outputID, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.disk.LinkActionToOutput(context.Background(), actionID, outputID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !used.IsZero() {
		if err := writeFileAtomic(filepath.Join(dir, usedDir), actionID, strconv.FormatInt(used.UnixNano(), 10)); err != nil {
			t.Fatal(err)
		}
	}
	return actionID
}

func TestPruneDelta(t *testing.T) {
	jobStart := time.Unix(1700000000, 0)
	earlier, during := jobStart.Add(-time.Hour), jobStart.Add(time.Minute)

	type entry struct {
		content string
		used    time.Time
	}
	for _, tc := range []struct {
		name    string
		entries []entry
		opts    pruneOptions
		want    []int // indexes of the entries kept
		kept    int64
		unused  bool
	}{
		{
			name:    "used since",
			entries: []entry{{"used", during}, {"unused", earlier}, {"at the start", jobStart}, {"never recorded", time.Time{}}},
			opts:    pruneOptions{usedSince: jobStart},
			want:    []int{0, 2},
			kept:    int64(len("used") + len("at the start")),
		},
		{
			name:    "output shared with a removed entry",
			entries: []entry{{"same", during}, {"same", earlier}},
			opts:    pruneOptions{usedSince: jobStart},
			want:    []int{0},
			kept:    int64(len("same")),
		},
		{
			name:    "size cap removes the least recently used",
			entries: []entry{{"aaaa", during}, {"bbbb", during.Add(time.Second)}, {"cccc", earlier}, {"dd", time.Time{}}},
			opts:    pruneOptions{maxSize: 10},
			want:    []int{0, 1},
			kept:    8,
		},
		{
			name:    "size cap counts a shared output once",
			entries: []entry{{"aaaa", during}, {"aaaa", earlier}, {"bbbb", earlier.Add(-time.Second)}},
			opts:    pruneOptions{maxSize: 8},
			want:    []int{0, 1, 2},
			kept:    8,
		},
		{
			name:    "both",
			entries: []entry{{"aaaa", during}, {"bbbb", during.Add(time.Second)}, {"cc", earlier}},
			opts:    pruneOptions{usedSince: jobStart, maxSize: 5},
			want:    []int{1},
			kept:    4,
		},
		{
			// a job that failed before its go commands ran
			name:    "nothing used since",
			entries: []entry{{"aaaa", earlier}, {"bbbb", earlier.Add(-time.Second)}, {"never recorded", time.Time{}}},
			opts:    pruneOptions{usedSince: jobStart},
			want:    []int{0, 1, 2},
			kept:    int64(len("aaaa") + len("bbbb") + len("never recorded")),
			unused:  true,
		},
		{
			name:    "nothing used since, with a size cap",
			entries: []entry{{"aaaa", earlier}, {"bbbb", earlier.Add(-time.Second)}, {"cc", time.Time{}}},
			opts:    pruneOptions{usedSince: jobStart, maxSize: 5},
			want:    []int{0},
			kept:    4,
			unused:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			var ids []string
			for i, e := range tc.entries {
				ids = append(ids, writeDeltaEntry(t, dir, byte(i), e.content, e.used))
			}

			result, err := pruneDelta(dir, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if result.Kept != len(tc.want) || result.Removed != len(tc.entries)-len(tc.want) || result.KeptBytes != tc.kept || result.Unused != tc.unused {
				t.Errorf("result %+v, want %d kept, %d bytes, unused %v", result, len(tc.want), tc.kept, tc.unused)
			}

			keep := map[int]bool{}
			for _, i := range tc.want {
				keep[i] = true
			}
			outputs := map[string]bool{}
			for i, id := range ids {
				_, err := os.Stat(filepath.Join(dir, actionDir, id))
				if kept := err == nil; kept != keep[i] {
					t.Errorf("entry %d %q kept = %v, want %v", i, tc.entries[i].content, kept, keep[i])
				}
				if _, err := os.Stat(filepath.Join(dir, usedDir, id)); err == nil && !keep[i] {
					t.Errorf("entry %d %q: use record kept", i, tc.entries[i].content)
				}
				if keep[i] {
					outputs[hashID([]byte(tc.entries[i].content))] = true
				}
			}
			names, err := os.ReadDir(filepath.Join(dir, outputDir))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range names {
				if !outputs[e.Name()] {
					t.Errorf("output %s kept, which no kept entry links to", e.Name())
				}
				delete(outputs, e.Name())
			}
			if len(outputs) != 0 {
				t.Errorf("outputs of kept entries removed: %v", outputs)
			}
		})
	}
}

// TestPruneDelta_Leftovers checks that pruning removes broken entries and
// what killed processes leave, and nothing else.
func TestPruneDelta_Leftovers(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	kept := writeDeltaEntry(t, dir, 1, "kept", now)
	broken := writeDeltaEntry(t, dir, 2, "output removed", now)
	if err := os.Remove(filepath.Join(dir, outputDir, hashID([]byte("output removed")))); err != nil {
		t.Fatal(err)
	}

	leftovers := []string{
		filepath.Join(actionDir, kept+".tmp.123"),
		filepath.Join(usedDir, kept+".tmp.456"),
		filepath.Join(usedDir, strings.Repeat("ef", 32)), // entry gone
		"output789",
	}
	others := []string{
		filepath.Join(actionDir, "README"),
		filepath.Join(outputDir, "notes.txt"),
		"outputs.txt",
		"go.mod",
	}
	for _, name := range append(append([]string{}, leftovers...), others...) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// a symlink under an output's name, which an entry links to
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("symlinked"), 0o644); err != nil {
		t.Fatal(err)
	}
	symlinked := writeDeltaEntry(t, dir, 3, "symlinked", now)
	symlink := filepath.Join(outputDir, hashID([]byte("symlinked")))
	if err := os.Remove(filepath.Join(dir, symlink)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, symlink)); err == nil {
		leftovers = append(leftovers, symlink, filepath.Join(actionDir, symlinked))
	}

	result, err := pruneDelta(dir, pruneOptions{usedSince: now.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Kept != 1 {
		t.Errorf("result %+v, want 1 kept", result)
	}
	if _, err := os.Stat(target); err != nil {
		t.Errorf("symlinked output's target removed: %v", err)
	}
	for _, name := range append(leftovers, filepath.Join(actionDir, broken), filepath.Join(usedDir, broken)) {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Errorf("%s not removed", name)
		}
	}
	for _, name := range append(others, filepath.Join(actionDir, kept), filepath.Join(usedDir, kept)) {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s removed: %v", name, err)
		}
	}
}

func TestPruneDelta_Missing(t *testing.T) {
	result, err := pruneDelta(filepath.Join(t.TempDir(), "never created"), pruneOptions{maxSize: 1})
	if err != nil || result != (pruneResult{}) {
		t.Errorf("pruning a missing delta: %+v, %v", result, err)
	}
}

// TestDelta_UseRecordedOncePerProcess checks that hits and puts record the
// entry's use, which each process does once.
func TestDelta_UseRecordedOncePerProcess(t *testing.T) {
	dir, underlying := sharedDir(t)
	deltaDir := t.TempDir()
	actionID := bytes.Repeat([]byte{0xaa}, 32)
	id := hex.EncodeToString(actionID)

	before := time.Now()
	put(t, newDeltaProcess(t, dir, deltaDir, underlying, 0), actionID, []byte("put"))
	put1 := readUsed(deltaDir, id)
	if put1.Before(before) {
		t.Fatalf("put's use recorded at %v, want after %v", put1, before)
	}

	c := newDeltaProcess(t, dir, deltaDir, underlying, 0)
	time.Sleep(10 * time.Millisecond)
	get(t, c, actionID)
	hit := readUsed(deltaDir, id)
	if !hit.After(put1) {
		t.Errorf("hit's use recorded at %v, want after the put's %v", hit, put1)
	}
	time.Sleep(10 * time.Millisecond)
	get(t, c, actionID)
	if again := readUsed(deltaDir, id); !again.Equal(hit) {
		t.Errorf("second hit recorded its use again, at %v", again)
	}
}

func TestParseSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
		err  bool
	}{
		{in: "0", want: 0},
		{in: "1234", want: 1234},
		{in: "10KiB", want: 10 << 10},
		{in: "64MiB", want: 64 << 20},
		{in: "2GiB", want: 2 << 30},
		{in: "10MB", err: true},
		{in: "-1", err: true},
		{in: "MiB", err: true},
		{in: "9000000000GiB", err: true},
	} {
		got, err := parseSize(tc.in)
		if (err != nil) != tc.err || got != tc.want {
			t.Errorf("parseSize(%q) = %d, %v, want %d, error %v", tc.in, got, err, tc.want, tc.err)
		}
	}
}

func TestParseTime(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Time
		err  bool
	}{
		{in: "1700000000", want: time.Unix(1700000000, 0)},
		{in: "2026-09-28T10:00:00Z", want: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)},
		{in: "2026-09-28T10:00:00.5+01:00", want: time.Date(2026, 9, 28, 9, 0, 0, 5e8, time.UTC)},
		{in: "yesterday", err: true},
	} {
		got, err := parseTime(tc.in)
		if (err != nil) != tc.err || !got.Equal(tc.want) {
			t.Errorf("parseTime(%q) = %v, %v, want %v, error %v", tc.in, got, err, tc.want, tc.err)
		}
	}
}
