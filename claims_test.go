package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// newProcess returns a Cacher as a separate process sharing dir and bucket
// would have.
func newProcess(t *testing.T, dir string, underlying *blob.Bucket, maxWait time.Duration) *Cacher {
	t.Helper()

	for _, sub := range []string{actionDir, outputDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	c := &Cacher{disk: &Disk{cacheDir: dir}}
	c.bucket = &Bucket{disk: c.disk, bucket: underlying}
	c.bucket.Start(context.Background())
	t.Cleanup(c.bucket.Close)

	claims, err := newClaims(dir, maxWait, &c.bucket.stats)
	if err != nil {
		t.Fatal(err)
	}
	c.claims = claims
	t.Cleanup(claims.releaseAll)

	return c
}

func sharedDir(t *testing.T) (string, *blob.Bucket) {
	t.Helper()

	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })
	return t.TempDir(), underlying
}

func get(t *testing.T, c *Cacher, actionID []byte) string {
	t.Helper()

	pathname, _, err := c.Get(context.Background(), &request{ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	return pathname
}

func put(t *testing.T, c *Cacher, actionID, content []byte) {
	t.Helper()

	outputID := bytes.Repeat([]byte{0}, 32)
	copy(outputID, mustHex(t, hashID(content)))
	_, err := c.Put(context.Background(), &request{ActionID: actionID, OutputID: outputID, Body: bytes.NewReader(content), BodySize: int64(len(content))})
	if err != nil {
		t.Fatal(err)
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestClaims_WaitsForTheProcessComputingIt(t *testing.T) {
	dir, underlying := sharedDir(t)
	a := newProcess(t, dir, underlying, time.Minute)
	b := newProcess(t, dir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	if got := get(t, a, actionID); got != "" {
		t.Fatalf("first get = %q, want a miss", got)
	}

	done := make(chan string, 1)
	go func() { done <- get(t, b, actionID) }()

	select {
	case got := <-done:
		t.Fatalf("second process didn't wait, got %q", got)
	case <-time.After(200 * time.Millisecond):
	}

	put(t, a, actionID, []byte("compiled once"))

	select {
	case got := <-done:
		if data, err := os.ReadFile(got); err != nil || string(data) != "compiled once" {
			t.Errorf("second process got %q: %q, %v", got, data, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second process didn't get the put entry")
	}

	if got := b.bucket.stats.ClaimHits.Load(); got != 1 {
		t.Errorf("claim hits = %d, want 1", got)
	}
}

func TestClaims_ReleasedWithoutPut(t *testing.T) {
	dir, underlying := sharedDir(t)
	a := newProcess(t, dir, underlying, time.Minute)
	b := newProcess(t, dir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	get(t, a, actionID)

	done := make(chan string, 1)
	go func() { done <- get(t, b, actionID) }()
	time.Sleep(100 * time.Millisecond)

	// the first process exits without putting it, so the second computes it
	a.claims.releaseAll()

	select {
	case got := <-done:
		if got != "" {
			t.Errorf("got %q, want a miss", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second process kept waiting after the claim was released")
	}

	if !b.claims.owns(hex.EncodeToString(actionID)) {
		t.Error("second process should now hold the claim")
	}
}

func TestClaims_DeadHolderIsIgnored(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process liveness isn't checked on windows")
	}

	dir, underlying := sharedDir(t)
	b := newProcess(t, dir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skipf("running true: %v", err)
	}
	dead := strconv.Itoa(cmd.Process.Pid)
	if err := os.WriteFile(b.claims.path(hex.EncodeToString(actionID)), []byte(dead), 0o644); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if got := get(t, b, actionID); got != "" {
		t.Errorf("got %q, want a miss", got)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("took %v to take over a dead process's claim", took)
	}
}

func TestClaims_TimeoutStopsFurtherWaits(t *testing.T) {
	dir, underlying := sharedDir(t)
	a := newProcess(t, dir, underlying, time.Minute)
	b := newProcess(t, dir, underlying, 100*time.Millisecond)
	c := newProcess(t, dir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	// the go command looks up some entries it never puts
	get(t, a, actionID)

	start := time.Now()
	if got := get(t, b, actionID); got != "" {
		t.Errorf("got %q, want a miss", got)
	}
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Errorf("gave up after %v, before the timeout", took)
	}
	if got := b.bucket.stats.ClaimTimeouts.Load(); got != 1 {
		t.Errorf("claim timeouts = %d, want 1", got)
	}

	start = time.Now()
	get(t, c, actionID)
	if took := time.Since(start); took > time.Second {
		t.Errorf("waited %v for an action already known not to be put", took)
	}
}

func TestClaims_NeverWaitsOnItself(t *testing.T) {
	dir, underlying := sharedDir(t)
	a := newProcess(t, dir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	get(t, a, actionID)

	start := time.Now()
	if got := get(t, a, actionID); got != "" {
		t.Errorf("got %q, want a miss", got)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("waited %v on its own claim", took)
	}
}

// awaitGet starts a get of actionID with c, which must wait on another
// process's claim, and returns what it gets.
func awaitGet(t *testing.T, c *Cacher, actionID []byte) <-chan claimHit {
	t.Helper()

	done := make(chan claimHit, 1)
	go func() {
		pathname, putTime, err := c.Get(context.Background(), &request{ActionID: actionID})
		if err != nil {
			t.Error(err)
		}
		done <- claimHit{pathname, putTime}
	}()

	select {
	case got := <-done:
		t.Fatalf("get didn't wait on the claim, got %q", got.path)
	case <-time.After(200 * time.Millisecond):
	}
	return done
}

type claimHit struct {
	path    string
	putTime time.Time
}

func receive(t *testing.T, done <-chan claimHit) claimHit {
	t.Helper()

	select {
	case got := <-done:
		if got.path == "" {
			t.Fatal("waiting process missed")
		}
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("waiting process didn't get the put entry")
	}
	return claimHit{}
}

// TestClaims_HitReportsPutTime checks that a process that waited on another's
// claim reports when the other put the entry, which can be long before, as
// for a link from the bucket.
func TestClaims_HitReportsPutTime(t *testing.T) {
	dir, underlying := sharedDir(t)
	a := newProcess(t, dir, underlying, time.Minute)
	b := newProcess(t, dir, underlying, time.Minute)
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	if got := get(t, a, actionID); got != "" {
		t.Fatalf("first get = %q, want a miss", got)
	}
	done := awaitGet(t, b, actionID)

	content := []byte("from the bucket")
	putTime := time.Unix(1700000000, 0)
	if _, _, err := a.disk.PutOutput(context.Background(), hashID(content), bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.disk.LinkActionToOutput(context.Background(), hex.EncodeToString(actionID), hashID(content), putTime); err != nil {
		t.Fatal(err)
	}

	if got := receive(t, done); !got.putTime.Equal(putTime) {
		t.Errorf("claim hit put at %v, want %v", got.putTime, putTime)
	}
}

// TestClaims_ExpireOthersHitIsExpired checks that with -expire-others, an
// entry another process sharing the local cache put while this one waited on
// its claim is reported as put at unknownPutTime, since this process didn't
// put it, and that the other process still reports its own put time.
func TestClaims_ExpireOthersHitIsExpired(t *testing.T) {
	dir, underlying := sharedDir(t)
	a := newProcess(t, dir, underlying, time.Minute)
	b := newProcess(t, dir, underlying, time.Minute)
	a.expireOthers, b.expireOthers = true, true
	actionID := bytes.Repeat([]byte{0xaa}, 32)

	if got := get(t, a, actionID); got != "" {
		t.Fatalf("first get = %q, want a miss", got)
	}
	done := awaitGet(t, b, actionID)

	before := time.Now()
	put(t, a, actionID, []byte("test result"))
	after := time.Now()

	if got := receive(t, done); !got.putTime.Equal(unknownPutTime) {
		t.Errorf("claim hit put at %v, want %v", got.putTime, unknownPutTime)
	}

	_, putTime, err := a.Get(context.Background(), &request{ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if putTime.Before(before.Add(-time.Second)) || putTime.After(after) {
		t.Errorf("putting process's get put at %v, want its put, between %v and %v", putTime, before, after)
	}
}
