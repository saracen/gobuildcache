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
