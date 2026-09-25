package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gocloud.dev/blob/memblob"
)

func TestFlagArray(t *testing.T) {
	var fa flagArray
	if err := fa.Set("a"); err != nil {
		t.Fatal(err)
	}
	if err := fa.Set("b=c"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b=c"}; !reflect.DeepEqual([]string(fa), want) {
		t.Errorf("got %v, want %v", []string(fa), want)
	}
	if got, want := fa.String(), "[a b=c]"; got != want {
		t.Errorf("String=%q, want %q", got, want)
	}
}

func newCacher(t *testing.T) *Cacher {
	t.Helper()
	dir := t.TempDir()
	for _, sub := range []string{actionDir, outputDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	underlying := memblob.OpenBucket(nil)
	t.Cleanup(func() { underlying.Close() })

	c := &Cacher{disk: &Disk{cacheDir: dir}}
	c.bucket = &Bucket{disk: c.disk, bucket: underlying}
	c.bucket.Start(context.Background())
	t.Cleanup(c.bucket.Close)
	return c
}

func TestCacher_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := newCacher(t)

	content := []byte("payload")
	outputID := sha256Bytes(content)
	actionID := bytes.Repeat([]byte{0xab}, 32)

	pathname, err := c.Put(ctx, &request{
		ActionID: actionID,
		OutputID: outputID,
		Body:     bytes.NewReader(content),
		BodySize: int64(len(content)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(pathname); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, content) {
		t.Errorf("on-disk content mismatch")
	}

	got, err := c.Get(ctx, &request{ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if got != pathname {
		t.Errorf("Get=%q, want %q", got, pathname)
	}
}

func TestCacher_GetMiss(t *testing.T) {
	ctx := context.Background()
	c := newCacher(t)

	got, err := c.Get(ctx, &request{ActionID: bytes.Repeat([]byte{0xab}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("got %q, want empty on miss", got)
	}
}

func TestPopulateFileInfo(t *testing.T) {
	dir := t.TempDir()
	outputID := strings.Repeat("a", 64)
	pathname := filepath.Join(dir, outputID)
	content := []byte("xyz")
	if err := os.WriteFile(pathname, content, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("populates fields on hit", func(t *testing.T) {
		req := &request{Command: cmdGet}
		resp := &response{DiskPath: pathname}
		populateFileInfo(req, resp)
		if resp.Err != "" {
			t.Errorf("unexpected Err=%q", resp.Err)
		}
		if resp.Size != int64(len(content)) {
			t.Errorf("Size=%d, want %d", resp.Size, len(content))
		}
		if resp.Time == nil {
			t.Error("Time was nil")
		}
		if len(resp.OutputID) != 32 {
			t.Errorf("OutputID len=%d, want 32", len(resp.OutputID))
		}
	})

	t.Run("missing file sets Err", func(t *testing.T) {
		resp := &response{DiskPath: filepath.Join(dir, "nonexistent")}
		populateFileInfo(&request{Command: cmdGet}, resp)
		if resp.Err == "" {
			t.Error("expected Err for missing file")
		}
	})

	t.Run("close skips stat", func(t *testing.T) {
		resp := &response{DiskPath: filepath.Join(dir, "nonexistent")}
		populateFileInfo(&request{Command: cmdClose}, resp)
		if resp.Err != "" {
			t.Errorf("close should be skipped, got Err=%q", resp.Err)
		}
	})

	t.Run("empty DiskPath skips stat", func(t *testing.T) {
		resp := &response{}
		populateFileInfo(&request{Command: cmdGet}, resp)
		if resp.Err != "" {
			t.Errorf("empty path should be skipped, got Err=%q", resp.Err)
		}
	})

	t.Run("non-hex basename sets invalid output id", func(t *testing.T) {
		bad := filepath.Join(dir, "not-hex")
		if err := os.WriteFile(bad, content, 0o600); err != nil {
			t.Fatal(err)
		}
		resp := &response{DiskPath: bad}
		populateFileInfo(&request{Command: cmdGet}, resp)
		if resp.Err != "invalid output id" {
			t.Errorf("Err=%q, want %q", resp.Err, "invalid output id")
		}
	})
}

func TestHandleRequest_Get(t *testing.T) {
	ctx := context.Background()
	c := newCacher(t)

	content := []byte("hi")
	outputID := sha256Bytes(content)
	actionID := bytes.Repeat([]byte{0xab}, 32)

	if _, err := c.Put(ctx, &request{
		ActionID: actionID, OutputID: outputID,
		Body: bytes.NewReader(content), BodySize: int64(len(content)),
	}); err != nil {
		t.Fatal(err)
	}

	resp := handleRequest(ctx, c, &request{ID: 7, Command: cmdGet, ActionID: actionID})
	if resp.ID != 7 {
		t.Errorf("ID=%d, want 7", resp.ID)
	}
	if resp.Err != "" {
		t.Errorf("Err=%s", resp.Err)
	}
	if resp.Miss {
		t.Error("Miss=true on hit")
	}
	if !bytes.Equal(resp.OutputID, outputID) {
		t.Errorf("OutputID mismatch")
	}
	if resp.Size != int64(len(content)) {
		t.Errorf("Size=%d, want %d", resp.Size, len(content))
	}
}

func TestHandleRequest_GetMiss(t *testing.T) {
	ctx := context.Background()
	c := newCacher(t)

	resp := handleRequest(ctx, c, &request{
		ID: 5, Command: cmdGet,
		ActionID: bytes.Repeat([]byte{0xab}, 32),
	})
	if !resp.Miss {
		t.Error("expected Miss=true")
	}
	if resp.DiskPath != "" {
		t.Errorf("DiskPath=%q, want empty", resp.DiskPath)
	}
}

func TestHandleRequest_Put(t *testing.T) {
	ctx := context.Background()
	c := newCacher(t)

	content := []byte("payload")
	resp := handleRequest(ctx, c, &request{
		ID: 9, Command: cmdPut,
		ActionID: bytes.Repeat([]byte{0xab}, 32),
		OutputID: sha256Bytes(content),
		Body:     bytes.NewReader(content),
		BodySize: int64(len(content)),
	})
	if resp.Err != "" {
		t.Errorf("Err=%s", resp.Err)
	}
	if resp.DiskPath == "" {
		t.Error("expected non-empty DiskPath")
	}
	if resp.Size != int64(len(content)) {
		t.Errorf("Size=%d, want %d", resp.Size, len(content))
	}
}

func TestHandleRequest_Close(t *testing.T) {
	ctx := context.Background()
	c := newCacher(t)
	resp := handleRequest(ctx, c, &request{ID: 11, Command: cmdClose})
	if resp.ID != 11 {
		t.Errorf("ID=%d, want 11", resp.ID)
	}
	if resp.Err != "" {
		t.Errorf("Err=%s", resp.Err)
	}
}

func TestServe_RoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := serve(ctx, underlying, dir, false, inR, outW)
		outW.Close()
		done <- err
	}()

	enc := json.NewEncoder(inW)
	dec := json.NewDecoder(outR)

	var handshake response
	if err := dec.Decode(&handshake); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if len(handshake.KnownCommands) != 3 {
		t.Errorf("KnownCommands=%v, want 3", handshake.KnownCommands)
	}

	content := []byte("payload!")
	outputID := sha256Bytes(content)
	actionID := bytes.Repeat([]byte{0xab}, 32)

	if err := enc.Encode(request{
		ID: 1, Command: cmdPut,
		ActionID: actionID, OutputID: outputID,
		BodySize: int64(len(content)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(content); err != nil {
		t.Fatal(err)
	}
	var putResp response
	if err := dec.Decode(&putResp); err != nil {
		t.Fatalf("decode put resp: %v", err)
	}
	if putResp.ID != 1 || putResp.Err != "" {
		t.Errorf("putResp=%+v", putResp)
	}

	if err := enc.Encode(request{ID: 2, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	var getResp response
	if err := dec.Decode(&getResp); err != nil {
		t.Fatalf("decode get resp: %v", err)
	}
	if getResp.ID != 2 || getResp.Err != "" || getResp.Miss {
		t.Errorf("getResp=%+v", getResp)
	}
	if !bytes.Equal(getResp.OutputID, outputID) {
		t.Errorf("OutputID mismatch")
	}

	if err := enc.Encode(request{ID: 3, Command: cmdClose}); err != nil {
		t.Fatal(err)
	}
	var closeResp response
	if err := dec.Decode(&closeResp); err != nil {
		t.Fatalf("decode close resp: %v", err)
	}
	if closeResp.ID != 3 {
		t.Errorf("close ID=%d", closeResp.ID)
	}

	inW.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return")
	}
}

func TestServe_Readonly_OmitsPut(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := serve(ctx, underlying, dir, true, inR, outW)
		outW.Close()
		done <- err
	}()

	dec := json.NewDecoder(outR)
	var handshake response
	if err := dec.Decode(&handshake); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if len(handshake.KnownCommands) != 2 {
		t.Errorf("KnownCommands=%v, want 2 (close, get)", handshake.KnownCommands)
	}
	for _, c := range handshake.KnownCommands {
		if c == cmdPut {
			t.Error("readonly mode should not advertise cmdPut")
		}
	}

	inW.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return")
	}
}

func TestServe_BodySizeMismatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := serve(ctx, underlying, dir, false, inR, outW)
		outW.Close()
		done <- err
	}()

	dec := json.NewDecoder(outR)
	var handshake response
	if err := dec.Decode(&handshake); err != nil {
		t.Fatal(err)
	}

	enc := json.NewEncoder(inW)
	if err := enc.Encode(request{
		ID: 1, Command: cmdPut,
		ActionID: bytes.Repeat([]byte{0xab}, 32),
		OutputID: bytes.Repeat([]byte{0xcd}, 32),
		BodySize: 10, // claim 10 bytes, send 4
	}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode([]byte("oops")); err != nil {
		t.Fatal(err)
	}
	inW.Close()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "incorrect length") {
			t.Errorf("expected 'incorrect length' error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return")
	}

	// Drain any pending response so the test goroutine doesn't leak on outR.
	io.Copy(io.Discard, outR)
}

func TestServe_ObjectIDFallback(t *testing.T) {
	// The pre-Go-1.24 driver sent ObjectID instead of OutputID. Verify that
	// the server transparently maps it to OutputID for puts.
	ctx := context.Background()
	dir := t.TempDir()
	underlying := memblob.OpenBucket(nil)
	defer underlying.Close()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := serve(ctx, underlying, dir, false, inR, outW)
		outW.Close()
		done <- err
	}()

	enc := json.NewEncoder(inW)
	dec := json.NewDecoder(outR)

	var handshake response
	if err := dec.Decode(&handshake); err != nil {
		t.Fatal(err)
	}

	content := []byte("legacy")
	outputID := sha256Bytes(content)
	actionID := bytes.Repeat([]byte{0xab}, 32)

	if err := enc.Encode(request{
		ID: 1, Command: cmdPut,
		ActionID: actionID,
		ObjectID: outputID, // legacy field
		BodySize: int64(len(content)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(content); err != nil {
		t.Fatal(err)
	}
	var putResp response
	if err := dec.Decode(&putResp); err != nil {
		t.Fatal(err)
	}
	if putResp.Err != "" {
		t.Errorf("put err: %s", putResp.Err)
	}

	// The expected file must be on disk under the OutputID.
	pathname := filepath.Join(dir, outputDir, hashID(content))
	if _, err := os.Stat(pathname); errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected file at %s", pathname)
	}

	inW.Close()
	<-done
}

func sha256Bytes(content []byte) []byte {
	id, err := hex.DecodeString(hashID(content))
	if err != nil {
		panic(err)
	}
	return id
}
