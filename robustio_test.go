package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var errBusy = errors.New("busy")

func TestRetry(t *testing.T) {
	ephemeral := func(err error) bool { return errors.Is(err, errBusy) }

	t.Run("an ephemeral error is retried until it passes", func(t *testing.T) {
		calls := 0
		err := retry(func() error {
			calls++
			if calls < 3 {
				return errBusy
			}
			return nil
		}, ephemeral, time.Second)
		if err != nil || calls != 3 {
			t.Fatalf("got %v after %d calls, want success after 3", err, calls)
		}
	})

	t.Run("another error isn't retried", func(t *testing.T) {
		calls := 0
		other := errors.New("other")
		err := retry(func() error { calls++; return other }, ephemeral, time.Second)
		if !errors.Is(err, other) || calls != 1 {
			t.Fatalf("got %v after %d calls, want the error after 1", err, calls)
		}
	})

	t.Run("an ephemeral error is given up on", func(t *testing.T) {
		start := time.Now()
		err := retry(func() error { return errBusy }, ephemeral, 20*time.Millisecond)
		if !errors.Is(err, errBusy) {
			t.Fatalf("got %v, want the ephemeral error", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("took %v to give up", elapsed)
		}
	})
}

func TestKeepExisting(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing")
	if err := os.WriteFile(existing, []byte("content"), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "directory"), 0o777); err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		err     error
		newpath string
		wantErr bool
	}{
		"renamed":                       {newpath: existing},
		"failed, content there":         {err: errBusy, newpath: existing},
		"failed, nothing there":         {err: errBusy, newpath: filepath.Join(dir, "missing"), wantErr: true},
		"failed, directory there":       {err: errBusy, newpath: filepath.Join(dir, "directory"), wantErr: true},
		"source missing, content there": {err: os.ErrNotExist, newpath: existing, wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if err := keepExisting(tc.err, tc.newpath); (err != nil) != tc.wantErr {
				t.Fatalf("got %v, want error %v", err, tc.wantErr)
			}
		})
	}
}
