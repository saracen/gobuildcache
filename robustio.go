package main

import (
	"errors"
	"math/rand/v2"
	"os"
	"time"
)

// Every go command runs its own gobuildcache, and they share the cache
// directory. On Windows, renaming a file into place fails while another
// process has the file it replaces open, and opening a file fails while
// another process is renaming over it, so jobs that run go commands at once
// lose entries they'd otherwise get or put. Those errors pass as soon as the
// other process is done, so these retry them for a while, as the go command
// does with its own cache (cmd/go/internal/robustio). Elsewhere, isEphemeral
// is always false, and they're os.Rename and os.Open.

// ephemeralTimeout is how long an error that passes is retried for.
const ephemeralTimeout = 2 * time.Second

// retryEphemeral runs fn until it succeeds, fails with an error isEphemeral
// doesn't report, or ephemeralTimeout passes, sleeping a random, growing
// time between attempts.
func retryEphemeral(fn func() error) error {
	return retry(fn, isEphemeral, ephemeralTimeout)
}

func retry(fn func() error, ephemeral func(error) bool, timeout time.Duration) error {
	start := time.Now()
	backoff := time.Millisecond
	for {
		err := fn()
		if err == nil || !ephemeral(err) || time.Since(start) >= timeout {
			return err
		}
		time.Sleep(backoff + rand.N(backoff))
		backoff = min(backoff*2, 100*time.Millisecond)
	}
}

// renameFile renames oldpath to newpath, replacing any newpath, retrying
// ephemeral errors.
func renameFile(oldpath, newpath string) error {
	return retryEphemeral(func() error { return os.Rename(oldpath, newpath) })
}

// renameContent renames oldpath to newpath, the pathname of content named
// by its hash. One already there is the same content, so if the rename
// fails because another process put it first, that's success.
func renameContent(oldpath, newpath string) error {
	return keepExisting(renameFile(oldpath, newpath), newpath)
}

// keepExisting returns nil if the rename failed but newpath has content.
func keepExisting(err error, newpath string) error {
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		if fi, statErr := os.Stat(newpath); statErr == nil && fi.Mode().IsRegular() {
			return nil
		}
	}
	return err
}

// openFile opens a file for reading, retrying ephemeral errors.
func openFile(name string) (*os.File, error) {
	var f *os.File
	err := retryEphemeral(func() error {
		var err error
		f, err = os.Open(name)
		return err
	})
	return f, err
}
