//go:build unix

package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"path/filepath"
	"syscall"
	"testing"

	"gocloud.dev/blob/memblob"
)

// TestDelta_ModesFollowUmask checks that the delta's files and directories,
// and -dir's files, get the go command's modes less the umask, as CI systems
// that restore caches for another user rely on, rather than being only their
// owner's.
func TestDelta_ModesFollowUmask(t *testing.T) {
	for _, umask := range []int{0, 0o022, 0o077} {
		t.Run(fmt.Sprintf("umask %04o", umask), func(t *testing.T) {
			root := t.TempDir()
			underlying := memblob.OpenBucket(nil)
			t.Cleanup(func() { underlying.Close() })

			old := syscall.Umask(umask)
			defer syscall.Umask(old)

			// a writer fills -dir with a download, and a readonly job puts
			// the rest in its delta and records a use
			fillBucket(t, underlying, bytes.Repeat([]byte{0xaa}, 32), []byte("from the bucket"))
			dir, deltaDir := filepath.Join(root, "dir"), filepath.Join(root, "delta")
			c := newDeltaProcess(t, dir, deltaDir, underlying, 0)
			get(t, c, bytes.Repeat([]byte{0xaa}, 32))
			put(t, c, bytes.Repeat([]byte{0xbb}, 32), []byte("computed"))
			get(t, newDeltaProcess(t, dir, deltaDir, underlying, 0), bytes.Repeat([]byte{0xbb}, 32))

			want := map[bool]fs.FileMode{false: 0o666 &^ fs.FileMode(umask), true: 0o777 &^ fs.FileMode(umask)}
			for _, tree := range []string{deltaDir, filepath.Join(dir, actionDir), filepath.Join(dir, outputDir)} {
				files := 0
				err := filepath.WalkDir(tree, func(p string, d fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					fi, err := d.Info()
					if err != nil {
						return err
					}
					// -dir's directories are created by newCacherIn
					if d.IsDir() && tree != deltaDir {
						return nil
					}
					if got := fi.Mode().Perm(); got != want[d.IsDir()] {
						t.Errorf("%s: mode %v, want %v", p, got, want[d.IsDir()])
					}
					if !d.IsDir() {
						files++
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if files == 0 {
					t.Errorf("%s has no files", tree)
				}
			}
		})
	}
}
