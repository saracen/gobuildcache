//go:build unix

package main

import (
	"io/fs"
	"syscall"
)

// allocated is what the file takes on disk, in whole blocks.
func allocated(info fs.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return info.Size()
}
