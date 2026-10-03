//go:build !unix

package main

import "io/fs"

// allocated is what the file takes on disk: its size, where the block count
// isn't known.
func allocated(info fs.FileInfo) int64 {
	return info.Size()
}
