//go:build !unix

package main

// processAlive can't tell on this platform, so a claim held by a process that
// died is only given up on when waiting for it times out.
func processAlive(pid int) bool {
	return true
}
