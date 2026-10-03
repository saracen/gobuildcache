//go:build !windows

package main

// isEphemeral is always false: other platforms rename over and open files
// other processes have open.
func isEphemeral(err error) bool {
	return false
}
