package main

import (
	"errors"
	"syscall"
)

const (
	errorSharingViolation syscall.Errno = 32
	errorLockViolation    syscall.Errno = 33
)

// isEphemeral reports errors another process using the file causes, which
// pass once it's done with it.
func isEphemeral(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	switch errno {
	case syscall.ERROR_ACCESS_DENIED, errorSharingViolation, errorLockViolation:
		return true
	}
	return false
}
