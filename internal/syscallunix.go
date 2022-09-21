//go:build linux && amd64

package internal

import (
	"golang.org/x/sys/unix"
)

const (
	FADV_NORMAL     = 0x0
	FADV_RANDOM     = 0x1
	FADV_SEQUENTIAL = 0x2
	FADV_WILLNEED   = 0x3
)

func Fadvise(fd int, offset int64, length int64, advice int) error {
	return unix.Fadvise(fd, offset, length, advice)
}
