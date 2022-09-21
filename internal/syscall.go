//go:build !(linux && amd64)

package internal

const (
	FADV_NORMAL     = 0x0
	FADV_RANDOM     = 0x1
	FADV_SEQUENTIAL = 0x2
	FADV_WILLNEED   = 0x3
)

func Fadvise(fd int, offset int64, length int64, advice int) error {
	// noop on non unix platforms
	return nil
}
