//go:build 386 || arm || mips || mipsle

package internal

import (
	"math"
)

const (
	MaxBlockSize = MaxUint16         // maximum length of a keyfile block in bytes (must not be larger than MaxKeySize due to on-disk representation)
	MaxKeySize   = MaxUint16         // maximum length of a data record's key in bytes
	MaxDataSize  = math.MaxInt32 - 1 // maximum length of a data record's value in bytes
)
