package internal

import (
	"errors"
)

var (
	ErrAppNumMismatch     = errors.New("appnum mismatch")
	ErrDataMissing        = errors.New("data missing")
	ErrDataTooLarge       = errors.New("data too large")
	ErrDifferentVersion   = errors.New("different version")
	ErrHashMismatch       = errors.New("hash mismatch")
	ErrInvalidBlockSize   = errors.New("invalid block size")
	ErrInvalidBucketCount = errors.New("invalid bucket count")
	ErrInvalidCapacity    = errors.New("invalid capacity")
	ErrInvalidDataRecord  = errors.New("not a data record: contains spill marker")
	ErrInvalidKeySize     = errors.New("invalid key size")
	ErrInvalidLoadFactor  = errors.New("invalid load factor")
	ErrInvalidRecordSize  = errors.New("invalid record size")
	ErrInvalidSpill       = errors.New("not a spill record: missing spill marker")
	ErrKeyExists          = errors.New("key exists")
	ErrKeyMismatch        = errors.New("key mismatch")
	ErrKeyMissing         = errors.New("key missing")
	ErrKeyNotFound        = errors.New("key not found")
	ErrKeySizeMismatch    = errors.New("key size mismatch")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrKeyWrongSize       = errors.New("key wrong size") // deprecated: use ErrKeyMissing and ErrKeyTooLarge instead
	ErrNotDataFile        = errors.New("not a data file")
	ErrNotKeyFile         = errors.New("not a key file")
	ErrNotLogFile         = errors.New("not a log file")
	ErrShortKeyFile       = errors.New("short key file")
	ErrUIDMismatch        = errors.New("uid mismatch")
)
