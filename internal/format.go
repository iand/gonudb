package internal

import (
	"bytes"
	"io"
)

// Format of the nudb files:

/*

Integer sizes

block_size          less than 32 bits (maybe restrict it to 16 bits)
buckets             more than 32 bits
capacity            (same as bucket index)
file offsets        63 bits
hash                up to 64 bits (48 currently)
item index          less than 32 bits (index of item in bucket)
modulus             (same as buckets)
value size          up to 32 bits (or 32-bit builds can't read it)

*/

const currentVersion = 1

const (
	DatFileHeaderSize = SizeUint64 + // Type
		SizeUint16 + // Version
		SizeUint64 + // UID
		SizeUint64 + // Appnum
		SizeUint16 + // KeySize
		64 // (Reserved)

	KeyFileHeaderSize = 8 + // Type
		SizeUint16 + // Version
		SizeUint64 + // UID
		SizeUint64 + // Appnum
		SizeUint16 + // KeySize
		SizeUint64 + // Salt
		SizeUint64 + // Pepper
		SizeUint16 + // BlockSize
		SizeUint16 + // LoadFactor
		56 // (Reserved)

	LogFileHeaderSize = 8 + // Type
		SizeUint16 + // Version
		SizeUint64 + // UID
		SizeUint64 + // Appnum
		SizeUint16 + // KeySize
		SizeUint64 + // Salt
		SizeUint64 + // Pepper
		SizeUint16 + // BlockSize
		SizeUint64 + // KeyFileSize
		SizeUint64 // DataFileSize

	SpillHeaderSize = SizeUint48 + // zero marker
		SizeUint16 // size
)

var (
	DatFileHeaderType = []byte("gonudbdt")
	KeyFileHeaderType = []byte("gonudbky")
	LogFileHeaderType = []byte("gonudblg")
)

type DatFileHeader struct {
	Type    [8]byte
	Version uint16
	UID     uint64
	AppNum  uint64
	KeySize int16
}

func (*DatFileHeader) Size() int {
	return DatFileHeaderSize
}

// DecodeFrom reads d from a reader
func (d *DatFileHeader) DecodeFrom(r io.Reader) error {
	var data [DatFileHeaderSize]byte
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return err
	}

	copy(d.Type[:], data[0:8])
	d.Version = DecodeUint16(data[8:10])
	d.UID = DecodeUint64(data[10:18])
	d.AppNum = DecodeUint64(data[18:26])
	d.KeySize = int16(DecodeUint16(data[26:28]))

	return nil
}

// EncodeTo writes d to a writer
func (d *DatFileHeader) EncodeTo(w io.Writer) error {
	var data [DatFileHeaderSize]byte

	copy(data[0:8], DatFileHeaderType)
	EncodeUint16(data[8:10], d.Version)
	EncodeUint64(data[10:18], d.UID)
	EncodeUint64(data[18:26], d.AppNum)
	EncodeUint16(data[26:28], uint16(d.KeySize))

	n, err := w.Write(data[:])
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

// Verify contents of data file header
func (d *DatFileHeader) Verify() error {
	if !bytes.Equal(DatFileHeaderType, d.Type[:]) {
		return ErrNotDataFile
	}

	if d.Version != currentVersion {
		return ErrDifferentVersion
	}

	if d.KeySize < 1 {
		return ErrInvalidKeySize
	}

	return nil
}

// VerifyMatchingKey makes sure key file and data file headers match
func (d *DatFileHeader) VerifyMatchingKey(k *KeyFileHeader) error {
	if k.UID != d.UID {
		return ErrUIDMismatch
	}
	if k.AppNum != d.AppNum {
		return ErrAppNumMismatch
	}
	if k.KeySize != d.KeySize {
		return ErrKeySizeMismatch
	}

	return nil
}

type KeyFileHeader struct {
	Type    [8]byte
	Version uint16
	UID     uint64
	AppNum  uint64
	KeySize int16

	Salt       uint64
	Pepper     uint64
	BlockSize  uint16
	LoadFactor uint16

	// Computed values
	Capacity int    // Entries per bucket
	Buckets  int    // Number of buckets
	Modulus  uint64 // pow(2,ceil(log2(buckets)))
}

func (k *KeyFileHeader) Size() int {
	return KeyFileHeaderSize
}

func (k *KeyFileHeader) DecodeFrom(r io.Reader, fileSize int64) error {
	var data [KeyFileHeaderSize]byte
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return err
	}

	copy(k.Type[:], data[0:8])
	k.Version = DecodeUint16(data[8:10])
	k.UID = DecodeUint64(data[10:18])
	k.AppNum = DecodeUint64(data[18:26])
	k.KeySize = int16(DecodeUint16(data[26:28]))
	k.Salt = DecodeUint64(data[28:36])
	k.Pepper = DecodeUint64(data[36:44])
	k.BlockSize = DecodeUint16(data[44:46])
	k.LoadFactor = DecodeUint16(data[46:48])

	k.Capacity = BucketCapacity(int(k.BlockSize))
	if fileSize > int64(k.BlockSize) {
		if k.BlockSize > 0 {
			k.Buckets = int((fileSize - int64(KeyFileHeaderSize)) / int64(k.BlockSize))
		} else {
			// Corruption or logic error
			k.Buckets = 0
		}
	} else {
		k.Buckets = 0
	}

	k.Modulus = ceil_pow2(uint64(k.Buckets))

	return nil
}

func (k *KeyFileHeader) EncodeTo(w io.Writer) error {
	var data [KeyFileHeaderSize]byte

	copy(data[0:8], KeyFileHeaderType)
	EncodeUint16(data[8:10], k.Version)
	EncodeUint64(data[10:18], k.UID)
	EncodeUint64(data[18:26], k.AppNum)
	EncodeUint16(data[26:28], uint16(k.KeySize))
	EncodeUint64(data[28:36], k.Salt)
	EncodeUint64(data[36:44], k.Pepper)
	EncodeUint16(data[44:46], k.BlockSize)
	EncodeUint16(data[46:48], k.LoadFactor)

	n, err := w.Write(data[:])
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

// Verify contents of key file header
func (k *KeyFileHeader) Verify() error {
	if !bytes.Equal(KeyFileHeaderType, k.Type[:]) {
		return ErrNotKeyFile
	}

	if k.Version != currentVersion {
		return ErrDifferentVersion
	}

	if k.KeySize < 1 {
		return ErrInvalidKeySize
	}

	if k.Pepper != pepper(k.Salt) {
		return ErrHashMismatch
	}

	if k.LoadFactor < 1 {
		return ErrInvalidLoadFactor
	}
	if k.Capacity < 1 {
		return ErrInvalidCapacity
	}
	if k.Buckets < 1 {
		return ErrInvalidBucketCount
	}

	return nil
}

type LogFileHeader struct {
	Type        [8]byte
	Version     uint16
	UID         uint64
	AppNum      uint64
	KeySize     int16
	Salt        uint64
	Pepper      uint64
	BlockSize   uint16
	KeyFileSize int64
	DatFileSize int64
}

func (l *LogFileHeader) Size() int {
	return LogFileHeaderSize
}

func (l *LogFileHeader) DecodeFrom(r io.Reader) error {
	var data [LogFileHeaderSize]byte
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return err
	}

	copy(l.Type[:], data[0:8])
	l.Version = DecodeUint16(data[8:10])
	l.UID = DecodeUint64(data[10:18])
	l.AppNum = DecodeUint64(data[18:26])
	l.KeySize = int16(DecodeUint16(data[26:28]))
	l.Salt = DecodeUint64(data[28:36])
	l.Pepper = DecodeUint64(data[36:44])
	l.BlockSize = DecodeUint16(data[44:46])
	l.KeyFileSize = int64(DecodeUint64(data[46:54]))
	l.DatFileSize = int64(DecodeUint64(data[54:62]))

	return nil
}

func (l *LogFileHeader) EncodeTo(w io.Writer) error {
	var data [LogFileHeaderSize]byte

	copy(data[0:8], LogFileHeaderType)
	EncodeUint16(data[8:10], l.Version)
	EncodeUint64(data[10:18], l.UID)
	EncodeUint64(data[18:26], l.AppNum)
	EncodeUint16(data[26:28], uint16(l.KeySize))
	EncodeUint64(data[28:36], l.Salt)
	EncodeUint64(data[36:44], l.Pepper)
	EncodeUint16(data[44:46], l.BlockSize)
	EncodeUint64(data[46:54], uint64(l.KeyFileSize))
	EncodeUint64(data[54:62], uint64(l.DatFileSize))

	n, err := w.Write(data[:])
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

type DataRecord struct {
	hash   uint64
	key    string
	data   []byte
	offset int64
	size   int64
}

type DataRecordHeader struct {
	size int64
	key  []byte
}

// IsData reports whether the data record contains data
func (d *DataRecordHeader) IsData() bool {
	return d.size != 0
}

// IsSpill reports whether the data record is a bucket spill
func (d *DataRecordHeader) IsSpill() bool {
	return d.size == 0
}

type BucketRecord struct {
	idx    int
	bucket *Bucket
}

// ceil_pow2 returns the closest power of 2 not less than v
func ceil_pow2(x uint64) uint64 {
	t := [6]uint64{
		0xFFFFFFFF00000000,
		0x00000000FFFF0000,
		0x000000000000FF00,
		0x00000000000000F0,
		0x000000000000000C,
		0x0000000000000002,
	}

	var y int
	if (x & (x - 1)) != 0 {
		y = 1
	}
	var j int = 32

	for i := 0; i < 6; i++ {
		var k int
		if (x & t[i]) != 0 {
			k = j
		}
		y += k
		x >>= k
		j >>= 1
	}

	return 1 << y
}
