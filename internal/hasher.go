package internal

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/OneOfOne/xxhash"
)

type Hasher uint64

func (h Hasher) Hash(data []byte) uint64 {
	return xxhash.Checksum64S(data, uint64(h))
}

func (h Hasher) HashString(data string) uint64 {
	return xxhash.ChecksumString64S(data, uint64(h))
}

// pepper computes pepper from salt
func pepper(salt uint64) uint64 {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], salt)
	return Hasher(salt).Hash(data[:])
}

// NewSalt returns a random salt or panics if the system source of entropy
// cannot be read
func NewSalt() uint64 {
	var v uint64
	err := binary.Read(rand.Reader, binary.BigEndian, &v)
	if err != nil {
		panic(err.Error())
	}
	return v
}

// NewUID returns a random identifier or panics if the system source of entropy
// cannot be read
func NewUID() uint64 {
	var v uint64
	err := binary.Read(rand.Reader, binary.BigEndian, &v)
	if err != nil {
		panic(err.Error())
	}
	return v
}
