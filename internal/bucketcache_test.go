package internal

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-logr/logr"
)

func TestBucketCacheInsert(t *testing.T) {
	bucketSize := BucketHeaderSize + BucketEntrySize*6

	n := 5000
	c := &BucketCache{
		bucketSize: bucketSize, // capacity of 6 entries per bucket
		modulus:    1,
		buckets:    []*Bucket{NewBucket(bucketSize, make([]byte, bucketSize))},
		dirty:      []bool{false},
		threshold:  3, // aim for an average of 3 entries per bucket
		tlogger:    logr.Discard(),
	}

	rng := rand.New(rand.NewSource(299792458))

	sm := make(spillMap)
	hashes := make([]uint64, n)
	for i := range hashes {
		hashes[i] = rng.Uint64()
		err := c.Insert(5, 5, hashes[i], sm)
		if err != nil {
			t.Fatalf("unexpected error on insert (%d): %v", hashes[i], err)
		}
	}
	if c.EntryCount() != len(hashes) {
		t.Errorf("c.EntryCount()=%d, wanted %d", c.EntryCount(), len(hashes))
	}

	for i := range hashes {
		has, err := c.Has(hashes[i], sm)
		if err != nil {
			t.Fatalf("unexpected error on has (%d): %v", hashes[i], err)
		}

		if !has {
			t.Errorf("did not find hash %d", hashes[i])
		}
	}
}

type spillMap map[int64][]byte

func (s spillMap) LoadBucketSpill(spill int64, buf []byte) error {
	data, ok := s[spill]
	if !ok {
		return fmt.Errorf("unknown spill: %d", spill)
	}
	copy(buf, data)
	return nil
}

func (s spillMap) AppendBucketSpill(buf []byte) (int64, error) {
	spill := int64(len(s)) + 1
	data := make([]byte, len(buf))
	copy(data, buf)
	s[spill] = data
	return spill, nil
}

func (s spillMap) Flush() error {
	return nil
}
