package internal

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-logr/logr"
)

type Spiller interface {
	LoadBucketSpill(int64, []byte) error
	AppendBucketSpill([]byte) (int64, error)
	Flush() error
}

type BucketCache struct {
	mu      sync.Mutex
	buckets []*Bucket
	dirty   []bool

	modulus uint64 // hash modulus

	bucketSize int
	count      int // number of entries
	threshold  int // target average number of entries per bucket

	tlogger logr.Logger // trace logger
}

func (c *BucketCache) Insert(offset int64, size int64, hash uint64, df Spiller) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.count++
	if c.count/len(c.buckets) > c.threshold {

		if uint64(len(c.buckets)) == c.modulus {
			c.modulus *= 2
		}

		// Split using linear hashing
		idxSplit := len(c.buckets) - int(c.modulus/2) // index of the bucket to be split
		c.dirty[idxSplit] = true

		idxNew := len(c.buckets) // new bucket will be added at end of list
		c.buckets = append(c.buckets, NewBucket(c.bucketSize, make([]byte, c.bucketSize)))
		c.dirty = append(c.dirty, true)

		if err := c.split(idxSplit, idxNew, df); err != nil {
			return fmt.Errorf("split bucket: %w", err)
		}
	}

	idx := c.bucketIndex(hash)
	b := c.buckets[idx]
	spilled, err := b.maybeSpill(df)

	if spilled && c.tlogger.Enabled() {
		c.tlogger.Info("bucket spilled", "index", idx, "hash", hash, "buckets", len(c.buckets), "modulus", c.modulus, "spill", b.spill)
	}
	if err != nil {
		return fmt.Errorf("maybe spill: %w", err)
	}

	// If bucket spilled then it will be empty
	b.insert(offset, size, hash)

	c.dirty[idx] = true
	return nil
}

// assumes caller holds lock
func (c *BucketCache) bucketIndex(h uint64) int {
	n := h % c.modulus
	if n >= uint64(len(c.buckets)) {
		n -= c.modulus / 2
	}
	return int(n)
}

// assumes caller holds lock
func (c *BucketCache) split(idxSplit, idxNew int, df Spiller) error {
	bSplit := c.buckets[idxSplit]
	// Trivial case: split empty bucket
	if bSplit.count == 0 && bSplit.spill == 0 {
		return nil
	}

	bNew := c.buckets[idxNew]

	for i := 0; i < bSplit.count; {
		e := bSplit.entry(i)
		idx := c.bucketIndex(e.Hash)
		if c.tlogger.Enabled() {
			c.tlogger.Info("entry rehash", "hash", e.Hash, "rehash_index", idx)
		}

		if idx != idxSplit && idx != idxNew {
			// panic due to a logic error. Something very bad must have happened.
			panic(fmt.Sprintf("bucket index of rehashed key (hash=%d, bucket=%d) does not correspond to bucket being split (bucket=%d) or new bucket (bucket=%d), modulus=%d, buckets=%d", e.Hash, idx, idxSplit, idxNew, c.modulus, len(c.buckets)))
		}

		// If the entry falls into the new bucket then add it and remove from the splitting bucket
		if idx == idxNew {
			bNew.insert(e.Offset, e.Size, e.Hash)
			bSplit.erase(i)
		} else {
			i++
		}
	}

	// Deal with any spills in the splitting bucket by rehashing the entries as well, walking the linked list
	// of spills. Potentially this can lead to the new bucket spilling.
	// Since spills are immutable we may leave orphaned entries that have been copied to the new bucket.

	spill := bSplit.spill
	bSplit.setSpill(0)

	tmp := NewBucket(c.bucketSize, make([]byte, c.bucketSize))

	if spill > 0 {
		for {
			// Make sure any spills are on disk
			// TODO: figure out semantics of buffered writer here
			if err := df.Flush(); err != nil {
				return fmt.Errorf("flush data file: %w", err)
			}

			// Read the spill record from the data file into the temporary bucket
			if err := tmp.LoadFrom(spill, df); err != nil {
				return fmt.Errorf("load from spill (%d): %w", spill, err)
			}

			if c.tlogger.Enabled() {
				c.tlogger.Info("loaded bucket from spill", "spill", spill, "bucket_entry_count", tmp.count)
			}

			for i := 0; i < tmp.count; i++ {
				e := tmp.entry(i)
				idx := c.bucketIndex(e.Hash)
				if c.tlogger.Enabled() {
					c.tlogger.Info("spill entry rehash", "hash", e.Hash, "rehash_index", idx, "buckets", len(c.buckets), "modulus", c.modulus)
				}
				if idx != idxSplit && idx != idxNew {
					panic(fmt.Sprintf("bucket index of rehashed key (%d) does not correspond to bucket being split (%d) or new bucket (%d)", idx, idxSplit, idxNew))
				}
				if idx == idxNew {
					spilled, err := bNew.maybeSpill(df)
					if spilled && c.tlogger.Enabled() {
						c.tlogger.Info("new bucket spilled during split", "index", idx, "spill", bNew.spill)
					}
					if err != nil {
						return fmt.Errorf("maybe spill: %w", err)
					}
					// we hold the lock on bNew
					bNew.insert(e.Offset, e.Size, e.Hash)
				} else {
					spilled, err := bSplit.maybeSpill(df)
					if spilled && c.tlogger.Enabled() {
						c.tlogger.Info("split bucket spilled during split", "index", idx, "spill", bSplit.spill)
					}
					if err != nil {
						return fmt.Errorf("maybe spill: %w", err)
					}
					bSplit.insert(e.Offset, e.Size, e.Hash)
				}

			}

			// Continue reading any further spills
			spill = tmp.spill

			if spill == 0 {
				break
			}
		}
	}

	return nil
}

// Exists reports whether a record with the given hash and key exists in the data file
func (c *BucketCache) Exists(hash uint64, key string, df *DataFile) (bool, error) {
	r, err := c.Fetch(hash, key, df)
	if err == ErrKeyNotFound {
		return false, nil
	}
	return r != nil, err
}

// Fetch returns a reader that can be used to read the data record associated with the key
func (c *BucketCache) Fetch(hash uint64, key string, df *DataFile) (io.Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx := c.bucketIndex(hash)
	b := c.buckets[idx]

	for {
		for i := b.lowerBound(hash); i < b.count; i++ {
			entry := b.entry(i)
			if entry.Hash != hash {
				break
			}

			dr, err := df.RecordDataReader(entry.Offset, key)
			if err != nil {
				if errors.Is(err, ErrKeyMismatch) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("load data record: %w", err)
				}
			}

			// Found a matching record
			return dr, nil
		}

		if b.spill == 0 {
			break
		}

		spill := b.spill

		blockBuf := make([]byte, c.bucketSize)
		b = NewBucket(c.bucketSize, blockBuf)
		if err := b.LoadFrom(spill, df); err != nil {
			return nil, fmt.Errorf("read spill: %w", err)
		}

	}

	return nil, ErrKeyNotFound
}

// computeStats counts the number of entries in buckets and spills
func (c *BucketCache) computeStats(df *DataFile) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	blockBuf := make([]byte, c.bucketSize)
	for idx := range c.buckets {
		b := c.buckets[idx]
		for {
			c.count += b.count
			if b.spill == 0 {
				break
			}
			spill := b.spill

			b = NewBucket(c.bucketSize, blockBuf)
			if err := b.LoadFrom(spill, df); err != nil {
				return fmt.Errorf("read spill: %w", err)
			}

		}
	}
	return nil
}

// EntryCount returns the number of entries in the cache
func (c *BucketCache) EntryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// BucketCount returns the number of buckets in the cache
func (c *BucketCache) BucketCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buckets)
}

// Get retrieves a copy of the bucket at index idx
func (c *BucketCache) Get(idx int) *Bucket {
	c.mu.Lock()
	defer c.mu.Unlock()

	buf := make([]byte, c.bucketSize)
	copy(buf, c.buckets[idx].blob)
	return NewBucket(c.bucketSize, buf)
}

func (c *BucketCache) Has(hash uint64, sp Spiller) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx := c.bucketIndex(hash)
	b := c.buckets[idx]

	// tmp is only used if we need to read from spuill
	var tmp *Bucket

	for {
		idx := b.lowerBound(hash)
		if idx < b.count {
			entry := b.entry(idx)
			if entry.Hash == hash {
				// Found a matching record
				return true, nil
			}
		}

		if b.spill == 0 {
			break
		}

		if tmp == nil {
			tmp = NewBucket(c.bucketSize, make([]byte, c.bucketSize))
		}
		if err := tmp.LoadFrom(int64(b.spill), sp); err != nil {
			return false, fmt.Errorf("read spill: %w", err)
		}
		b = tmp
	}

	return false, nil
}

func (c *BucketCache) WriteDirty(lf *LogFile, kf *KeyFile) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	work := int64(0)
	for idx := range c.dirty {
		if !c.dirty[idx] {
			continue
		}
		written, err := lf.AppendBucket(idx, c.buckets[idx])
		work += written
		if err != nil {
			return work, fmt.Errorf("append bucket to log %d: %w", idx, err)
		}
	}

	if err := lf.Sync(); err != nil {
		return work, fmt.Errorf("sync log file: %w", err)
	}

	for idx := range c.dirty {
		if !c.dirty[idx] {
			continue
		}
		if err := kf.PutBucket(idx, c.buckets[idx]); err != nil {
			return work, fmt.Errorf("put bucket %d: %w", idx, err)
		}
	}

	if err := kf.Sync(); err != nil {
		return work, fmt.Errorf("sync key file: %w", err)
	}

	for idx := range c.dirty {
		c.dirty[idx] = false
	}

	return work, nil
}
