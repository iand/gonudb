package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	// Bucket header
	BucketHeaderSize = SizeUint16 + // Count
		SizeUint48 // Spill

	// Bucket item
	BucketEntrySize = SizeUint48 + // Offset
		SizeUint48 + // Size
		SizeUint64 // Hash
)

func BucketIndex(h uint64, buckets int, modulus uint64) int {
	n := h % modulus
	if n >= uint64(buckets) {
		n -= modulus / 2
	}
	return int(n)
}

// BucketSize returns the actual size of a bucket.
// This can be smaller than the block size.
func BucketSize(capacity int) int {
	// Bucket Record
	return SizeUint16 + // Count
		SizeUint48 + // Spill
		capacity*(SizeUint48+ // Offset
			SizeUint48+ // Size
			SizeUint64) // Hash
}

// BucketCapacity returns the number of entries that fit in a bucket
func BucketCapacity(blockSize int) int {
	if blockSize < BucketEntrySize || blockSize < BucketHeaderSize {
		return 0
	}
	return (blockSize - BucketHeaderSize) / BucketEntrySize
}

type Entry struct {
	Offset int64 // 48 bits
	Size   int64 // 48 bits
	Hash   uint64
}

// TODO: evaluate tradeoffs of using a slice of Entry instead of blob
type Bucket struct {
	// Read only
	blockSize int // Size of a key file block

	mu    sync.Mutex // protects following including writes into blob slice
	count int        // Current key count
	spill int64      // Offset of next spill record or 0
	blob  []byte
}

// bucket takes ownership of blob
func NewBucket(blockSize int, blob []byte) *Bucket {
	if len(blob) != blockSize {
		panic("bucket blob size must equal block size")
	}

	b := &Bucket{
		blockSize: blockSize,
		blob:      blob,
	}

	b.initFromHeader()

	return b
}

func (b *Bucket) Lock() {
	b.mu.Lock()
}

func (b *Bucket) Unlock() {
	b.mu.Unlock()
}

// LowerBound returns index of entry with hash
// equal to or greater than the given hash.
func (b *Bucket) lowerBound(h uint64) int {
	// expects caller to hold lock
	const w = BucketEntrySize

	// offset to first hash
	const offset = BucketHeaderSize +
		// first bucket Entry
		SizeUint48 + // Offset
		SizeUint48 // Size

	first := 0
	count := b.count
	for count > 0 {
		step := count / 2
		i := first + step
		h1 := binary.BigEndian.Uint64(b.blob[offset+i*w : offset+i*w+SizeUint64])
		if h1 < h {
			first = i + 1
			count -= step + 1
		} else {
			count = step
		}
	}

	return first
}

func (b *Bucket) Has(h uint64) bool {
	const w = BucketEntrySize

	// offset to first hash
	const offset = BucketHeaderSize +
		// first bucket Entry
		SizeUint48 + // Offset
		SizeUint48 // Size

	b.mu.Lock()
	defer b.mu.Unlock()

	first := 0
	count := b.count
	for count > 0 {
		step := count / 2
		i := first + step
		h1 := binary.BigEndian.Uint64(b.blob[offset+i*w : offset+i*w+SizeUint64])
		if h1 == h {
			return true
		} else if h1 < h {
			first = i + 1
			count -= step + 1
		} else {
			count = step
		}
	}

	return false
}

// Count returns the number of entries in the bucket
func (b *Bucket) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// ActualSize returns the serialized bucket size, excluding empty space
func (b *Bucket) ActualSize() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BucketSize(b.count)
}

func (b *Bucket) BlockSize() int {
	return b.blockSize
}

func (b *Bucket) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count == 0
}

func (b *Bucket) IsFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count >= BucketCapacity(b.blockSize)
}

func (b *Bucket) Capacity() int {
	return BucketCapacity(b.blockSize)
}

// Spill returns offset of next spill record or 0
func (b *Bucket) Spill() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spill
}

// SetSpill sets the offset of next spill record
func (b *Bucket) SetSpill(v int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setSpill(v)
}

func (b *Bucket) LowestHash() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.count == 0 {
		return 0
	}
	pos := BucketHeaderSize
	return DecodeUint64(b.blob[pos+SizeUint48*2 : pos+SizeUint48*2+SizeUint64])
}

func (b *Bucket) HighestHash() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.count == 0 {
		return 0
	}
	pos := BucketHeaderSize + (b.count-1)*BucketEntrySize
	return DecodeUint64(b.blob[pos+SizeUint48*2 : pos+SizeUint48*2+SizeUint64])
}

func (b *Bucket) clear() {
	// expects caller to hold lock
	b.count = 0
	b.spill = 0
	for i := range b.blob {
		b.blob[i] = 0
	}
}

// Returns the record for a key entry without bounds checking.
func (b *Bucket) Entry(idx int) Entry {
	return b.entry(idx)
}

func (b *Bucket) entry(idx int) Entry {
	// expects caller to hold lock

	// Start position of item in blob
	pos := BucketHeaderSize + idx*BucketEntrySize

	return Entry{
		Offset: int64(DecodeUint48(b.blob[pos : pos+SizeUint48])),
		Size:   int64(DecodeUint48(b.blob[pos+SizeUint48 : pos+SizeUint48*2])),
		Hash:   DecodeUint64(b.blob[pos+SizeUint48*2 : pos+SizeUint48*2+SizeUint64]),
	}
}

// Erase an entry by index
func (b *Bucket) erase(idx int) {
	// expects caller to hold lock

	// Start position of item in blob
	pos := BucketHeaderSize + idx*BucketEntrySize
	// Start position of next item in blob
	next := BucketHeaderSize + (idx+1)*BucketEntrySize
	// Position immediately after last entry
	end := next + (b.count-idx-1)*BucketEntrySize

	b.count--
	if b.count < 0 {
		panic("logic error: erase resulted in negative bucket count")
	}

	if idx < b.count {
		// Shift remainder down
		copy(b.blob[pos:], b.blob[next:end])
	}

	// TODO: bounds checks
	zeroLower := BucketHeaderSize + b.count*BucketEntrySize
	zeroUpper := BucketHeaderSize + (b.count+1)*(BucketEntrySize) - 1

	if zeroLower < 0 || zeroLower > len(b.blob)-1 || zeroUpper < 0 || zeroUpper > len(b.blob)-1 {
		panic(fmt.Sprintf("logic error: zeroing [%d:%d] out of bounds of blob length %d", zeroLower, zeroUpper, len(b.blob)))
	}

	for i := zeroLower; i < zeroUpper; i++ {
		b.blob[i] = 0
	}

	b.update()
}

// Insert an entry
func (b *Bucket) insert(offset int64, size int64, hash uint64) {
	// expects caller to hold lock

	idx := b.lowerBound(hash)

	// Position we want to insert the item in blob
	pos := BucketHeaderSize + idx*BucketEntrySize
	// Start position of next item in blob
	next := BucketHeaderSize + (idx+1)*BucketEntrySize
	// Position immediately after last entry
	end := next + (b.count-idx)*BucketEntrySize

	// Make room for the item
	copy(b.blob[next:], b.blob[pos:end])
	b.count++
	b.update()

	EncodeUint48(b.blob[pos:pos+SizeUint48], uint64(offset))
	EncodeUint48(b.blob[pos+SizeUint48:pos+SizeUint48*2], uint64(size))
	EncodeUint64(b.blob[pos+SizeUint48*2:pos+SizeUint48*2+SizeUint64], hash)
}

// update updates the bucket header
func (b *Bucket) update() {
	// expects caller to hold lock
	EncodeUint16(b.blob[0:SizeUint16], uint16(b.count))
	EncodeUint48(b.blob[SizeUint16:SizeUint16+SizeUint48], uint64(b.spill))
}

func (b *Bucket) initFromHeader() {
	// expects caller to hold lock
	b.count = int(DecodeUint16(b.blob[0:SizeUint16]))
	b.spill = int64(DecodeUint48(b.blob[SizeUint16 : SizeUint16+SizeUint48]))
}

func (b *Bucket) CopyInto(b2 *Bucket) {
	copy(b2.blob, b.blob)
	b.initFromHeader()
}

// WriteTo writes data to w until all entries in the bucket are written or an error occurs.
func (b *Bucket) WriteTo(w io.Writer) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	actualSize := BucketSize(b.count)
	n, err := w.Write(b.blob[:actualSize])
	if err == nil && n != actualSize {
		err = io.ErrShortWrite
	}

	return int64(n), err
}

// LoadFrom reads data containing entries from r, padding the rest of the bucket with zero bytes.
func (b *Bucket) LoadFrom(spill int64, s Spiller) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if spill == 0 {
		panic("attempt to load from zero spill")
	}

	if err := s.LoadBucketSpill(spill, b.blob); err != nil {
		return fmt.Errorf("load bucket spill (at %d): %w", spill, err)
	}
	b.initFromHeader()

	return nil
}

// StoreFullTo writes until the entire blob is written (including zero padding) or an error occurs.
func (b *Bucket) storeFullTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.blob)
	if err == nil && n != len(b.blob) {
		err = io.ErrShortWrite
	}
	return int64(n), err
}

// LoadFullFrom reads the entire blob from r
func (b *Bucket) loadFullFrom(r io.Reader) error {
	n, err := io.ReadFull(r, b.blob)
	if err == io.EOF || n != len(b.blob) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return err
	}
	b.initFromHeader()

	return nil
}

// MaybeSpill spills the bucket if full. Bucket is cleared after it spills.
func (b *Bucket) maybeSpill(sp Spiller) (bool, error) {
	// expects caller to hold lock

	if b.count < BucketCapacity(b.blockSize) {
		return false, nil
	}

	actualSize := BucketSize(b.count)
	offset, err := sp.AppendBucketSpill(b.blob[:actualSize])
	if err != nil {
		return true, fmt.Errorf("write bucket spill: %w", err)
	}

	b.clear()

	// Set the spill location to be the start of the blob so a bucket can simply be read in from that spot
	b.setSpill(offset)

	return true, nil
}

func (b *Bucket) setSpill(spill int64) {
	b.spill = spill
	EncodeUint48(b.blob[SizeUint16:SizeUint16+SizeUint48], uint64(b.spill))
}
