package gonudb

import (
	"io"
	"time"

	"github.com/go-logr/logr"

	"github.com/iand/gonudb/internal"
)

func CreateStore(datPath, keyPath, logPath string, appnum, salt uint64, blockSize int, loadFactor float64) error {
	return internal.CreateStore(datPath, keyPath, logPath, appnum, internal.NewUID(), salt, blockSize, loadFactor)
}

func OpenStore(datPath, keyPath, logPath string, options *StoreOptions) (*Store, error) {
	if options == nil {
		options = &StoreOptions{
			Logger: logr.Discard(),
		}
	}
	if options.BackgroundSyncInterval < time.Second {
		options.BackgroundSyncInterval = time.Second
	}

	store, err := internal.OpenStore(
		datPath,
		keyPath,
		logPath,
		options.BackgroundSyncInterval,
		options.Logger,
		options.Logger.V(LogLevelDiagnostics),
		options.Logger.V(LogLevelTrace),
	)
	if err != nil {
		return nil, err
	}
	return &Store{store: store}, nil
}

type StoreOptions struct {
	Logger                 logr.Logger
	BackgroundSyncInterval time.Duration
}

type Store struct {
	store *internal.Store
}

func (s *Store) Close() error {
	return s.store.Close()
}

// Insert adds a key/value pair to the store. Zero length values are not supported.
func (s *Store) Insert(key string, value []byte) error {
	return s.store.Insert(key, value)
}

func (s *Store) Flush() error {
	s.store.Flush()
	return s.store.Err()
}

// Fetch fetches the value associated with key from the store.
func (s *Store) Fetch(key string) ([]byte, error) {
	r, err := s.store.FetchReader(key)
	if err != nil {
		return nil, err
	}

	d, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// Fetch fetches a reader that may be used to read the value associated with a key.
func (s *Store) FetchReader(key string) (io.Reader, error) {
	return s.store.FetchReader(key)
}

// Exists reports whether a data record is associated with a key.
func (s *Store) Exists(key string) (bool, error) {
	return s.store.Exists(key)
}

// DataSize returns the size of the data record associated with a key.
func (s *Store) DataSize(key string) (int64, error) {
	return s.store.DataSize(key)
}

// Err returns an error if the store is in an error state, nil otherwise
func (s *Store) Err() error {
	return s.store.Err()
}

// RecordScanner returns a scanner that may be used to iterate the datastore's values. The caller is responsible
// for calling Close on the scanner after use.
func (s *Store) RecordScanner() *RecordScanner {
	return &RecordScanner{scanner: s.store.DataFile().RecordScanner()}
}

// BucketScanner returns a scanner that may be used to iterate the datastore's index of keys. The caller is responsible
// for calling Close on the scanner after use.
func (s *Store) BucketScanner() *BucketScanner {
	return &BucketScanner{
		scanner: s.store.KeyFile().BucketScanner(s.store.DataFile()),
		bucket:  internal.NewBucket(int(s.store.KeyFile().BlockSize()), make([]byte, int(s.store.KeyFile().BlockSize()))),
	}
}

// Version returns the version number of the store's data format.
func (s *Store) Version() uint16 {
	return s.store.DataFile().Header.Version
}

// AppNum returns the store's unique id that was generated on creation.
func (s *Store) UID() uint64 {
	return s.store.DataFile().Header.UID
}

// AppNum returns the store's application-defined integer constant.
func (s *Store) AppNum() uint64 {
	return s.store.DataFile().Header.AppNum
}

// BlockSize returns the physical size of a key file bucket.
func (s *Store) BlockSize() uint16 {
	return s.store.KeyFile().Header.BlockSize
}

// RecordCount returns the number of data records in the store.
func (s *Store) RecordCount() int {
	return s.store.RecordCount()
}

// Rate returns the data write rate in bytes per second.
func (s *Store) Rate() float64 {
	return s.store.Rate()
}

// RecordScanner implements a sequential scan through a store's data file. Successive calls to the Next method will step through
// the records in the file. Note that the scanner does not include data buffered in memory. Call Flush to ensure all
// written data is visible to the scanner.
type RecordScanner struct {
	scanner *internal.RecordScanner
}

// Next reads the next bucket in sequence, including spills to the data store. It returns false
// if it encounters an error or there are no more buckets to read.
func (s *RecordScanner) Next() bool {
	return s.scanner.Next()
}

// Reader returns an io.Reader that may be used to read the data from the record. Should not be called until Next has been called.
// The Reader is only valid for use until the next call to Next().
func (s *RecordScanner) Reader() io.Reader {
	return s.scanner.Reader()
}

// IsSpill reports whether the current record is a bucket spill
func (s *RecordScanner) IsSpill() bool {
	return s.scanner.IsSpill()
}

// IsData reports whether the current record is a data record
func (s *RecordScanner) IsData() bool {
	return s.scanner.IsData()
}

// Size returns the size of the current record's data in bytes
func (s *RecordScanner) Size() int64 {
	return s.scanner.Size()
}

// RecordSize returns the number of bytes occupied by the current record including its header
func (s *RecordScanner) RecordSize() int64 {
	return s.scanner.RecordSize()
}

// Size returns the key of the current record
func (s *RecordScanner) Key() string {
	return s.scanner.Key()
}

// Err returns the first non-EOF error that was encountered by the RecordScanner.
func (s *RecordScanner) Err() error {
	return s.scanner.Err()
}

func (s *RecordScanner) Close() error {
	return s.scanner.Close()
}

// BucketScanner implements a sequential scan through a key file. Successive calls to the Next method will step through
// the buckets in the file, including spilled buckets in the data file.
type BucketScanner struct {
	scanner *internal.BucketScanner
	bucket  *internal.Bucket
}

// Next reads the next bucket in sequence, including spills to the data store. It returns false
// if it encounters an error or there are no more buckets to read.
func (s *BucketScanner) Next() bool {
	return s.scanner.Next()
}

// Index returns the index of the current bucket. Should not be called until Next has been called. Spill buckets
// share an index with their parent.
func (s *BucketScanner) Index() int {
	return s.scanner.Index()
}

// IsSpill reports whether the current bucket was read from a data store spill.
func (s *BucketScanner) IsSpill() bool {
	return s.scanner.IsSpill()
}

// Bucket returns the current bucket. Should not be called until Next has been called. The bucket is backed by data
// that may be overwritten with a call to Next so should not be retained.
func (s *BucketScanner) Bucket() *Bucket {
	s.scanner.Bucket().CopyInto(s.bucket)
	return &Bucket{bucket: s.bucket}
}

// Err returns the first non-EOF error that was encountered by the BucketScanner.
func (s *BucketScanner) Err() error {
	return s.scanner.Err()
}

// Close closes the underlying reader used by the scanner.
func (s *BucketScanner) Close() error {
	return s.scanner.Close()
}

// A Bucket contains a set of key entries that form part of the data store's index.
type Bucket struct {
	bucket *internal.Bucket
}

// Has reports whether the bucket contains an entry with the given hash.
func (b *Bucket) Has(h uint64) bool {
	return b.bucket.Has(h)
}

// Count returns the number of key entries in the bucket
func (b *Bucket) Count() int {
	return b.bucket.Count()
}

// ActualSize returns the serialized bucket size, excluding empty space
func (b *Bucket) ActualSize() int {
	return b.bucket.ActualSize()
}

// BlockSize returns the physical size of a key file bucket.
func (b *Bucket) BlockSize() int {
	return b.bucket.BlockSize()
}

// IsEmpty reports whether the bucket has any key entries.
func (b *Bucket) IsEmpty() bool {
	return b.bucket.IsEmpty()
}

// Capacity returns the maximum number of key entries that can be held in the bucket.
func (b *Bucket) Capacity() int {
	return b.bucket.Capacity()
}

// Spill returns offset in the store's data file of next spill record or 0 is there no spill.
func (b *Bucket) Spill() int64 {
	return b.bucket.Spill()
}

// HashRange returns the range of hashed keys that are contained in the bucket.
func (b *Bucket) HashRange() (uint64, uint64) {
	return b.bucket.LowestHash(), b.bucket.HighestHash()
}

// Entry returns the record for a key entry
func (b *Bucket) Entry(idx int) BucketEntry {
	// TODO: bounds check
	e := b.bucket.Entry(idx)
	return BucketEntry{
		Offset: e.Offset,
		Size:   e.Size,
		Hash:   e.Hash,
	}
}

type BucketEntry struct {
	// Offset is the position in the store's data file of the data record.
	Offset int64

	// Size is the size of the data value within the data record.
	Size int64

	// Hash is the hashed version of the key used to insert the data value.
	Hash uint64
}

func NewSalt() uint64 {
	return internal.NewSalt()
}

func Version() string {
	return internal.Version()
}

var (
	ErrAppNumMismatch     = internal.ErrAppNumMismatch
	ErrDataMissing        = internal.ErrDataMissing
	ErrDataTooLarge       = internal.ErrDataTooLarge
	ErrDifferentVersion   = internal.ErrDifferentVersion
	ErrHashMismatch       = internal.ErrHashMismatch
	ErrInvalidBlockSize   = internal.ErrInvalidBlockSize
	ErrInvalidBucketCount = internal.ErrInvalidBucketCount
	ErrInvalidCapacity    = internal.ErrInvalidCapacity
	ErrInvalidDataRecord  = internal.ErrInvalidDataRecord
	ErrInvalidKeySize     = internal.ErrInvalidKeySize
	ErrInvalidLoadFactor  = internal.ErrInvalidLoadFactor
	ErrInvalidRecordSize  = internal.ErrInvalidRecordSize
	ErrInvalidSpill       = internal.ErrInvalidSpill
	ErrKeyExists          = internal.ErrKeyExists
	ErrKeyMismatch        = internal.ErrKeyMismatch
	ErrKeyMissing         = internal.ErrKeyMissing
	ErrKeyNotFound        = internal.ErrKeyNotFound
	ErrKeySizeMismatch    = internal.ErrKeySizeMismatch
	ErrKeyTooLarge        = internal.ErrKeyTooLarge
	ErrKeyWrongSize       = internal.ErrKeyWrongSize // deprecated: use ErrKeyMissing and ErrKeyTooLarge instead
	ErrNotDataFile        = internal.ErrNotDataFile
	ErrNotKeyFile         = internal.ErrNotKeyFile
	ErrNotLogFile         = internal.ErrNotLogFile
	ErrShortKeyFile       = internal.ErrShortKeyFile
	ErrUIDMismatch        = internal.ErrUIDMismatch
)

const (
	LogLevelDiagnostics = 1 // log level increment for diagnostics logging
	LogLevelTrace       = 2 // log level increment for verbose tracing
)
