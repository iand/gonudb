package internal

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/go-logr/logr"
)

func openFile(name string, flag int, perm os.FileMode, advice int) (*os.File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	err = Fadvise(int(f.Fd()), 0, 0, advice)
	if err != nil {
		return nil, fmt.Errorf("fadvise: %w", err)
	}

	return f, nil
}

// openFileForScan creates a file for sequential reads
func openFileForScan(name string) (*os.File, error) {
	return openFile(name, os.O_RDONLY, 0o644, FADV_SEQUENTIAL)
}

func block_size(path string) int {
	// A reasonable default for many SSD devices
	return 4096
}

type CountWriter interface {
	WriterFlusher

	// Offset returns the position in the file at which the next write will be made
	Offset() int64

	// Count returns the number of bytes written
	Count() int64
}

type WriterFlusher interface {
	io.Writer
	Flush() error
}

// DataFile assumes it has exclusive write access to the file
type DataFile struct {
	Path   string
	Header DatFileHeader

	offset  int64
	file    *os.File
	writer  *bufio.Writer
	elogger logr.Logger
}

func CreateDataFile(path string, appnum, uid uint64) error {
	f, err := openFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644, FADV_RANDOM)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	dh := DatFileHeader{
		Version: currentVersion,
		UID:     uid,
		AppNum:  appnum,
	}

	if err := dh.EncodeTo(f); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("write header: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

// OpenDataFile opens a data file for appending and random reads
func OpenDataFile(path string) (*DataFile, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	err = Fadvise(int(f.Fd()), 0, 0, FADV_RANDOM)
	if err != nil {
		return nil, fmt.Errorf("fadvise: %w", err)
	}

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat data file: %w", err)
	}

	var dh DatFileHeader
	if err := dh.DecodeFrom(f); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if err := dh.Verify(); err != nil {
		return nil, fmt.Errorf("verify header: %w", err)
	}

	return &DataFile{
		Path:   path,
		Header: dh,
		file:   f,
		offset: st.Size(),
		// Buffered writes to avoid write amplification
		writer:  bufio.NewWriterSize(f, 32*block_size(path)),
		elogger: logr.Discard(),
	}, nil
}

func (d *DataFile) Offset() int64 {
	return d.offset
}

func (d *DataFile) Sync() error {
	if err := d.writer.Flush(); err != nil {
		return err
	}
	return d.file.Sync()
}

func (d *DataFile) Flush() error {
	return d.writer.Flush()
}

func (d *DataFile) Close() error {
	if err := d.writer.Flush(); err != nil {
		return err
	}
	return d.file.Close()
}

func (d *DataFile) Size() (int64, error) {
	st, err := d.file.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// AppendRecord writes a record to the data file. It returns the position at which
// the record was written.
func (d *DataFile) AppendRecord(dr *DataRecord) (int64, error) {
	hdr := make([]byte, SizeUint48+SizeUint16)
	EncodeUint48(hdr[0:SizeUint48], uint64(len(dr.data)))
	EncodeUint16(hdr[SizeUint48:SizeUint48+SizeUint16], uint16(len(dr.key)))

	offset := d.offset

	n, err := d.file.Write(hdr[:])
	d.offset += int64(n)
	if err != nil {
		return offset, err
	}
	if n != len(hdr) {
		return offset, io.ErrShortWrite
	}

	nk, err := d.file.Write([]byte(dr.key))
	d.offset += int64(nk)
	if err != nil {
		return offset, err
	}
	if nk != len(dr.key) {
		return offset, io.ErrShortWrite
	}

	nd, err := d.file.Write(dr.data)
	d.offset += int64(nd)
	if err != nil {
		return offset, err
	}
	if nd != len(dr.data) {
		return offset, io.ErrShortWrite
	}

	return offset, nil
}

func (d *DataFile) LoadRecordHeader(offset int64) (*DataRecordHeader, error) {
	hdr := make([]byte, SizeUint48+SizeUint16)

	_, err := d.file.ReadAt(hdr, offset)
	if err != nil {
		return nil, fmt.Errorf("read data record header: %w", err)
	}

	dataSize := DecodeUint48(hdr[:SizeUint48])
	if dataSize == 0 {
		// Data size 0 indicates a bucket spill follows
		return nil, ErrInvalidDataRecord
	}
	keySize := DecodeUint16(hdr[SizeUint48 : SizeUint48+SizeUint16])
	if keySize == 0 {
		return nil, ErrInvalidDataRecord
	}

	key := make([]byte, keySize)
	_, err = d.file.ReadAt(key, offset+SizeUint48+SizeUint16)
	if err != nil {
		return nil, fmt.Errorf("read data record key: %w", err)
	}

	return &DataRecordHeader{
		Key:      key,
		DataSize: int64(dataSize),
		KeySize:  keySize,
	}, nil
}

func (d *DataFile) RecordDataReader(offset int64, key string) (io.Reader, error) {
	rh, err := d.LoadRecordHeader(offset)
	if err != nil {
		return nil, fmt.Errorf("read data record header: %w", err)
	}
	if !bytes.Equal([]byte(key), rh.Key) {
		return nil, ErrKeyMismatch
	}

	return io.NewSectionReader(d.file, offset+rh.Size(), int64(rh.DataSize)), nil
}

func (d *DataFile) AppendBucketSpill(blob []byte) (int64, error) {
	offset := d.offset

	var hdr [SpillHeaderSize]byte
	// Initial Uint48 is zero to indicate this is a spill record
	EncodeUint16(hdr[SizeUint48:], uint16(len(blob)))

	hn, err := d.writer.Write(hdr[:])
	d.offset += int64(hn)
	if err == nil && hn != len(hdr) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if d.elogger.Enabled() && errors.Is(err, io.ErrShortWrite) {
			d.elogger.Info("data file: short write on bucket header", "expected", len(hdr), "wrote", hn)
		}
		return offset, fmt.Errorf("write header: %w", err)
	}

	bn, err := d.writer.Write(blob)
	d.offset += int64(bn)
	if err == nil && bn != len(blob) {
		err = io.ErrShortWrite
	}

	if err != nil {
		if d.elogger.Enabled() && errors.Is(err, io.ErrShortWrite) {
			d.elogger.Info("data file: short write on bucket data", "expected", len(blob), "wrote", bn)
		}
		return offset, fmt.Errorf("write header: %w", err)
	}

	return offset, nil
}

func (d *DataFile) LoadBucketSpill(offset int64, blob []byte) error {
	var hdr [SpillHeaderSize]byte
	_, err := d.file.ReadAt(hdr[:], offset)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	marker := DecodeUint48(hdr[:SizeUint48])
	if marker != 0 {
		return ErrInvalidSpill
	}

	size := DecodeUint16(hdr[SizeUint48 : SizeUint48+SizeUint16])

	sr := io.NewSectionReader(d.file, offset+int64(len(hdr)), int64(size))
	off := 0
	for {
		n, err := sr.Read(blob[off:])
		off += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read bucket data: %w", err)
		}
		if off >= len(blob) {
			return io.ErrShortBuffer
		}
	}

	for i := off; i < len(blob); i++ {
		blob[i] = 0
	}

	return nil
}

// RecordScanner returns a RecordScanner that may be used to iterate over the records in the data file.
func (d *DataFile) RecordScanner() *RecordScanner {
	f, err := openFileForScan(d.Path)
	if err != nil {
		return &RecordScanner{err: err}
	}

	r := bufio.NewReaderSize(f, 32*block_size(d.Path))
	n, err := r.Discard(DatFileHeaderSize)

	return &RecordScanner{
		err:     err,
		r:       r,
		closer:  f,
		offset:  int64(n),
		lr:      io.LimitedReader{R: r, N: 0},
		size:    -1,
		isSpill: false,
	}
}

// RecordScanner implements a sequential scan through a data file. Successive calls to the Next method will step through
// the records in the file.
type RecordScanner struct {
	r       *bufio.Reader
	closer  io.Closer
	err     error
	offset  int64
	size    int64
	key     []byte
	isSpill bool
	lr      io.LimitedReader
}

// Next reads the next bucket in sequence, including spills to the data store. It returns false
// if it encounters an error or there are no more buckets to read.
func (s *RecordScanner) Next() bool {
	if s.err != nil {
		return false
	}

	var n int

	// Skip any unread bytes
	n, s.err = s.r.Discard(int(s.lr.N))
	if s.err != nil {
		return false
	}
	s.offset += int64(n)

	hdr := make([]byte, int64(SizeUint48+SizeUint16))
	n, s.err = io.ReadFull(s.r, hdr)
	if s.err != nil {
		return false
	}
	s.offset += int64(n)

	s.size = int64(DecodeUint48(hdr[:SizeUint48]))
	if s.size == 0 {
		s.isSpill = true
		s.key = nil
		// Spill size is in the next 2 bytes
		s.size = int64(DecodeUint16(hdr[SizeUint48 : SizeUint48+SizeUint16]))
		if s.size == 0 {
			s.err = ErrInvalidRecordSize
		}
	} else {
		s.isSpill = false
		keySize := int(DecodeUint16(hdr[SizeUint48 : SizeUint48+SizeUint16]))
		key := make([]byte, keySize)
		n, s.err = io.ReadFull(s.r, key)
		s.offset += int64(n)
		if s.err != nil {
			return false
		}
		s.key = key
	}

	// Set the limited reader hard limit
	s.lr.N = s.size

	return s.err == nil
}

// Reader returns an io.Reader that may be used to read the data from the record. Should not be called until Next has been called.
// The Reader is only valid for use until the next call to Next().
func (s *RecordScanner) Reader() io.Reader {
	if s.err != nil {
		return nil
	}
	return &s.lr
}

// IsSpill reports whether the current record is a bucket spill
func (s *RecordScanner) IsSpill() bool {
	return s.isSpill
}

// IsData reports whether the current record is a data record
func (s *RecordScanner) IsData() bool {
	return !s.isSpill
}

// Size returns the size of the current record's data in bytes
func (s *RecordScanner) Size() int64 {
	return s.size
}

// RecordSize returns the number of bytes occupied by the current record including its header
func (s *RecordScanner) RecordSize() int64 {
	if s.isSpill {
		return SizeUint48 + // holds marker
			SizeUint16 + // holds spill size
			s.size // spill data
	}
	return SizeUint48 + // holds data size
		SizeUint16 + // holds key size
		s.size + // data
		int64(len(s.key)) // key
}

// Key returns the key of the current record
func (s *RecordScanner) Key() string {
	if s.key == nil {
		return ""
	}
	return string(s.key)
}

// Err returns the first non-EOF error that was encountered by the RecordScanner.
func (s *RecordScanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

func (s *RecordScanner) Close() error {
	return s.closer.Close()
}

// KeyFile assumes it has exclusive write access to the file
type KeyFile struct {
	Path   string
	Header KeyFileHeader

	file    *os.File
	hasher  Hasher
	elogger logr.Logger

	// bucketLocks is a list of locks corresponding to each bucket in the file
	// the locks guard access to read/writes of that portion of the keyfile
	// blmu guards mutations to bucketLocks
	blmu        sync.Mutex
	bucketLocks []*sync.Mutex
}

func CreateKeyFile(path string, uid uint64, appnum uint64, salt uint64, blockSize int, loadFactor float64) error {
	kf, err := openFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644, FADV_RANDOM)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	abandon := func() {
		kf.Close()
		os.Remove(path)
	}

	kh := KeyFileHeader{
		Version:    currentVersion,
		UID:        uid,
		AppNum:     appnum,
		Salt:       salt,
		Pepper:     pepper(salt),
		BlockSize:  uint16(blockSize),
		LoadFactor: uint16(math.Min((MaxUint16+1)*loadFactor, MaxUint16)),
	}

	if err := kh.EncodeTo(kf); err != nil {
		abandon()
		return fmt.Errorf("write header: %w", err)
	}

	buf := make([]byte, blockSize)
	b := NewBucket(blockSize, buf)

	sw := NewSectionWriter(kf, KeyFileHeaderSize, int64(kh.BlockSize))
	n, err := b.storeFullTo(sw)
	if err != nil {
		abandon()
		return fmt.Errorf("write initial bucket: %w", err)
	}
	if n != int64(len(buf)) {
		abandon()
		return fmt.Errorf("write initial bucket (%d!=%d): %w", n, int64(len(buf)), io.ErrShortWrite)
	}

	if err := kf.Sync(); err != nil {
		abandon()
		return fmt.Errorf("sync: %w", err)
	}
	if err := kf.Close(); err != nil {
		abandon()
		return fmt.Errorf("close : %w", err)
	}
	return nil
}

// OpenKeyFile opens a key file for random reads and writes
func OpenKeyFile(path string) (*KeyFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	err = Fadvise(int(f.Fd()), 0, 0, FADV_RANDOM)
	if err != nil {
		return nil, fmt.Errorf("fadvise: %w", err)
	}

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat key file: %w", err)
	}

	var kh KeyFileHeader
	if err := kh.DecodeFrom(f, st.Size()); err != nil {
		return nil, fmt.Errorf("read key file header: %w", err)
	}
	if err := kh.Verify(); err != nil {
		return nil, fmt.Errorf("verify key file header: %w", err)
	}

	bucketLocks := make([]*sync.Mutex, int(kh.Buckets))
	for i := 0; i < int(kh.Buckets); i++ {
		bucketLocks[i] = &sync.Mutex{}
	}

	return &KeyFile{
		Path:        path,
		Header:      kh,
		file:        f,
		hasher:      Hasher(kh.Salt),
		elogger:     logr.Discard(),
		bucketLocks: bucketLocks,
	}, nil
}

func (k *KeyFile) Sync() error {
	return k.file.Sync()
}

func (k *KeyFile) Close() error {
	return k.file.Close()
}

func (k *KeyFile) Size() (int64, error) {
	st, err := k.file.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (k *KeyFile) BlockSize() uint16 {
	return k.Header.BlockSize
}

func (k *KeyFile) Hash(key []byte) uint64 {
	return k.hasher.Hash(key)
}

func (k *KeyFile) HashString(key string) uint64 {
	return k.hasher.HashString(key)
}

func (k *KeyFile) LoadBucket(idx int) (*Bucket, error) {
	var bmu *sync.Mutex
	k.blmu.Lock()
	if len(k.bucketLocks) > idx {
		k.bucketLocks[idx].Lock()
		bmu = k.bucketLocks[idx]
	}
	k.blmu.Unlock()

	if bmu == nil {
		if k.elogger.Enabled() {
			k.elogger.Error(fmt.Errorf("unknown bucket index"), "attempt to load invalid bucket index", "index", idx, "bucket_count", k.Header.Buckets)
		}
		panic("attempt to load invalid bucket index")
	}
	defer bmu.Unlock()

	offset := KeyFileHeaderSize + int64(idx)*int64(k.Header.BlockSize)
	b := NewBucket(int(k.Header.BlockSize), make([]byte, int(k.Header.BlockSize)))

	sr := io.NewSectionReader(k.file, offset, int64(k.Header.BlockSize))
	if err := b.loadFullFrom(sr); err != nil {
		return nil, fmt.Errorf("read bucket: %w", err)
	}
	return b, nil
}

// expects to have exclusive access to b
func (k *KeyFile) PutBucket(idx int, b *Bucket) error {
	var bmu *sync.Mutex
	k.blmu.Lock()
	for idx > len(k.bucketLocks)-1 {
		k.bucketLocks = append(k.bucketLocks, &sync.Mutex{})
	}
	k.bucketLocks[idx].Lock()
	bmu = k.bucketLocks[idx]
	k.blmu.Unlock()
	if bmu == nil {
		panic("attempt to put invalid bucket index")
	}
	defer bmu.Unlock()

	offset := KeyFileHeaderSize + int64(idx)*int64(k.Header.BlockSize)
	sw := NewSectionWriter(k.file, offset, int64(k.Header.BlockSize))
	_, err := b.storeFullTo(sw)
	if err != nil {
		return fmt.Errorf("write bucket: %w", err)
	}
	return nil
}

// BucketScanner returns a BucketScanner that may be used to iterate over the buckets in the key file.
func (k *KeyFile) BucketScanner(df *DataFile) *BucketScanner {
	f, err := openFileForScan(k.Path)
	if err != nil {
		return &BucketScanner{err: err}
	}

	r := bufio.NewReaderSize(f, 32*block_size(k.Path))
	_, err = r.Discard(KeyFileHeaderSize)

	return &BucketScanner{
		err:       err,
		r:         r,
		closer:    f,
		bucket:    NewBucket(int(k.Header.BlockSize), make([]byte, int(k.Header.BlockSize))),
		blockSize: int64(k.Header.BlockSize),
		index:     -1,
		df:        df,
		elogger:   k.elogger,
	}
}

// BucketScanner implements a sequential scan through a key file. Successive calls to the Next method will step through
// the buckets in the file, including spilled buckets in the data file.
type BucketScanner struct {
	r         *bufio.Reader
	closer    io.Closer
	df        *DataFile
	bucket    *Bucket
	blockSize int64
	index     int
	err       error
	spill     int64 // non-zero if next read is a spill to the data store
	isSpill   bool  // true if the current bucket was read from a spill
	elogger   logr.Logger
}

// Next reads the next bucket in sequence, including spills to the data store. It returns false
// if it encounters an error or there are no more buckets to read.
func (b *BucketScanner) Next() bool {
	if b.err != nil {
		return false
	}
	// Is next bucket in a spill?
	if b.spill != 0 {
		b.err = b.bucket.LoadFrom(b.spill, b.df)
		b.isSpill = true
		if b.elogger.Enabled() && b.err != nil {
			b.elogger.Error(b.err, "reading spill", "index", b.index, "spill", b.spill)
		}
	} else {
		lr := io.LimitedReader{R: b.r, N: b.blockSize}
		b.err = b.bucket.loadFullFrom(&lr)
		b.isSpill = false
		b.index++
		if b.elogger.Enabled() && b.err != nil && b.err != io.EOF {
			b.elogger.Error(b.err, "reading bucket", "index", b.index)
		}
	}

	if b.err != nil {
		b.spill = b.bucket.spill
	}

	return b.err == nil
}

// Index returns the index of the current bucket. Should not be called until Next has been called. Spill buckets
// share an index with their parent.
func (b *BucketScanner) Index() int {
	return b.index
}

// IsSpill reports whether the current bucket was read from a data store spill.
func (b *BucketScanner) IsSpill() bool {
	return b.isSpill
}

// Bucket returns the current bucket. Should not be called until Next has been called. The bucket is backed by data
// that may be overwritten with a call to Next so should not be retained.
func (b *BucketScanner) Bucket() *Bucket {
	if b.err != nil {
		return nil
	}
	return b.bucket
}

// Err returns the first non-EOF error that was encountered by the BucketScanner.
func (b *BucketScanner) Err() error {
	if b.err == io.EOF {
		return nil
	}
	return b.err
}

func (b *BucketScanner) Close() error {
	return b.closer.Close()
}

type LogFile struct {
	Path   string
	Header LogFileHeader

	file    *os.File
	writer  *bufio.Writer
	elogger logr.Logger
}

// OpenLogFile opens a log file for appending, creating it if necessary.
func OpenLogFile(path string) (*LogFile, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	err = Fadvise(int(f.Fd()), 0, 0, FADV_RANDOM)
	if err != nil {
		return nil, fmt.Errorf("fadvise: %w", err)
	}

	return &LogFile{
		Path: path,
		file: f,
		// Buffered writes to avoid write amplification
		writer:  bufio.NewWriterSize(f, 32*block_size(path)),
		elogger: logr.Discard(),
	}, nil
}

func (l *LogFile) Sync() error {
	if err := l.writer.Flush(); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *LogFile) Flush() error {
	return l.writer.Flush()
}

func (l *LogFile) Close() error {
	if err := l.writer.Flush(); err != nil {
		return err
	}
	return l.file.Close()
}

func (l *LogFile) Truncate() error {
	l.writer.Reset(l.file)
	return l.file.Truncate(0)
}

func (l *LogFile) Prepare(df *DataFile, kf *KeyFile) error {
	// Prepare rollback information
	lh := LogFileHeader{
		Version:   currentVersion,
		UID:       kf.Header.UID,
		AppNum:    kf.Header.AppNum,
		Salt:      kf.Header.Salt,
		Pepper:    pepper(kf.Header.Salt),
		BlockSize: kf.Header.BlockSize,
	}

	var err error
	lh.DatFileSize, err = df.Size()
	if err != nil {
		return fmt.Errorf("data file size: %w", err)
	}

	lh.KeyFileSize, err = kf.Size()
	if err != nil {
		return fmt.Errorf("key file size: %w", err)
	}

	if err := lh.EncodeTo(l.writer); err != nil {
		return fmt.Errorf("write log file header: %w", err)
	}

	// Checkpoint
	if err := l.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	return nil
}

func (l *LogFile) AppendBucket(idx int, b *Bucket) (int64, error) {
	var idxBuf [SizeUint64]byte
	EncodeUint64(idxBuf[:], uint64(idx))
	n, err := l.writer.Write(idxBuf[:])
	if err == nil && n != len(idxBuf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return int64(n), fmt.Errorf("write index: %w", err)
	}

	bn, err := b.WriteTo(l.writer)
	if err != nil {
		if l.elogger.Enabled() && errors.Is(err, io.ErrShortWrite) {
			l.elogger.Info("log file: short write on bucket data", "expected", b.ActualSize(), "wrote", bn)
		}
		return bn + int64(n), fmt.Errorf("write data: %w", err)
	}

	return bn + int64(n), nil
}

// SectionWriter implements Write on a section of an underlying WriterAt
type SectionWriter struct {
	w      io.WriterAt
	offset int64
	limit  int64
}

func NewSectionWriter(w io.WriterAt, offset int64, size int64) *SectionWriter {
	return &SectionWriter{
		w:      w,
		offset: offset,
		limit:  offset + size,
	}
}

func (s *SectionWriter) Write(v []byte) (int, error) {
	size := int64(len(v))
	if size > s.limit-s.offset {
		size = s.limit - s.offset
	}

	n, err := s.w.WriteAt(v[:size], s.offset)
	s.offset += int64(n)
	if err == nil && n < len(v) {
		err = io.ErrShortWrite
	}
	return n, err
}
