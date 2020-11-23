package internal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

type Store struct {
	// Fields written when open or close is called
	df *DataFile
	kf *KeyFile
	lf *LogFile

	// Currently imu guards all calls to p0 and bc methods
	imu sync.Mutex
	p0  *Pool
	bc  *BucketCache

	rmu  sync.Mutex // guards acess to rate and when
	rate float64    // rate at which data can be flushed
	when time.Time

	emu  sync.Mutex // guards access to open and err
	open bool
	err  error

	monitor chan struct{}

	elogger logr.Logger // error logger
	dlogger logr.Logger // diagnostics logger
	tlogger logr.Logger // trace logger
}

func CreateStore(datPath, keyPath, logPath string, appnum, uid, salt uint64, blockSize int, loadFactor float64) error {
	// TODO make this a constant MaxBlockSize
	if blockSize > MaxBlockSize {
		return ErrInvalidBlockSize
	}

	if loadFactor <= 0 || loadFactor >= 1 {
		return ErrInvalidLoadFactor
	}

	capacity := BucketCapacity(blockSize)
	if capacity < 1 {
		return ErrInvalidBlockSize
	}

	if err := CreateDataFile(datPath, appnum, uid); err != nil {
		return fmt.Errorf("create data file: %w", err)
	}

	if err := CreateKeyFile(keyPath, uid, appnum, salt, blockSize, loadFactor); err != nil {
		return fmt.Errorf("create key file: %w", err)
	}

	return nil
}

func OpenStore(datPath, keyPath, logPath string, syncInterval time.Duration, elogger logr.Logger, dlogger logr.Logger, tlogger logr.Logger) (*Store, error) {
	df, err := OpenDataFile(datPath)
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}

	abandon := func() {
		df.Close() // TODO: handle close error
	}

	kf, err := OpenKeyFile(keyPath)
	if err != nil {
		abandon()
		return nil, fmt.Errorf("open key file: %w", err)
	}

	abandon = func() {
		df.Close() // TODO: handle close error
		kf.Close() // TODO: handle close error
	}

	if err := df.Header.VerifyMatchingKey(&kf.Header); err != nil {
		abandon()
		return nil, fmt.Errorf("verify key file matches data file: %w", err)
	}

	if kf.Header.Buckets < 1 {
		abandon()
		return nil, ErrShortKeyFile
	}

	lf, err := OpenLogFile(logPath)
	if err != nil {
		abandon()

		var pathErr *os.PathError
		if errors.As(err, &pathErr) && os.IsExist(pathErr) {
			return nil, fmt.Errorf("log file exists, store requires recovery")
		}

		return nil, fmt.Errorf("open log file: %w", err)
	}

	df.elogger = elogger
	kf.elogger = elogger
	lf.elogger = elogger

	s := &Store{
		when: time.Now(),
		df:   df,
		kf:   kf,
		lf:   lf,

		p0: NewPool(0),
		bc: &BucketCache{
			bucketSize: int(kf.Header.BlockSize),
			modulus:    ceil_pow2(uint64(kf.Header.Buckets)),
			buckets:    make([]*Bucket, int(kf.Header.Buckets)),
			dirty:      make([]bool, int(kf.Header.Buckets)),
			threshold:  (int(kf.Header.LoadFactor) * int(kf.Header.Capacity)) / 65536,
			tlogger:    tlogger,
		},

		open:    true,
		monitor: make(chan struct{}),

		elogger: elogger,
		dlogger: dlogger,
		tlogger: tlogger,
	}

	for idx := range s.bc.buckets {
		b, err := kf.LoadBucket(idx)
		if err != nil {
			return nil, fmt.Errorf("read bucket: %w", err)
		}
		s.bc.buckets[idx] = b
	}
	s.bc.computeStats(df)

	// Flush writes automatically
	go func() {
		d := time.NewTicker(syncInterval)

		for {
			select {

			case <-s.monitor:
				d.Stop()
				select {
				case <-d.C:
				default:
				}
				return

			case <-d.C:
				if s.tlogger.Enabled() {
					s.tlogger.Info("Background flush")
				}
				s.Flush()

			}
		}
	}()

	return s, nil
}

func (s *Store) Close() error {
	s.emu.Lock()
	open := s.open
	s.open = false
	s.emu.Unlock()
	if !open {
		return nil
	}

	close(s.monitor)

	s.imu.Lock()
	defer s.imu.Unlock()

	if !s.p0.IsEmpty() {
		if _, err := s.commit(); err != nil {
			if s.elogger.Enabled() {
				s.elogger.Error(err, "commit")
			}
			s.setErr(err)
		}
	}

	// Return if the store is in an error state, such as from a failed flush
	if err := s.Err(); err != nil {
		return err
	}

	if s.lf != nil {
		if err := s.lf.Close(); err != nil {
			return fmt.Errorf("close log file: %w", err)
		}

		if err := os.Remove(s.lf.Path); err != nil {
			return fmt.Errorf("delete log file: %w", err)
		}
	}

	if err := s.kf.Close(); err != nil {
		return fmt.Errorf("close key file: %w", err)
	}

	if err := s.df.Close(); err != nil {
		return fmt.Errorf("close data file: %w", err)
	}

	return nil
}

// Err returns an error if the store is in an error state, nil otherwise
func (s *Store) Err() error {
	s.emu.Lock()
	defer s.emu.Unlock()
	return s.err
}

func (s *Store) setErr(err error) {
	s.emu.Lock()
	s.err = err
	s.emu.Unlock()
}

func (s *Store) DataFile() *DataFile { return s.df }
func (s *Store) KeyFile() *KeyFile   { return s.kf }
func (s *Store) LogFile() *LogFile   { return s.lf }

func (s *Store) RecordCount() int {
	return s.bc.EntryCount()
}

func (s *Store) Rate() float64 {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	return s.rate
}

func (s *Store) Insert(key string, data []byte) error {
	if s.tlogger.Enabled() {
		s.tlogger.Info("Store.Insert", "key", key, "data_len", len(data))
	}

	// Return if the store is in an error state, such as from a failed flush
	if err := s.Err(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrKeyMissing
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(data) == 0 {
		return ErrDataMissing
	} else if len(data) > MaxDataSize {
		return ErrDataTooLarge
	}

	s.imu.Lock()
	err := s.insert(key, data)
	s.imu.Unlock()

	if err != nil {
		return err
	}

	// Calculate throttling
	now := time.Now()
	s.rmu.Lock()
	elapsed := now.Sub(s.when)
	work := s.p0.DataSize() + 3*s.p0.Count()*int(s.kf.Header.BlockSize) // TODO: move this calculation into Pool
	rate := math.Ceil(float64(work) / elapsed.Seconds())
	sleep := s.rate > 0 && rate > s.rate
	s.rmu.Unlock()

	if s.dlogger.Enabled() {
		s.dlogger.Info("insert work rate", "rate", rate, "work", work, "time", elapsed.Seconds(), "throttle", sleep)
	}

	// The caller of insert must be blocked when the rate of insertion
	// (measured in approximate bytes per second) exceeds the maximum rate
	// that can be flushed. The precise sleep duration is not important.

	if sleep {
		time.Sleep(25 * time.Millisecond)
	}

	return nil
}

// insert expects caller to hold s.imu lock
func (s *Store) insert(key string, data []byte) error {
	h := s.kf.HashString(key)
	if s.p0.Has(key) {
		return ErrKeyExists
	}

	found, err := s.bc.Exists(h, key, s.df)
	if err != nil {
		return fmt.Errorf("exists in bucket: %w", err)
	}
	if found {
		return ErrKeyExists
	}

	// Perform insert
	if s.tlogger.Enabled() {
		s.tlogger.Info("inserting into pool p1", "key", key, "size", len(data))
	}
	s.p0.Insert(h, key, data)

	return nil
}

func (s *Store) Flush() {
	if s.tlogger.Enabled() {
		s.tlogger.Info("Store.Flush")
	}

	s.rmu.Lock()
	s.when = time.Now()
	s.rmu.Unlock()

	s.imu.Lock()
	defer s.imu.Unlock()

	if s.p0.IsEmpty() {
		// Nothing to flush
		return
	}

	work, err := s.commit()
	if err != nil {
		if s.elogger.Enabled() {
			s.elogger.Error(err, "flush")
		}
		s.setErr(err)
		return
	}

	now := time.Now()
	s.rmu.Lock()
	elapsed := now.Sub(s.when)
	s.rate = math.Ceil(float64(work) / elapsed.Seconds())
	s.rmu.Unlock()

	if s.dlogger.Enabled() {
		s.dlogger.Info("flush work rate", "rate", s.rate, "work", work, "time", elapsed.Seconds())
	}
}

// Currently expects s.imu to be held
func (s *Store) commit() (int64, error) {
	if s.tlogger.Enabled() {
		s.tlogger.Info("Store.commit")
	}

	if err := s.lf.Prepare(s.df, s.kf); err != nil {
		return 0, fmt.Errorf("prepare log: %w", err)
	}

	// Append data and spills to data file

	work, err := s.p0.WriteRecords(s.df)
	if err != nil {
		return 0, fmt.Errorf("write data file: %w", err)
	}

	if err := s.p0.WithRecords(func(rs []DataRecord) error {
		for i := range rs {
			err := s.bc.Insert(rs[i].offset, rs[i].size, rs[i].hash, s.df)
			if err != nil {
				return fmt.Errorf("bucket cache insert: %w", err)
			}
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("write to buckets: %w", err)
	}

	// Ensure any data written to data file is on disk.
	if err := s.df.Flush(); err != nil {
		return 0, fmt.Errorf("flush data file: %w", err)
	}
	// work += int(s.kf.Header.BlockSize) * (2*mutatedBuckets.Count() + newBuckets.Count())

	s.p0.Clear()

	written, err := s.bc.WriteDirty(s.lf, s.kf)
	work += written
	if err != nil {
		return work, fmt.Errorf("write dirty buckets: %w", err)
	}

	// Finalize the commit
	if err := s.df.Sync(); err != nil {
		return 0, fmt.Errorf("sync data file: %w", err)
	}

	if err := s.lf.Truncate(); err != nil {
		return 0, fmt.Errorf("trunc log file: %w", err)
	}

	if err := s.lf.Sync(); err != nil {
		return 0, fmt.Errorf("sync log file: %w", err)
	}

	return work, nil
}

func (s *Store) FetchReader(key string) (io.Reader, error) {
	if s.tlogger.Enabled() {
		s.tlogger.Info("Store.FetchReader", "key", key)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}

	h := s.kf.HashString(key)

	if s.tlogger.Enabled() {
		s.tlogger.Info("looking for data in pool p0", "key", key)
	}

	s.imu.Lock()
	defer s.imu.Unlock()

	if data, exists := s.p0.Find(key); exists {
		return bytes.NewReader(data), nil
	}

	r, err := s.bc.Fetch(h, key, s.df)
	if err != nil {
		return nil, fmt.Errorf("read bucket: %w", err)
	}
	return r, nil
}

func (s *Store) Exists(key string) (bool, error) {
	if s.tlogger.Enabled() {
		s.tlogger.Info("Store.Exists", "key", key)
	}
	if err := s.Err(); err != nil {
		return false, err
	}

	if s.p0.Has(key) {
		return true, nil
	}

	h := s.kf.HashString(key)
	return s.bc.Exists(h, key, s.df)
}

func (s *Store) DataSize(key string) (int64, error) {
	if s.tlogger.Enabled() {
		s.tlogger.Info("Store.DataSize", "key", key)
	}
	if err := s.Err(); err != nil {
		return 0, err
	}

	if data, exists := s.p0.Find(key); exists {
		return int64(len(data)), nil
	}

	h := s.kf.HashString(key)
	rh, err := s.bc.FetchHeader(h, key, s.df)
	if err != nil {
		return 0, fmt.Errorf("fetch header: %w", err)
	}

	return rh.DataSize, nil
}
