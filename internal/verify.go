package internal

import (
	"fmt"

	"github.com/go-logr/logr"
)

// VerifyStore verifies consistency of the data and key files.
func VerifyStore(datPath, keyPath string, logger logr.Logger) (*VerifyResult, error) {
	df, err := OpenDataFile(datPath)
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}
	defer df.Close()
	if logger != nil {
		df.elogger = logger
	}
	logger.Info("opened data file", "version", df.Header.Version, "uid", df.Header.UID, "appnum", df.Header.AppNum)

	kf, err := OpenKeyFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("open key file: %w", err)
	}
	defer kf.Close()
	if logger != nil {
		kf.elogger = logger
	}
	logger.Info("opened key file", "version", kf.Header.Version, "uid", kf.Header.UID, "appnum", kf.Header.AppNum, "buckets", kf.Header.Buckets, "block_size", kf.Header.BlockSize, "load_factor", kf.Header.LoadFactor)

	if err := df.Header.VerifyMatchingKey(&kf.Header); err != nil {
		return nil, fmt.Errorf("key file and data file have incompatible metadata: %w", err)
	}

	kfSize, err := kf.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to read key file size: %w", err)
	}

	if kf.Header.Buckets < 1 {
		return nil, fmt.Errorf("possibly corrupt key file: file should contain at least one bucket")
	}

	expectedFileSize := int64(KeyFileHeaderSize) + (int64(kf.Header.BlockSize) * int64(kf.Header.Buckets))
	if kfSize != expectedFileSize {
		return nil, fmt.Errorf("possibly corrupt key file: file size %d does not match expected %d", kfSize, expectedFileSize)
	}

	if kfSize < int64(kf.Header.BlockSize) {
		return nil, fmt.Errorf("possibly corrupt key file, file smaller than a single block")
	}

	res := &VerifyResult{
		DatPath:    datPath,
		KeyPath:    keyPath,
		Version:    df.Header.Version,
		UID:        df.Header.UID,
		AppNum:     df.Header.AppNum,
		Salt:       kf.Header.Salt,
		Pepper:     kf.Header.Pepper,
		BlockSize:  kf.Header.BlockSize,
		LoadFactor: float64(kf.Header.LoadFactor) / float64(MaxUint16),
		Capacity:   kf.Header.Capacity,
		Buckets:    kf.Header.Buckets,
		Modulus:    kf.Header.Modulus,
	}

	res.DatFileSize, err = df.Size()
	if err != nil {
		return nil, fmt.Errorf("reading data file size: %w", err)
	}
	res.KeyFileSize, err = kf.Size()
	if err != nil {
		return nil, fmt.Errorf("reading key file size: %w", err)
	}

	// Verify records
	rs := df.RecordScanner()
	defer rs.Close()
	totalFetches := 0
	for rs.Next() {
		res.RecordBytesTotal += rs.RecordSize()
		if rs.IsData() {
			res.ValueCountTotal++
			res.ValueBytesTotal += rs.Size()
		} else {
			res.SpillCountTotal++
			res.SpillBytesTotal += rs.Size()
		}

		fetches, err := countFetches(rs.Key(), df, kf)
		if err != nil {
			return nil, fmt.Errorf("counting fetches: %w", err)
		}
		totalFetches += fetches

	}
	if rs.Err() != nil {
		return nil, fmt.Errorf("scanning data file: %w", rs.Err())
	}

	if res.DatFileSize != res.RecordBytesTotal+DatFileHeaderSize {
		return nil, fmt.Errorf("data file size mismatch: file size is %d, size of records is %d (diff: %d)", res.DatFileSize, res.RecordBytesTotal+DatFileHeaderSize, res.DatFileSize-(res.RecordBytesTotal+DatFileHeaderSize))
	}

	// Verify buckets
	bs := kf.BucketScanner(df)
	defer bs.Close()
	for bs.Next() {
		b := bs.Bucket()
		res.KeyCount += int64(b.Count())
		if bs.IsSpill() {
			res.SpillCountInUse++
			res.SpillBytesInUse += SpillHeaderSize + int64(b.ActualSize())
			res.RecordBytesInUse += SpillHeaderSize + int64(b.ActualSize())
		}

		for i := 0; i < b.Count(); i++ {
			e := b.entry(i)
			ehdr, err := df.LoadRecordHeader(e.Offset)
			if err != nil {
				return nil, fmt.Errorf("load record header at offset %d: %w", e.Offset, bs.Err())
			}

			if !ehdr.IsData() {
				return nil, fmt.Errorf("record type mismatch at offset %d, key file expects data record", e.Offset)
			}

			if ehdr.DataSize != e.Size {
				return nil, fmt.Errorf("record size mismatch at offset %d, data file record size %d, key file expects size %d", e.Offset, ehdr.DataSize, e.Size)
			}

			hash := kf.Hash(ehdr.Key)
			if hash != e.Hash {
				return nil, fmt.Errorf("record key hash mismatch at offset %d, data file record hash %d, key file expects hash %d", e.Offset, hash, e.Hash)
			}

			res.ValueCountInUse++
			res.ValueBytesInUse += ehdr.DataSize
			res.RecordBytesInUse += ehdr.Size() + ehdr.DataSize
		}

	}
	if bs.Err() != nil {
		return nil, fmt.Errorf("scanning key file (index: %d): %w", bs.Index(), bs.Err())
	}

	res.Waste = float64(res.SpillBytesTotal-res.SpillBytesInUse) / float64(res.DatFileSize)
	res.ActualLoad = float64(res.KeyCount) / float64(res.Capacity*res.Buckets)

	if res.ValueCountInUse > 0 {
		res.Overhead = float64(res.KeyFileSize+res.DatFileSize) / float64(res.RecordBytesTotal)
		res.AverageFetch = float64(totalFetches) / float64(res.ValueCountInUse)
	}

	return res, nil
}

func countFetches(key string, df *DataFile, kf *KeyFile) (int, error) {
	fetches := 0
	h := kf.HashString(key)

	idx := BucketIndex(h, kf.Header.Buckets, kf.Header.Modulus)
	tmpb, err := kf.LoadBucket(idx)
	if err != nil {
		return fetches, fmt.Errorf("read bucket: %w", err)
	}
	fetches++
	for {
		for i := tmpb.lowerBound(h); i < tmpb.count; i++ {
			entry := tmpb.entry(i)
			if entry.Hash != h {
				break
			}

			ehdr, err := df.LoadRecordHeader(entry.Offset)
			if err != nil {
				return fetches, fmt.Errorf("read data record: %w", err)
			}
			fetches++

			if string(ehdr.Key) != key {
				continue
			}

			return fetches, nil
		}

		spill := tmpb.Spill()

		if spill == 0 {
			break
		}

		blockBuf := make([]byte, kf.Header.BlockSize)
		tmpb = NewBucket(int(kf.Header.BlockSize), blockBuf)
		if err := tmpb.LoadFrom(int64(spill), df); err != nil {
			return fetches, fmt.Errorf("read spill: %w", err)
		}
		fetches++

	}

	// record not reachable from the key file so don't count it as a fetch
	return 0, nil
}

type VerifyResult struct {
	DatPath    string  // The path to the data file
	KeyPath    string  // The path to the key file
	Version    uint16  // The API version used to create the database
	UID        uint64  // The unique identifier
	AppNum     uint64  // The application-defined constant
	Salt       uint64  // The salt used in the key file
	Pepper     uint64  // The salt fingerprint
	BlockSize  uint16  // The block size used in the key file
	LoadFactor float64 // The target load factor used in the key file

	KeyFileSize int64 // The size of the key file in bytes
	DatFileSize int64 // The size of the data file in bytes
	Capacity    int   // The maximum number of keys each bucket can hold
	Buckets     int   // The number of buckets in the key file
	BucketSize  int64 // The size of a bucket in bytes
	Modulus     uint64

	KeyCount         int64   // The number of keys found
	ValueCountInUse  int64   // The number of values found that are referenced by a key
	ValueCountTotal  int64   // The number of values found
	ValueBytesInUse  int64   // The total number of bytes occupied by values that are referenced by a key
	ValueBytesTotal  int64   // The total number of bytes occupied by values
	RecordBytesInUse int64   // The total number of bytes occupied by records (header + value) that are referenced by a key
	RecordBytesTotal int64   // The total number of bytes occupied by records (header + value)
	SpillCountInUse  int64   // The number of spill records in use
	SpillCountTotal  int64   // The total number of spill records
	SpillBytesInUse  int64   // The number of bytes occupied by spill records in use
	SpillBytesTotal  int64   // The number of bytes occupied by all spill records
	AverageFetch     float64 // Average number of key file reads per fetch
	Waste            float64 // The fraction of the data file that is wasted
	Overhead         float64 // The data amplification ratio (size of data files compared to the size of the underlying data and keys)
	ActualLoad       float64 // The measured bucket load fraction (number of keys as a fraction of the total capacity)
}
