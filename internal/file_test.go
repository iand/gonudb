package internal

import (
	"os"
	"testing"
)

func TestCreateKeyFile(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "gonudb.*")
	if err != nil {
		t.Fatalf("unexpected error creating temp directory: %v", err)
	}
	defer os.RemoveAll(tmpdir)

	const blockSize = 256

	filename := tmpdir + "key"
	err = CreateKeyFile(filename, 121212, 222222, 333333, blockSize, 0.7)
	if err != nil {
		t.Errorf("CreateKeyFile: unexpected error: %v", err)
	}

	st, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("key file was not created")
		}
		t.Fatalf("Stat: unexpected error: %v", err)
	}

	wantSize := int64(KeyFileHeaderSize + blockSize)
	if st.Size() != wantSize {
		t.Errorf("got size %d, wanted %d", st.Size(), wantSize)
	}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: unexpected error: %v", err)
	}
	defer f.Close()

	var kh KeyFileHeader
	if err := kh.DecodeFrom(f, st.Size()); err != nil {
		t.Fatalf("DecodeFrom: unexpected error: %v", err)
	}
	if err := kh.Verify(); err != nil {
		t.Fatalf("Verify: unexpected error: %v", err)
	}

	if kh.UID != 121212 {
		t.Errorf("got uid %d, wanted %d", kh.UID, 121212)
	}
	if kh.AppNum != 222222 {
		t.Errorf("got appnum %d, wanted %d", kh.AppNum, 222222)
	}
	if kh.Salt != 333333 {
		t.Errorf("got salt %d, wanted %d", kh.Salt, 333333)
	}

	blob := make([]byte, blockSize)
	if _, err := f.ReadAt(blob, KeyFileHeaderSize); err != nil {
		t.Fatalf("ReadAt: unexpected error: %v", err)
	}

	for i, b := range blob {
		if b != 0 {
			t.Fatalf("non zero byte found in bucket blob at %d", i)
		}
	}
}

func TestTruncateLogFileWithoutError(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "gonudb.*")
	if err != nil {
		t.Fatalf("unexpected error creating temp directory: %v", err)
	}
	defer os.RemoveAll(tmpdir)

	filename := tmpdir + "log"
	lf, err := OpenLogFile(filename)
	if err != nil {
		t.Errorf("OpenLogFile: unexpected error: %v", err)
	}

	blob := make([]byte, BucketHeaderSize+BucketEntrySize*2)
	b := &Bucket{
		blockSize: len(blob),
		blob:      blob,
	}

	entries := []Entry{
		{
			Offset: 15555,
			Size:   14444,
			Hash:   19999,
		},
		{
			Offset: 25555,
			Size:   24444,
			Hash:   29999,
		},
	}

	for i := range entries {
		b.insert(entries[i].Offset, entries[i].Size, entries[i].Hash)
	}

	if _, err := lf.AppendBucket(0, b); err != nil {
		t.Errorf("AppendBucket: unexpected error: %v", err)
	}

	if err := lf.Flush(); err != nil {
		t.Errorf("Flush: unexpected error: %v", err)
	}

	if err := lf.Truncate(); err != nil {
		t.Fatalf("Truncate: unexpected error: %v", err)
	}
}
