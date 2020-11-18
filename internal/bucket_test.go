package internal

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestEntry(t *testing.T) {
	blob := make([]byte, BucketHeaderSize+BucketEntrySize*2)

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

	pos := BucketHeaderSize
	for i := range entries {
		EncodeUint48(blob[pos:pos+SizeUint48], uint64(entries[i].Offset))
		EncodeUint48(blob[pos+SizeUint48:pos+SizeUint48*2], uint64(entries[i].Size))
		EncodeUint64(blob[pos+SizeUint48*2:pos+SizeUint48*2+SizeUint64], entries[i].Hash)
		pos += BucketEntrySize
	}

	b := Bucket{
		blockSize: len(blob),
		blob:      blob,
	}

	for i := range entries {
		got := b.entry(i)
		if got != entries[i] {
			t.Errorf("%d: got %+v, wanted %+v", i, got, entries[i])
		}
	}
}

func TestInsert(t *testing.T) {
	blob := make([]byte, BucketHeaderSize+BucketEntrySize*2)

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

	b := Bucket{
		blockSize: len(blob),
		blob:      blob,
	}

	for i := range entries {
		b.insert(entries[i].Offset, entries[i].Size, entries[i].Hash)
	}

	for i := range entries {
		got := b.entry(i)
		if got != entries[i] {
			t.Errorf("%d: got %+v, wanted %+v", i, got, entries[i])
		}
	}
}

func TestErase(t *testing.T) {
	blob := make([]byte, BucketHeaderSize+BucketEntrySize*2)

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

	b := Bucket{
		blockSize: len(blob),
		blob:      blob,
	}

	for i := range entries {
		b.insert(entries[i].Offset, entries[i].Size, entries[i].Hash)
	}

	b.erase(0)

	for i := 1; i < len(entries); i++ {
		got := b.entry(i - 1)
		if got != entries[i] {
			t.Errorf("%d: got %+v, wanted %+v", i, got, entries[i])
		}
	}
}

func TestWriteReadFull(t *testing.T) {
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
	size := BucketHeaderSize + BucketEntrySize*2

	b := Bucket{
		blockSize: size,
		blob:      make([]byte, size),
	}

	for i := range entries {
		b.insert(entries[i].Offset, entries[i].Size, entries[i].Hash)
	}

	buf := &bytes.Buffer{}
	_, err := b.storeFullTo(buf)
	if err != nil {
		t.Fatalf("unexpected error during write: %v", err)
	}

	b2 := Bucket{
		blockSize: size,
		blob:      make([]byte, size),
	}

	err = b2.loadFullFrom(buf)
	if err != nil {
		t.Fatalf("unexpected error during read: %v", err)
	}

	for i := 0; i < len(entries); i++ {
		got := b2.entry(i)
		if got != entries[i] {
			t.Errorf("%d: got %+v, wanted %+v", i, got, entries[i])
		}
	}
}

func TestBucketHas(t *testing.T) {
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
	size := BucketHeaderSize + BucketEntrySize*2

	b := Bucket{
		blockSize: size,
		blob:      make([]byte, size),
	}

	for i := range entries {
		b.insert(entries[i].Offset, entries[i].Size, entries[i].Hash)
	}

	for i := range entries {
		if !b.Has(entries[i].Hash) {
			t.Errorf("did not find hash %d", entries[i].Hash)
		}
	}

	if b.Has(112233) {
		t.Errorf("unexpectedly found hash %d", 112233)
	}
}

func TestEntryDuplicateHashes(t *testing.T) {
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
		{
			Offset: 35555,
			Size:   34444,
			Hash:   19999,
		},
		{
			Offset: 45555,
			Size:   44444,
			Hash:   19999,
		},
	}

	blob := make([]byte, BucketHeaderSize+BucketEntrySize*len(entries))
	b := Bucket{
		blockSize: len(blob),
		blob:      blob,
	}

	for i := range entries {
		b.insert(entries[i].Offset, entries[i].Size, entries[i].Hash)
	}

	testCases := []struct {
		hash  uint64
		count int
	}{

		{
			hash:  19999,
			count: 3,
		},
		{
			hash:  29999,
			count: 1,
		},
		{
			hash:  39999,
			count: 0,
		},
	}

	for _, tc := range testCases {
		var got []Entry
		for i := b.lowerBound(tc.hash); i < b.count; i++ {
			entry := b.entry(i)
			if entry.Hash != tc.hash {
				break
			}
			got = append(got, entry)
		}

		if len(got) != tc.count {
			t.Errorf("%d: got %d, wanted %d", tc.hash, len(got), tc.count)
		}

	}
}

func TestBucketMaybeSpill(t *testing.T) {
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
	blockSize := BucketHeaderSize + BucketEntrySize*len(entries)

	tmpdir, err := ioutil.TempDir("", "gonudb.*")
	if err != nil {
		t.Fatalf("unexpected error creating temp directory: %v", err)
	}
	defer os.RemoveAll(tmpdir)

	t.Run("empty bucket does not spill", func(t *testing.T) {
		b := Bucket{
			blockSize: blockSize,
			blob:      make([]byte, blockSize),
		}
		tmpfile := filepath.Join(tmpdir, "empty")

		if err := CreateDataFile(tmpfile, 5, 6); err != nil {
			t.Fatalf("unexpected error creating data file: %v", err)
		}

		df, err := OpenDataFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error opening data file: %v", err)
		}

		_, err = b.maybeSpill(df)
		if err != nil {
			t.Fatalf("unexpected error during write: %v", err)
		}
		df.Close()
		written, err := ioutil.ReadFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error reading data file: %v", err)
		}

		if len(written) != DatFileHeaderSize {
			t.Errorf("got %d bytes written, wanted %d", len(written), DatFileHeaderSize)
		}
	})

	t.Run("half full bucket does not spill", func(t *testing.T) {
		b := Bucket{
			blockSize: blockSize,
			blob:      make([]byte, blockSize),
		}
		b.insert(entries[0].Offset, entries[0].Size, entries[0].Hash)

		tmpfile := filepath.Join(tmpdir, "half")

		if err := CreateDataFile(tmpfile, 5, 6); err != nil {
			t.Fatalf("unexpected error creating data file: %v", err)
		}

		df, err := OpenDataFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error opening data file: %v", err)
		}

		_, err = b.maybeSpill(df)
		if err != nil {
			t.Fatalf("unexpected error during write: %v", err)
		}
		df.Close()
		written, err := ioutil.ReadFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error reading data file: %v", err)
		}

		if len(written) != DatFileHeaderSize {
			t.Errorf("got %d bytes written, wanted %d", len(written), DatFileHeaderSize)
		}
	})

	t.Run("full bucket does spill", func(t *testing.T) {
		b := Bucket{
			blockSize: blockSize,
			blob:      make([]byte, blockSize),
		}
		b.insert(entries[0].Offset, entries[0].Size, entries[0].Hash)
		b.insert(entries[1].Offset, entries[1].Size, entries[1].Hash)

		tmpfile := filepath.Join(tmpdir, "full")

		if err := CreateDataFile(tmpfile, 5, 6); err != nil {
			t.Fatalf("unexpected error creating data file: %v", err)
		}

		df, err := OpenDataFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error opening data file: %v", err)
		}

		_, err = b.maybeSpill(df)
		if err != nil {
			t.Fatalf("unexpected error during write: %v", err)
		}
		df.Close()

		written, err := ioutil.ReadFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error reading data file: %v", err)
		}

		if len(written) != DatFileHeaderSize+BucketHeaderSize+blockSize {
			t.Errorf("got %d bytes written, wanted %d", len(written), BucketHeaderSize+blockSize)
		}
		if b.Spill() != DatFileHeaderSize {
			t.Errorf("got spill %d, wanted %d", b.Spill(), DatFileHeaderSize)
		}
		if b.Count() != 0 {
			t.Errorf("got %d entries in bucket, wanted %d", b.Count(), 0)
		}

		marker := DecodeUint48(written[DatFileHeaderSize : DatFileHeaderSize+SizeUint48])
		if marker != 0 {
			t.Errorf("got marker %x, wanted %x", marker, 0)
		}

		size := int64(DecodeUint16(written[DatFileHeaderSize+SizeUint48 : DatFileHeaderSize+SizeUint48+SizeUint16]))
		if size != int64(blockSize) {
			t.Errorf("got size %d, wanted %d", size, blockSize)
		}
	})

	t.Run("read from spill", func(t *testing.T) {
		b := Bucket{
			blockSize: blockSize,
			blob:      make([]byte, blockSize),
		}
		b.insert(entries[0].Offset, entries[0].Size, entries[0].Hash)
		b.insert(entries[1].Offset, entries[1].Size, entries[1].Hash)

		tmpfile := filepath.Join(tmpdir, "read")

		if err := CreateDataFile(tmpfile, 5, 6); err != nil {
			t.Fatalf("unexpected error creating data file: %v", err)
		}

		df, err := OpenDataFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error opening data file: %v", err)
		}

		_, err = b.maybeSpill(df)
		if err != nil {
			t.Fatalf("unexpected error during write: %v", err)
		}
		df.Close()
		written, err := ioutil.ReadFile(tmpfile)
		if err != nil {
			t.Fatalf("unexpected error reading data file: %v", err)
		}

		r := io.NewSectionReader(bytes.NewReader(written), DatFileHeaderSize+SizeUint48+SizeUint16, int64(blockSize))

		b2 := Bucket{
			blockSize: blockSize,
			blob:      make([]byte, blockSize),
		}

		err = b2.loadFullFrom(r)
		if err != nil {
			t.Fatalf("unexpected error during read: %v", err)
		}
		if b2.Count() != 2 {
			t.Errorf("got count %d, wanted %d", b.Count(), 2)
		}

		for i := 0; i < len(entries); i++ {
			got := b2.entry(i)
			if got != entries[i] {
				t.Errorf("%d: got %+v, wanted %+v", i, got, entries[i])
			}
		}
	})
}
