package internal

// CacheData is a read only view of a bucket cache. It is safe for concurrent use.
type CacheData struct {
	index   map[int]int
	buckets []BucketRecord
}

func (c *CacheData) Find(n int) (*Bucket, bool) {
	idx, exists := c.index[n]
	if !exists {
		return nil, false
	}
	return c.buckets[idx].bucket, true
}

func (c *CacheData) Has(n int) bool {
	_, exists := c.index[n]
	return exists
}

func (c *CacheData) Count() int {
	return len(c.buckets)
}

func (c *CacheData) WithBuckets(fn func(bs []BucketRecord) error) error {
	return fn(c.buckets)
}

// Cache is an in memory buffer of buckets. It is not safe for concurrent use.
type Cache struct {
	keySize   int
	blockSize int
	sizeHint  int

	data *CacheData
}

func NewCache(keySize int, blockSize int, sizeHint int) *Cache {
	return &Cache{
		keySize:   keySize,
		blockSize: blockSize,
		sizeHint:  sizeHint,
		data: &CacheData{
			index:   make(map[int]int, sizeHint),
			buckets: make([]BucketRecord, 0, sizeHint),
		},
	}
}

func (c *Cache) Find(n int) (*Bucket, bool) {
	return c.data.Find(n)
}

func (c *Cache) Has(n int) bool {
	return c.data.Has(n)
}

func (c *Cache) Count() int {
	return c.data.Count()
}

func (c *Cache) WithBuckets(fn func(bs []BucketRecord) error) error {
	return c.data.WithBuckets(fn)
}

func (c *Cache) Insert(idx int, b *Bucket) {
	br := BucketRecord{
		idx:    idx,
		bucket: b,
	}

	c.data.buckets = append(c.data.buckets, br)
	c.data.index[idx] = len(c.data.buckets) - 1
}

func (c *Cache) Clear() {
	c.data = &CacheData{
		index:   make(map[int]int, c.sizeHint),
		buckets: make([]BucketRecord, 0, c.sizeHint),
	}
}

// TakeData takes ownership of the Cache's data. The Cache is cleared after.
func (c *Cache) TakeData() *CacheData {
	data := c.data
	c.data = &CacheData{
		index:   make(map[int]int, c.sizeHint),
		buckets: make([]BucketRecord, 0, c.sizeHint),
	}
	return data
}
