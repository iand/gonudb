package internal

import (
	"fmt"
	"sync"
)

// Buffers data records in a map
type Pool struct {
	mu       sync.RWMutex // guards index, records and dataSize
	index    map[string]int
	records  []DataRecord
	dataSize int
}

func NewPool(sizeHint int) *Pool {
	return &Pool{
		index:   make(map[string]int, sizeHint),
		records: make([]DataRecord, sizeHint),
	}
}

func (p *Pool) IsEmpty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.records) == 0
}

// Count returns the number of data records in the pool
func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.records)
}

// DataSize returns the sum of data sizes in the pool
func (p *Pool) DataSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dataSize
}

func (p *Pool) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dataSize = 0
	p.records = p.records[:0]
	for k := range p.index {
		delete(p.index, k)
	}
}

func (p *Pool) Find(key string) ([]byte, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	idx, exists := p.index[key]
	if !exists {
		return nil, false
	}
	return p.records[idx].data, true
}

func (p *Pool) Has(key string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.index[key]
	return exists
}

func (p *Pool) Insert(hash uint64, key string, value []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.index[key]; exists {
		panic("duplicate key inserted: " + key)
	}

	// TODO: review need to make copy of value
	r := DataRecord{
		hash: hash,
		key:  key,
		data: make([]byte, len(value)),
		size: int64(len(value)),
	}
	copy(r.data, value)

	p.records = append(p.records, r)
	p.index[key] = len(p.records) - 1
	p.dataSize += len(value)
}

func (p *Pool) WithRecords(fn func([]DataRecord) error) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return fn(p.records)
}

func (p *Pool) WriteRecords(df *DataFile) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	written := int64(0)
	for i := range p.records {
		offset, err := df.AppendRecord(&p.records[i])
		if err != nil {
			return written, fmt.Errorf("encode record: %w", err)
		}
		// if s.tlogger.Enabled() {
		// 	s.tlogger.Info("wrote p0 record", "index", i, "offset", offset, "record_key", rs[i].key, "record_size", rs[i].size)
		// }
		p.records[i].offset = offset
		written += p.records[i].size
	}

	return written, nil
}
