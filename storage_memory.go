package walspool

import (
	"sync"
)

// MemoryStorageEngine is an in-memory implementation of StorageEngine.
// Conforms behaviorally to the same postconditions as the disk-backed engine.
type MemoryStorageEngine struct {
	mu          sync.RWMutex
	records     []Record
	checkpoint  Offset
	closed      bool
	maxCapacity int
}

// NewMemoryStorageEngine creates a thread-safe in-memory storage engine.
func NewMemoryStorageEngine(maxCapacity int) *MemoryStorageEngine {
	if maxCapacity <= 0 {
		maxCapacity = 10000
	}
	return &MemoryStorageEngine{
		records:     make([]Record, 0, 128),
		maxCapacity: maxCapacity,
	}
}

func (m *MemoryStorageEngine) Append(rec Record) (Offset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, ErrStorageUnavailable
	}

	uncommitted := len(m.records) - int(m.checkpoint)
	if uncommitted >= m.maxCapacity {
		return 0, ErrSpoolFull
	}

	rec.Offset = Offset(len(m.records))
	m.records = append(m.records, rec)
	return rec.Offset, nil
}

func (m *MemoryStorageEngine) ReadBatch(maxCount int) ([]Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageUnavailable
	}

	startIdx := int(m.checkpoint)
	if startIdx >= len(m.records) {
		return nil, nil
	}

	endIdx := startIdx + maxCount
	if endIdx > len(m.records) {
		endIdx = len(m.records)
	}

	batch := make([]Record, endIdx-startIdx)
	copy(batch, m.records[startIdx:endIdx])
	return batch, nil
}

func (m *MemoryStorageEngine) Commit(offset Offset) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageUnavailable
	}

	// Commit updates high-water mark; offset is 0-indexed inclusive.
	newCheckpoint := offset + 1
	if newCheckpoint > m.checkpoint {
		m.checkpoint = newCheckpoint
	}
	return nil
}

func (m *MemoryStorageEngine) UncommittedCount() (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0, ErrStorageUnavailable
	}

	uncommitted := len(m.records) - int(m.checkpoint)
	if uncommitted < 0 {
		return 0, nil
	}
	return uncommitted, nil
}

func (m *MemoryStorageEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}
