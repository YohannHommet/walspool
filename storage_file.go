package walspool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStorageEngine is an append-only, crash-resilient disk storage engine.
// Writes are appended to a .wal log file; checkpoints are committed atomically.
type FileStorageEngine struct {
	mu          sync.RWMutex
	dir         string
	walFile     *os.File
	checkpoint  Offset
	records     []OffsetPos // in-memory index of [offset -> file byte position]
	maxCapacity int
	closed      bool
}

type OffsetPos struct {
	Offset Offset
	Pos    int64
	Length int
}

// NewFileStorageEngine opens or initializes a WAL directory.
// On startup, it replays the WAL from the last checkpoint to rebuild offsets.
func NewFileStorageEngine(dir string, maxCapacity int) (*FileStorageEngine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("%w: failed to create wal directory", ErrStorageUnavailable)
	}

	walPath := filepath.Join(dir, "active.wal")
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open wal file", ErrStorageUnavailable)
	}

	engine := &FileStorageEngine{
		dir:         dir,
		walFile:     walFile,
		maxCapacity: maxCapacity,
		records:     make([]OffsetPos, 0, 1024),
	}

	if err := engine.recover(); err != nil {
		_ = walFile.Close()
		return nil, err
	}

	return engine, nil
}

func (f *FileStorageEngine) recover() error {
	// Read checkpoint file if it exists.
	chkPath := filepath.Join(f.dir, "checkpoint.meta")
	if chkData, err := os.ReadFile(chkPath); err == nil && len(chkData) >= 8 {
		f.checkpoint = Offset(binary.BigEndian.Uint64(chkData[:8]))
	}

	// Scan WAL to rebuild the offset index.
	if _, err := f.walFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: seek error during recovery", ErrStorageUnavailable)
	}

	offsetCounter := Offset(0)
	for {
		curPos, err := f.walFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("%w: failed to get file pos", ErrStorageUnavailable)
		}

		header := make([]byte, headerSize)
		_, err = io.ReadFull(f.walFile, header)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: wal header read error", ErrStorageUnavailable)
		}

		topicLen := int(binary.BigEndian.Uint16(header[23:25]))
		payloadLen := int(binary.BigEndian.Uint32(header[25:29]))
		totalRecordLen := headerSize + topicLen + payloadLen

		f.records = append(f.records, OffsetPos{
			Offset: offsetCounter,
			Pos:    curPos,
			Length: totalRecordLen,
		})
		offsetCounter++

		// Skip body bytes to reach the next record.
		if _, err := f.walFile.Seek(int64(topicLen+payloadLen), io.SeekCurrent); err != nil {
			return fmt.Errorf("%w: failed to advance record body", ErrStorageUnavailable)
		}
	}

	// Seek back to end for subsequent appends.
	if _, err := f.walFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("%w: failed to seek end of wal", ErrStorageUnavailable)
	}

	return nil
}

func (f *FileStorageEngine) Append(rec Record) (Offset, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, ErrStorageUnavailable
	}

	uncommitted := len(f.records) - int(f.checkpoint)
	if uncommitted >= f.maxCapacity {
		return 0, ErrSpoolFull
	}

	newOffset := Offset(len(f.records))
	rec.Offset = newOffset

	data, err := rec.MarshalBinary()
	if err != nil {
		return 0, err
	}

	pos, err := f.walFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("%w: seek failed", ErrStorageUnavailable)
	}

	if _, err := f.walFile.Write(data); err != nil {
		return 0, fmt.Errorf("%w: write failed", ErrStorageUnavailable)
	}

	// Sync ensures durability against OS crash before returning contract success.
	if err := f.walFile.Sync(); err != nil {
		return 0, fmt.Errorf("%w: sync failed", ErrStorageUnavailable)
	}

	f.records = append(f.records, OffsetPos{
		Offset: newOffset,
		Pos:    pos,
		Length: len(data),
	})

	return newOffset, nil
}

func (f *FileStorageEngine) ReadBatch(maxCount int) ([]Record, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return nil, ErrStorageUnavailable
	}

	startIdx := int(f.checkpoint)
	if startIdx >= len(f.records) {
		return nil, nil
	}

	endIdx := startIdx + maxCount
	if endIdx > len(f.records) {
		endIdx = len(f.records)
	}

	batch := make([]Record, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		entry := f.records[i]
		buf := make([]byte, entry.Length)

		if _, err := f.walFile.ReadAt(buf, entry.Pos); err != nil {
			return nil, fmt.Errorf("%w: read error at offset %d", ErrStorageUnavailable, entry.Offset)
		}

		var rec Record
		if err := rec.UnmarshalBinary(buf); err != nil {
			return nil, fmt.Errorf("%w: corruption at offset %d", ErrStorageUnavailable, entry.Offset)
		}
		rec.Offset = entry.Offset
		batch = append(batch, rec)
	}

	return batch, nil
}

func (f *FileStorageEngine) Commit(offset Offset) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return ErrStorageUnavailable
	}

	newCheckpoint := offset + 1
	if newCheckpoint <= f.checkpoint {
		return nil
	}

	// Atomically write checkpoint via temporary file swap.
	tmpPath := filepath.Join(f.dir, "checkpoint.tmp")
	chkPath := filepath.Join(f.dir, "checkpoint.meta")

	chkBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(chkBytes, uint64(newCheckpoint))

	if err := os.WriteFile(tmpPath, chkBytes, 0644); err != nil {
		return fmt.Errorf("%w: failed to write checkpoint tmp", ErrStorageUnavailable)
	}

	if err := os.Rename(tmpPath, chkPath); err != nil {
		return fmt.Errorf("%w: failed to commit checkpoint", ErrStorageUnavailable)
	}

	f.checkpoint = newCheckpoint
	return nil
}

func (f *FileStorageEngine) UncommittedCount() (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return 0, ErrStorageUnavailable
	}

	count := len(f.records) - int(f.checkpoint)
	if count < 0 {
		return 0, nil
	}
	return count, nil
}

func (f *FileStorageEngine) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true
	return f.walFile.Close()
}
