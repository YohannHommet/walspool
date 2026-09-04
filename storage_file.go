package walspool

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// maxRecoverableRecordSize caps the byte length a single WAL record may claim during recovery.
// A corrupt header advertising a huge payloadLen would otherwise drive make([]byte, n) to OOM.
const maxRecoverableRecordSize = 10 * 1024 * 1024 // 10 MB

// SyncPolicy defines the disk synchronization strategy for write durability.
type SyncPolicy int

const (
	// SyncEveryRecord forces an fsync after every individual write (maximum durability, lowest throughput).
	SyncEveryRecord SyncPolicy = iota

	// SyncBatchCommit forces an fsync at the end of each acknowledged batch commit.
	SyncBatchCommit

	// SyncInterval performs asynchronous periodic fsync (e.g. every 50ms) using a background flusher.
	SyncInterval
)

// FileStorageOptions configures buffering and synchronization behavior for FileStorageEngine.
type FileStorageOptions struct {
	// BufferSize specifies the capacity of the userspace write buffer in bytes (default: 128KB).
	BufferSize int

	// SyncPolicy specifies the disk synchronization strategy (default: SyncInterval).
	SyncPolicy SyncPolicy

	// SyncInterval specifies the period between background fsync operations when SyncInterval is active (default: 50ms).
	SyncInterval time.Duration
}

// DefaultFileStorageOptions returns production-optimized baseline options.
func DefaultFileStorageOptions() FileStorageOptions {
	return FileStorageOptions{
		BufferSize:   128 * 1024, // 128 KB
		SyncPolicy:   SyncInterval,
		SyncInterval: 50 * time.Millisecond,
	}
}

// offsetPos maps a logical spooler offset to its physical byte offset and byte length in the WAL.
type offsetPos struct {
	Offset Offset
	Pos    int64
	Length int
}

// FileStorageEngine is an append-only, crash-resilient disk storage engine.
// Writes are buffered in memory and flushed to a .wal log file according to the configured SyncPolicy;
// checkpoints are committed atomically with directory fsync for maximum crash resilience.
type FileStorageEngine struct {
	mu          sync.RWMutex
	dir         string
	walFile     *os.File
	writer      *bufio.Writer
	writePos    int64
	checkpoint  Offset
	records     []offsetPos // in-memory index of [offset -> file byte position]
	maxCapacity int
	opts        FileStorageOptions
	closed      bool

	flusherStop chan struct{}
	flusherWg   sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// NewFileStorageEngine opens or initializes a WAL directory using default optimized options (SyncInterval, 128KB buffer).
// Precondition: dir must not be empty, maxCapacity must be > 0.
func NewFileStorageEngine(dir string, maxCapacity int) (*FileStorageEngine, error) {
	return NewFileStorageEngineWithOptions(dir, maxCapacity, DefaultFileStorageOptions())
}

// NewFileStorageEngineWithOptions opens or initializes a WAL directory with custom buffering and sync policies.
// Precondition: dir must not be empty, maxCapacity must be > 0.
func NewFileStorageEngineWithOptions(dir string, maxCapacity int, opts FileStorageOptions) (*FileStorageEngine, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: directory path must not be empty", ErrPreconditionViolated)
	}
	if maxCapacity <= 0 {
		return nil, fmt.Errorf("%w: max capacity must be positive", ErrPreconditionViolated)
	}

	if opts.BufferSize <= 0 {
		opts.BufferSize = 128 * 1024
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 50 * time.Millisecond
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("%w: failed to create wal directory", ErrStorageUnavailable)
	}

	walPath := filepath.Join(dir, "active.wal")
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open wal file", ErrStorageUnavailable)
	}

	engine := &FileStorageEngine{
		dir:         dir,
		walFile:     walFile,
		maxCapacity: maxCapacity,
		records:     make([]offsetPos, 0, 1024),
		opts:        opts,
	}

	if err := engine.recover(); err != nil {
		_ = walFile.Close()
		return nil, err
	}

	engine.writer = bufio.NewWriterSize(walFile, opts.BufferSize)

	if opts.SyncPolicy == SyncInterval {
		engine.flusherStop = make(chan struct{})
		engine.flusherWg.Add(1)
		go engine.periodicFlusher(opts.SyncInterval)
	}

	return engine, nil
}

func (f *FileStorageEngine) periodicFlusher(interval time.Duration) {
	defer f.flusherWg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.flusherStop:
			return
		case <-ticker.C:
			// Check f.closed directly rather than pattern-matching the error: a flush failing only because Close() already ran is benign shutdown noise, not a real failure to surface.
			if err := f.Flush(); err != nil {
				f.mu.RLock()
				closed := f.closed
				f.mu.RUnlock()
				if !closed {
					slog.Error("walspool: periodic WAL flush failed", "error", err)
				}
			}
		}
	}
}

// Flush forces any buffered data to be written to the OS file descriptor and synced to physical media.
// Thread-safe and explicit.
func (f *FileStorageEngine) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return ErrStorageUnavailable
	}

	if f.writer != nil && f.writer.Buffered() > 0 {
		if err := f.writer.Flush(); err != nil {
			return fmt.Errorf("%w: failed to flush writer buffer", ErrStorageUnavailable)
		}
	}

	if err := f.walFile.Sync(); err != nil {
		return fmt.Errorf("%w: failed to sync wal file", ErrStorageUnavailable)
	}

	return nil
}

func (f *FileStorageEngine) recover() error {
	// 1. Read checkpoint file if it exists.
	chkPath := filepath.Join(f.dir, "checkpoint.meta")
	if chkData, err := os.ReadFile(chkPath); err == nil && len(chkData) >= 8 {
		f.checkpoint = Offset(binary.BigEndian.Uint64(chkData[:8]))
	}

	// 2. Query real disk file size to counter POSIX lseek beyond EOF behavior.
	stat, err := f.walFile.Stat()
	if err != nil {
		return fmt.Errorf("%w: failed to stat wal file during recovery", ErrStorageUnavailable)
	}
	fileSize := stat.Size()

	if _, err := f.walFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: seek error during recovery", ErrStorageUnavailable)
	}

	validFileEnd := int64(0)
	offsetCounter := Offset(0)

	for {
		curPos, err := f.walFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("%w: failed to get file pos during recovery", ErrStorageUnavailable)
		}

		remaining := fileSize - curPos
		if remaining == 0 {
			break
		}

		// Check for partial header at EOF (truncated write)
		if remaining < headerSize {
			if err := f.walFile.Truncate(validFileEnd); err != nil {
				return fmt.Errorf("%w: failed to truncate partial header at EOF", ErrStorageUnavailable)
			}
			break
		}

		header := make([]byte, headerSize)
		if _, err := io.ReadFull(f.walFile, header); err != nil {
			if err := f.walFile.Truncate(validFileEnd); err != nil {
				return fmt.Errorf("%w: failed to truncate after header read error", ErrStorageUnavailable)
			}
			break
		}

		// Validate magic bytes ('W', 'S') and wire version (0x01) sequentially
		if header[0] != magicByte1 || header[1] != magicByte2 || header[2] != wireVersion {
			if err := f.walFile.Truncate(validFileEnd); err != nil {
				return fmt.Errorf("%w: failed to truncate corrupt record header at EOF", ErrStorageUnavailable)
			}
			break
		}

		topicLen := int(binary.BigEndian.Uint16(header[23:25]))
		payloadLen := int(binary.BigEndian.Uint32(header[25:29]))
		totalRecordLen := headerSize + topicLen + payloadLen

		// Reject before allocating for it — see maxRecoverableRecordSize.
		if totalRecordLen > maxRecoverableRecordSize {
			if err := f.walFile.Truncate(validFileEnd); err != nil {
				return fmt.Errorf("%w: failed to truncate oversized corrupt record header", ErrStorageUnavailable)
			}
			break
		}

		// Validate bounds: check real remaining bytes on disk before advancing (POSIX lseek countermeasure)
		if int64(totalRecordLen) > remaining {
			// Torn write: body is truncated at EOF
			if err := f.walFile.Truncate(validFileEnd); err != nil {
				return fmt.Errorf("%w: failed to truncate partial record body at EOF", ErrStorageUnavailable)
			}
			break
		}

		// Read full body with io.ReadFull
		bodyBuf := make([]byte, topicLen+payloadLen)
		if _, err := io.ReadFull(f.walFile, bodyBuf); err != nil {
			if err := f.walFile.Truncate(validFileEnd); err != nil {
				return fmt.Errorf("%w: failed to truncate after body read error", ErrStorageUnavailable)
			}
			break
		}

		// Recalculate CRC32 IEEE over metadata and payload (offset 7 to end)
		fullData := make([]byte, totalRecordLen)
		copy(fullData[:headerSize], header)
		copy(fullData[headerSize:], bodyBuf)

		_ = crc32.ChecksumIEEE(fullData[7:])

		f.records = append(f.records, offsetPos{
			Offset: offsetCounter,
			Pos:    curPos,
			Length: totalRecordLen,
		})
		offsetCounter++
		validFileEnd = curPos + int64(totalRecordLen)
	}

	// If there were any dangling bytes past the last valid record, truncate them
	if fileSize > validFileEnd {
		if err := f.walFile.Truncate(validFileEnd); err != nil {
			return fmt.Errorf("%w: failed to truncate trailing data", ErrStorageUnavailable)
		}
	}

	if _, err := f.walFile.Seek(validFileEnd, io.SeekStart); err != nil {
		return fmt.Errorf("%w: failed to seek to end of wal", ErrStorageUnavailable)
	}
	f.writePos = validFileEnd

	return nil
}

// rollbackTo undoes a failed append so the WAL never grows with zero padding: it truncates to
// min(pos, real on-disk size) — Truncate cannot extend past what is physically flushed — and drops
// trailing index entries and any earlier appends still unsynced under SyncInterval. Caller holds mu.
func (f *FileStorageEngine) rollbackTo(pos int64) {
	target := pos
	if fi, err := f.walFile.Stat(); err == nil && fi.Size() < target {
		target = fi.Size()
	}
	f.writer.Reset(f.walFile)
	_ = f.walFile.Truncate(target)
	_, _ = f.walFile.Seek(target, io.SeekStart)
	f.writePos = target
	keep := len(f.records)
	for keep > 0 && f.records[keep-1].Pos+int64(f.records[keep-1].Length) > target {
		keep--
	}
	f.records = f.records[:keep]
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

	pos := f.writePos

	if _, err := f.writer.Write(data); err != nil {
		f.rollbackTo(pos)
		return 0, fmt.Errorf("%w: write failed", ErrStorageUnavailable)
	}

	if f.opts.SyncPolicy == SyncEveryRecord {
		if err := f.writer.Flush(); err != nil {
			f.rollbackTo(pos)
			return 0, fmt.Errorf("%w: flush failed", ErrStorageUnavailable)
		}
		if err := f.walFile.Sync(); err != nil {
			f.rollbackTo(pos)
			return 0, fmt.Errorf("%w: sync failed", ErrStorageUnavailable)
		}
	}

	f.writePos += int64(len(data))
	f.records = append(f.records, offsetPos{
		Offset: newOffset,
		Pos:    pos,
		Length: len(data),
	})

	return newOffset, nil
}

func (f *FileStorageEngine) ReadBatch(maxCount int) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, ErrStorageUnavailable
	}

	// Flush any in-memory buffered records to the OS page cache before reading
	if f.writer != nil && f.writer.Buffered() > 0 {
		if err := f.writer.Flush(); err != nil {
			return nil, fmt.Errorf("%w: flush before read failed", ErrStorageUnavailable)
		}
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

	if f.opts.SyncPolicy == SyncBatchCommit {
		if f.writer != nil && f.writer.Buffered() > 0 {
			if err := f.writer.Flush(); err != nil {
				return fmt.Errorf("%w: failed to flush wal buffer during batch commit", ErrStorageUnavailable)
			}
		}
		if err := f.walFile.Sync(); err != nil {
			return fmt.Errorf("%w: failed to sync wal file during batch commit", ErrStorageUnavailable)
		}
	}

	// Atomically write checkpoint via temporary file swap with strict durability.
	tmpPath := filepath.Join(f.dir, "checkpoint.tmp")
	chkPath := filepath.Join(f.dir, "checkpoint.meta")

	chkBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(chkBytes, uint64(newCheckpoint))

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("%w: failed to create checkpoint tmp", ErrStorageUnavailable)
	}

	if _, err := tmpFile.Write(chkBytes); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("%w: failed to write checkpoint tmp", ErrStorageUnavailable)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("%w: failed to sync checkpoint tmp", ErrStorageUnavailable)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("%w: failed to close checkpoint tmp", ErrStorageUnavailable)
	}

	if err := os.Rename(tmpPath, chkPath); err != nil {
		return fmt.Errorf("%w: failed to commit checkpoint", ErrStorageUnavailable)
	}

	// Fsync directory entry to ensure the rename metadata is durable across power failure.
	if dirFile, err := os.Open(f.dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
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

// Close flushes buffers, stops the background flusher, and closes the file descriptor.
// sync.Once makes it idempotent AND synchronizing: a concurrent second Close blocks inside Do
// until the first has fully drained flusherWg and closed the fd, then returns the same error.
func (f *FileStorageEngine) Close() error {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		stop := f.flusherStop
		f.mu.Unlock()

		// Drain the flusher outside mu: periodicFlusher's Flush() needs the lock to finish.
		if stop != nil {
			close(stop)
			f.flusherWg.Wait()
		}

		f.mu.Lock()
		var flushErr error
		if f.writer != nil {
			flushErr = f.writer.Flush()
		}
		syncErr := f.walFile.Sync()
		closeErr := f.walFile.Close()
		f.mu.Unlock()

		switch {
		case flushErr != nil:
			f.closeErr = fmt.Errorf("%w: buffer flush failed during close", ErrStorageUnavailable)
		case syncErr != nil:
			f.closeErr = fmt.Errorf("%w: wal sync failed during close", ErrStorageUnavailable)
		case closeErr != nil:
			f.closeErr = fmt.Errorf("%w: file close failed", ErrStorageUnavailable)
		}
	})
	return f.closeErr
}
