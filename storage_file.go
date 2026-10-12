package walspool

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
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

	// MaxSegmentSize specifies the maximum size in bytes of a single WAL segment file before rotating.
	// When <= 0, segment rotation is disabled and all records are appended to active.wal (default: 0).
	MaxSegmentSize int64
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
	Offset  Offset
	SegBase Offset // base offset of the segment file containing this record
	Pos     int64
	Length  int
}

// FileStorageEngine is an append-only, crash-resilient disk storage engine.
// Writes are buffered in memory and flushed to a .wal log file according to the configured SyncPolicy;
// checkpoints are committed atomically with directory fsync for maximum crash resilience.
type FileStorageEngine struct {
	mu                sync.RWMutex
	dir               string
	walFile           *os.File
	writer            *bufio.Writer
	writePos          int64
	checkpoint        Offset
	activeSegBase     Offset      // base offset of currently active write segment
	baseOffset        Offset      // logical offset corresponding to records[0] (for memory reclamation)
	isLegacyActiveWAL bool        // true if legacy active.wal was discovered
	records           []offsetPos // in-memory index of [offset -> file byte position]
	maxID             uint64      // highest recovered or persisted record ID
	maxCapacity       int
	opts              FileStorageOptions
	closed            bool

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

	var walFile *os.File
	var err error
	if opts.MaxSegmentSize <= 0 {
		walPath := filepath.Join(dir, "active.wal")
		walFile, err = os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to open wal file", ErrStorageUnavailable)
		}
	}

	engine := &FileStorageEngine{
		dir:         dir,
		walFile:     walFile,
		maxCapacity: maxCapacity,
		records:     make([]offsetPos, 0, 1024),
		opts:        opts,
	}

	if err := engine.recover(); err != nil {
		if engine.walFile != nil {
			_ = engine.walFile.Close()
		}
		return nil, err
	}

	engine.writer = bufio.NewWriterSize(engine.walFile, opts.BufferSize)

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

	if f.opts.MaxSegmentSize <= 0 {
		return f.recoverSingle()
	}
	return f.recoverSegmented()
}

func (f *FileStorageEngine) recoverSingle() error {
	stat, err := f.walFile.Stat()
	if err != nil {
		return fmt.Errorf("%w: failed to stat wal file during recovery", ErrStorageUnavailable)
	}
	fileSize := stat.Size()

	validFileEnd, err := f.recoverSegment(f.walFile, fileSize, 0)
	if err != nil {
		return err
	}

	totalRecovered := Offset(len(f.records))
	if f.checkpoint > totalRecovered {
		slog.Warn("walspool: checkpoint ahead of recovered WAL, clamping", "checkpoint", uint64(f.checkpoint), "recovered", uint64(totalRecovered))
		f.checkpoint = totalRecovered
	}

	if _, err := f.walFile.Seek(validFileEnd, io.SeekStart); err != nil {
		return fmt.Errorf("%w: failed to seek to end of wal", ErrStorageUnavailable)
	}
	f.writePos = validFileEnd

	return nil
}

func (f *FileStorageEngine) recoverSegmented() error {
	type segFileInfo struct {
		baseOffset Offset
		path       string
	}
	var segFiles []segFileInfo

	matches, _ := filepath.Glob(filepath.Join(f.dir, "segment-*.wal"))
	if len(matches) > 0 {
		for _, m := range matches {
			base := filepath.Base(m)
			var baseOff uint64
			if n, _ := fmt.Sscanf(base, "segment-%012d.wal", &baseOff); n == 1 {
				segFiles = append(segFiles, segFileInfo{baseOffset: Offset(baseOff), path: m})
			}
		}
		sort.Slice(segFiles, func(i, j int) bool {
			return segFiles[i].baseOffset < segFiles[j].baseOffset
		})
	}

	if len(segFiles) == 0 {
		activePath := filepath.Join(f.dir, "active.wal")
		if _, err := os.Stat(activePath); err == nil {
			f.isLegacyActiveWAL = true
			segFiles = append(segFiles, segFileInfo{baseOffset: 0, path: activePath})
		} else {
			initPath := f.segmentPath(0)
			segFiles = append(segFiles, segFileInfo{baseOffset: 0, path: initPath})
		}
	}

	if len(segFiles) > 0 {
		f.baseOffset = segFiles[0].baseOffset
	}

	for i, segInfo := range segFiles {
		isLast := (i == len(segFiles)-1)

		file, err := os.OpenFile(segInfo.path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("%w: failed to open wal segment %s", ErrStorageUnavailable, segInfo.path)
		}

		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("%w: failed to stat wal segment %s", ErrStorageUnavailable, segInfo.path)
		}
		fileSize := stat.Size()

		validFileEnd, err := f.recoverSegment(file, fileSize, segInfo.baseOffset)
		if err != nil {
			_ = file.Close()
			return err
		}

		if isLast {
			f.walFile = file
			f.activeSegBase = segInfo.baseOffset
			f.writePos = validFileEnd
			if _, err := f.walFile.Seek(validFileEnd, io.SeekStart); err != nil {
				return fmt.Errorf("%w: failed to seek to end of wal", ErrStorageUnavailable)
			}
		} else {
			_ = file.Close()
		}
	}

	totalRecovered := f.baseOffset + Offset(len(f.records))
	if f.checkpoint > totalRecovered {
		slog.Warn("walspool: checkpoint ahead of recovered WAL, clamping", "checkpoint", uint64(f.checkpoint), "recovered", uint64(totalRecovered))
		f.checkpoint = totalRecovered
	}

	if f.opts.MaxSegmentSize > 0 && len(f.records) > 0 {
		f.pruneCommittedSegments()
	}

	return nil
}

func (f *FileStorageEngine) recoverSegment(file *os.File, fileSize int64, segBase Offset) (int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("%w: seek error during recovery", ErrStorageUnavailable)
	}

	validFileEnd := int64(0)
	offsetCounter := segBase

	for {
		curPos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("%w: failed to get file pos during recovery", ErrStorageUnavailable)
		}

		remaining := fileSize - curPos
		if remaining == 0 {
			break
		}

		// Check for partial header at EOF (truncated write)
		if remaining < headerSize {
			if err := file.Truncate(validFileEnd); err != nil {
				return 0, fmt.Errorf("%w: failed to truncate partial header at EOF", ErrStorageUnavailable)
			}
			break
		}

		header := make([]byte, headerSize)
		if _, err := io.ReadFull(file, header); err != nil {
			if err := file.Truncate(validFileEnd); err != nil {
				return 0, fmt.Errorf("%w: failed to truncate after header read error", ErrStorageUnavailable)
			}
			break
		}

		// Validate magic bytes ('W', 'S') and wire version (0x01) sequentially
		if header[0] != magicByte1 || header[1] != magicByte2 || header[2] != wireVersion {
			if err := file.Truncate(validFileEnd); err != nil {
				return 0, fmt.Errorf("%w: failed to truncate corrupt record header at EOF", ErrStorageUnavailable)
			}
			break
		}

		topicLen := int(binary.BigEndian.Uint16(header[23:25]))
		payloadLen := int(binary.BigEndian.Uint32(header[25:29]))
		totalRecordLen := headerSize + topicLen + payloadLen

		// Reject before allocating for it — see maxRecoverableRecordSize.
		if totalRecordLen > maxRecoverableRecordSize {
			if err := file.Truncate(validFileEnd); err != nil {
				return 0, fmt.Errorf("%w: failed to truncate oversized corrupt record header", ErrStorageUnavailable)
			}
			break
		}

		// Validate bounds: check real remaining bytes on disk before advancing (POSIX lseek countermeasure)
		if int64(totalRecordLen) > remaining {
			// Torn write: body is truncated at EOF
			if err := file.Truncate(validFileEnd); err != nil {
				return 0, fmt.Errorf("%w: failed to truncate partial record body at EOF", ErrStorageUnavailable)
			}
			break
		}

		// Read full body with io.ReadFull
		bodyBuf := make([]byte, topicLen+payloadLen)
		if _, err := io.ReadFull(file, bodyBuf); err != nil {
			if err := file.Truncate(validFileEnd); err != nil {
				return 0, fmt.Errorf("%w: failed to truncate after body read error", ErrStorageUnavailable)
			}
			break
		}

		recID := binary.BigEndian.Uint64(header[7:15])
		if recID > f.maxID {
			f.maxID = recID
		}

		f.records = append(f.records, offsetPos{
			Offset:  offsetCounter,
			SegBase: segBase,
			Pos:     curPos,
			Length:  totalRecordLen,
		})
		offsetCounter++
		validFileEnd = curPos + int64(totalRecordLen)
	}

	// If there were any dangling bytes past the last valid record, truncate them
	if fileSize > validFileEnd {
		if err := file.Truncate(validFileEnd); err != nil {
			return 0, fmt.Errorf("%w: failed to truncate trailing data", ErrStorageUnavailable)
		}
	}

	return validFileEnd, nil
}

func (f *FileStorageEngine) segmentPath(baseOffset Offset) string {
	if (f.opts.MaxSegmentSize <= 0 || f.isLegacyActiveWAL) && baseOffset == 0 {
		return filepath.Join(f.dir, "active.wal")
	}
	return filepath.Join(f.dir, fmt.Sprintf("segment-%012d.wal", baseOffset))
}

func (f *FileStorageEngine) readEntryAt(entry offsetPos, buf []byte) error {
	if entry.SegBase == f.activeSegBase {
		_, err := f.walFile.ReadAt(buf, entry.Pos)
		return err
	}
	path := f.segmentPath(entry.SegBase)
	segFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer segFile.Close()
	_, err = segFile.ReadAt(buf, entry.Pos)
	return err
}

func (f *FileStorageEngine) rotateSegment(nextOffset Offset) error {
	if f.writer != nil && f.writer.Buffered() > 0 {
		if err := f.writer.Flush(); err != nil {
			return fmt.Errorf("%w: failed to flush segment before rotation", ErrStorageUnavailable)
		}
	}
	if err := f.walFile.Sync(); err != nil {
		return fmt.Errorf("%w: failed to sync segment before rotation", ErrStorageUnavailable)
	}
	if err := f.walFile.Close(); err != nil {
		return fmt.Errorf("%w: failed to close segment before rotation", ErrStorageUnavailable)
	}

	f.activeSegBase = nextOffset
	newPath := f.segmentPath(nextOffset)
	newFile, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("%w: failed to open new segment file", ErrStorageUnavailable)
	}

	f.walFile = newFile
	f.writer = bufio.NewWriterSize(newFile, f.opts.BufferSize)
	f.writePos = 0
	return nil
}

func (f *FileStorageEngine) pruneCommittedSegments() {
	prunedAny := false
	for len(f.records) > 0 {
		firstSegBase := f.records[0].SegBase
		if firstSegBase == f.activeSegBase {
			break
		}

		segEndIdx := 0
		for segEndIdx < len(f.records) && f.records[segEndIdx].SegBase == firstSegBase {
			segEndIdx++
		}

		lastRecordInSeg := f.records[segEndIdx-1]
		if lastRecordInSeg.Offset < f.checkpoint {
			path := f.segmentPath(firstSegBase)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				slog.Warn("walspool: failed to remove pruned segment file", "segment", path, "error", err)
			}
			f.baseOffset += Offset(segEndIdx)
			f.records = f.records[segEndIdx:]
			prunedAny = true
		} else {
			break
		}
	}

	if prunedAny {
		if dirFile, err := os.Open(f.dir); err == nil {
			_ = dirFile.Sync()
			_ = dirFile.Close()
		}
	}
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
	if err := f.walFile.Truncate(target); err != nil {
		slog.Error("walspool: rollback truncate failed", "target", target, "error", err)
	}
	if _, err := f.walFile.Seek(target, io.SeekStart); err != nil {
		slog.Error("walspool: rollback seek failed", "target", target, "error", err)
	}
	f.writePos = target
	keep := len(f.records)
	for keep > 0 && f.records[keep-1].SegBase == f.activeSegBase && f.records[keep-1].Pos+int64(f.records[keep-1].Length) > target {
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

	uncommitted := int((f.baseOffset + Offset(len(f.records))) - f.checkpoint)
	if uncommitted >= f.maxCapacity {
		return 0, ErrSpoolFull
	}

	newOffset := f.baseOffset + Offset(len(f.records))
	rec.Offset = newOffset

	data, err := rec.MarshalBinary()
	if err != nil {
		return 0, err
	}

	if f.opts.MaxSegmentSize > 0 && f.writePos > 0 && f.writePos+int64(len(data)) > f.opts.MaxSegmentSize {
		if err := f.rotateSegment(newOffset); err != nil {
			return 0, err
		}
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

	if rec.ID > f.maxID {
		f.maxID = rec.ID
	}

	f.writePos += int64(len(data))
	f.records = append(f.records, offsetPos{
		Offset:  newOffset,
		SegBase: f.activeSegBase,
		Pos:     pos,
		Length:  len(data),
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

	startIdx := int(f.checkpoint - f.baseOffset)
	if startIdx < 0 {
		startIdx = 0
	}
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

		if err := f.readEntryAt(entry, buf); err != nil {
			return nil, fmt.Errorf("%w: read error at offset %d", ErrStorageUnavailable, entry.Offset)
		}

		var rec Record
		if err := rec.UnmarshalBinary(buf); err != nil {
			// Hand back the valid prefix first; the next call then surfaces the corrupt record alone.
			if len(batch) > 0 {
				return batch, nil
			}
			return nil, &CorruptRecordError{Offset: entry.Offset, Err: err}
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

	if f.opts.MaxSegmentSize > 0 && len(f.records) > 0 {
		f.pruneCommittedSegments()
	}

	return nil
}

func (f *FileStorageEngine) UncommittedCount() (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return 0, ErrStorageUnavailable
	}

	total := f.baseOffset + Offset(len(f.records))
	count := int(total - f.checkpoint)
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

		var errs []error
		if flushErr != nil {
			errs = append(errs, fmt.Errorf("%w: buffer flush failed during close", ErrStorageUnavailable))
		}
		if syncErr != nil {
			errs = append(errs, fmt.Errorf("%w: wal sync failed during close", ErrStorageUnavailable))
		}
		if closeErr != nil {
			errs = append(errs, fmt.Errorf("%w: file close failed", ErrStorageUnavailable))
		}
		f.closeErr = errors.Join(errs...)
	})
	return f.closeErr
}

// LastID returns the highest record ID persisted or recovered by the storage engine.
func (f *FileStorageEngine) LastID() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.maxID
}
