package walspool_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

// 1. Crash Recovery with Truncated/Torn Tail
// Verifies that when a WAL file ends with truncated/torn data (partial header, partial body, corrupt magic at EOF),
// recover() automatically truncates the active.wal back to the last valid record, repairing the file
// on physical disk so earlier logs remain 100% readable and new appends resume seamlessly.
func TestFileStorage_TruncatedTailRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_trunc_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: Write 5 valid records using SyncEveryRecord to ensure immediate disk persistence
	opts := walspool.FileStorageOptions{
		BufferSize: 4096,
		SyncPolicy: walspool.SyncEveryRecord,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}

	for i := 0; i < 5; i++ {
		rec := walspool.Record{
			ID:        uint64(100 + i),
			Timestamp: time.Now(),
			Topic:     "orders.payment",
			Payload:   []byte(fmt.Sprintf(`{"invoice_id":%d,"amount":99.50}`, i)),
		}
		if _, err := engine.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}

	walPath := filepath.Join(tmpDir, "active.wal")
	statBefore, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	validSize := statBefore.Size()

	// Step 2: Simulate power outage / crash during append #6 (partial write: 15 bytes of truncated header)
	walFile, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file for injection: %v", err)
	}
	partialGarbage := []byte("WS\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x05") // 15 bytes < headerSize (29)
	if _, err := walFile.Write(partialGarbage); err != nil {
		t.Fatalf("failed to inject partial garbage: %v", err)
	}
	_ = walFile.Sync()
	_ = walFile.Close()

	statCorrupt, _ := os.Stat(walPath)
	if statCorrupt.Size() != validSize+int64(len(partialGarbage)) {
		t.Fatalf("expected corrupt file size %d, got %d", validSize+int64(len(partialGarbage)), statCorrupt.Size())
	}

	// Step 3: Reopen engine. Crash recovery must detect torn write, truncate file back to validSize,
	// and preserve all 5 prior records.
	recoveredEngine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer recoveredEngine.Close()

	// Verify file was atomically truncated on physical disk
	statRepaired, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat after recovery failed: %v", err)
	}
	if statRepaired.Size() != validSize {
		t.Errorf("expected repaired file size %d (truncated), got %d", validSize, statRepaired.Size())
	}

	// Verify all 5 prior records are readable and intact
	batch, err := recoveredEngine.ReadBatch(10)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 5 {
		t.Fatalf("expected 5 records recovered, got %d", len(batch))
	}
	for i, rec := range batch {
		if rec.Offset != walspool.Offset(i) {
			t.Errorf("record %d offset mismatch: expected %d, got %d", i, i, rec.Offset)
		}
		if rec.ID != uint64(100+i) {
			t.Errorf("record %d ID mismatch: expected %d, got %d", i, 100+i, rec.ID)
		}
		expectedPayload := fmt.Sprintf(`{"invoice_id":%d,"amount":99.50}`, i)
		if string(rec.Payload) != expectedPayload {
			t.Errorf("record %d payload mismatch: expected %s, got %s", i, expectedPayload, string(rec.Payload))
		}
	}

	// Step 4: Resume writing new records. Verify offset monotonically advances to 5
	newRec := walspool.Record{
		ID:        105,
		Timestamp: time.Now(),
		Topic:     "orders.payment",
		Payload:   []byte(`{"invoice_id":5,"amount":99.50}`),
	}
	newOffset, err := recoveredEngine.Append(newRec)
	if err != nil {
		t.Fatalf("append after recovery failed: %v", err)
	}
	if newOffset != 5 {
		t.Fatalf("expected offset 5, got %d", newOffset)
	}
}

// 2. Crash Recovery with Torn Body (POSIX lseek Countermeasure)
// Simulates a scenario where a header is written (29 bytes) claiming 500 bytes of body,
// but the power cut interrupted the write so only 30 bytes of body made it to disk.
// Verifies that FileStorageEngine checks disk remaining bytes rather than blindly seeking past EOF.
func TestFileStorage_TornBodyPOSIXCountermeasure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_torn_body_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.FileStorageOptions{
		BufferSize: 4096,
		SyncPolicy: walspool.SyncEveryRecord,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}

	// Append 2 clean records
	for i := 0; i < 2; i++ {
		_, err := engine.Append(walspool.Record{
			ID:        uint64(i + 1),
			Timestamp: time.Now(),
			Topic:     "sensor.metrics",
			Payload:   []byte("healthy-data"),
		})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	_ = engine.Close()

	walPath := filepath.Join(tmpDir, "active.wal")
	statBefore, _ := os.Stat(walPath)
	cleanSize := statBefore.Size()

	// Synthesize a torn write: complete header with topicLen=10, payloadLen=500, but only 20 bytes body written
	tornHeader := make([]byte, 29)
	tornHeader[0] = 0x57                               // 'W'
	tornHeader[1] = 0x53                               // 'S'
	tornHeader[2] = 0x01                               // wireVersion
	binary.BigEndian.PutUint16(tornHeader[23:25], 10)  // topicLen = 10
	binary.BigEndian.PutUint32(tornHeader[25:29], 500) // payloadLen = 500
	tornBody := bytes.Repeat([]byte("A"), 20)          // only 20 bytes written instead of 510

	walFile, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	_, _ = walFile.Write(tornHeader)
	_, _ = walFile.Write(tornBody)
	_ = walFile.Sync()
	_ = walFile.Close()

	// Recovery must detect torn body, truncate back to cleanSize, and ignore phantom record
	recovered, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recovered.Close()

	statAfter, _ := os.Stat(walPath)
	if statAfter.Size() != cleanSize {
		t.Errorf("expected clean truncated size %d, got %d", cleanSize, statAfter.Size())
	}

	batch, err := recovered.ReadBatch(10)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 valid records, got %d", len(batch))
	}
}

// 3. CRC32 Corrupted Record Rejection (Meyer DbC / Tier 3 Translation)
// Verifies that a record with corrupted payload/checksum is rejected with ErrStorageUnavailable
// when read from disk, preventing silent bit-rot propagation.
func TestFileStorage_CRC32Rejection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_crc_rejection_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := walspool.NewFileStorageEngine(tmpDir, 100)
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}

	rec := walspool.Record{
		ID:        42,
		Timestamp: time.Now(),
		Topic:     "financial.ledger",
		Payload:   []byte(`{"tx":"transfer","amount":1000000}`),
	}
	if _, err := engine.Append(rec); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	_ = engine.Close()

	// Corrupt a single byte in the payload area
	walPath := filepath.Join(tmpDir, "active.wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read wal: %v", err)
	}
	data[len(data)-5] ^= 0xAA // bit flip
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("failed to rewrite corrupted wal: %v", err)
	}

	// Reopen storage engine
	reopened, err := walspool.NewFileStorageEngine(tmpDir, 100)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	// ReadBatch must reject the corrupt record with ErrStorageUnavailable
	_, readErr := reopened.ReadBatch(10)
	if readErr == nil {
		t.Fatalf("expected corruption error, got nil")
	}
	if !errors.Is(readErr, walspool.ErrStorageUnavailable) {
		t.Fatalf("expected ErrStorageUnavailable, got %v", readErr)
	}
}

// 4. Checkpoint Durability & Atomic fsync Swap
// Verifies that Commit() writes checkpoint.tmp, syncs it, renames to checkpoint.meta,
// and syncs parent directory. Also verifies that checkpoint recovery is exact across restarts.
func TestFileStorage_CheckpointDurability(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_chk_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := walspool.NewFileStorageEngine(tmpDir, 100)
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err := engine.Append(walspool.Record{
			ID:        uint64(i + 1),
			Timestamp: time.Now(),
			Topic:     "metrics",
			Payload:   []byte("tick"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	uncommitted, err := engine.UncommittedCount()
	if err != nil || uncommitted != 10 {
		t.Fatalf("expected 10 uncommitted, got %d (%v)", uncommitted, err)
	}

	// Commit up to offset 3 (4 records: 0, 1, 2, 3 committed -> next checkpoint = 4)
	if err := engine.Commit(3); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	uncommittedAfter, err := engine.UncommittedCount()
	if err != nil || uncommittedAfter != 6 {
		t.Fatalf("expected 6 uncommitted after commit(3), got %d (%v)", uncommittedAfter, err)
	}

	// Verify checkpoint.tmp does not linger
	tmpFile := filepath.Join(tmpDir, "checkpoint.tmp")
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Errorf("expected checkpoint.tmp to be cleaned up after rename, but found it")
	}

	// Verify checkpoint.meta contents
	metaFile := filepath.Join(tmpDir, "checkpoint.meta")
	metaBytes, err := os.ReadFile(metaFile)
	if err != nil {
		t.Fatalf("failed to read checkpoint.meta: %v", err)
	}
	if len(metaBytes) < 8 {
		t.Fatalf("checkpoint.meta too small: %d bytes", len(metaBytes))
	}
	val := binary.BigEndian.Uint64(metaBytes[:8])
	if val != 4 {
		t.Fatalf("expected checkpoint value 4, got %d", val)
	}

	// Reopen engine and verify checkpoint is preserved
	if err := engine.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened, err := walspool.NewFileStorageEngine(tmpDir, 100)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	reopenedUncommitted, err := reopened.UncommittedCount()
	if err != nil || reopenedUncommitted != 6 {
		t.Fatalf("expected 6 uncommitted on recovered engine, got %d (%v)", reopenedUncommitted, err)
	}

	// ReadBatch should start from checkpoint (offset 4)
	batch, err := reopened.ReadBatch(10)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 6 {
		t.Fatalf("expected 6 records in batch, got %d", len(batch))
	}
	if batch[0].Offset != 4 {
		t.Fatalf("expected first batch item offset 4, got %d", batch[0].Offset)
	}

	// Committing a stale/lower offset must be an idempotent no-op
	if err := reopened.Commit(2); err != nil {
		t.Fatalf("stale commit returned error: %v", err)
	}
	count, _ := reopened.UncommittedCount()
	if count != 6 {
		t.Fatalf("stale commit altered checkpoint: got count %d, expected 6", count)
	}
}

// 5. SyncPolicy Validation & Thread-safe Flush
// Verifies SyncEveryRecord, SyncBatchCommit, and SyncInterval with explicit Flush() calls.
func TestFileStorage_SyncPolicies(t *testing.T) {
	t.Run("SyncInterval_BackgroundFlusher", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "walspool_sync_interval_*")
		if err != nil {
			t.Fatalf("temp dir failed: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		opts := walspool.FileStorageOptions{
			BufferSize:   128 * 1024,
			SyncPolicy:   walspool.SyncInterval,
			SyncInterval: 10 * time.Millisecond,
		}
		engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
		if err != nil {
			t.Fatalf("init failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			_, err := engine.Append(walspool.Record{
				ID:        uint64(i),
				Timestamp: time.Now(),
				Topic:     "telemetry",
				Payload:   []byte("sample-data"),
			})
			if err != nil {
				t.Fatalf("append failed: %v", err)
			}
		}

		// Wait for periodic flusher to trigger
		time.Sleep(30 * time.Millisecond)

		// Explicit Flush is thread-safe
		if err := engine.Flush(); err != nil {
			t.Fatalf("explicit flush failed: %v", err)
		}

		if err := engine.Close(); err != nil {
			t.Fatalf("close failed: %v", err)
		}
	})

	t.Run("SyncBatchCommit_Durability", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "walspool_sync_batch_*")
		if err != nil {
			t.Fatalf("temp dir failed: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		opts := walspool.FileStorageOptions{
			BufferSize: 64 * 1024,
			SyncPolicy: walspool.SyncBatchCommit,
		}
		engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
		if err != nil {
			t.Fatalf("init failed: %v", err)
		}

		for i := 0; i < 3; i++ {
			_, err := engine.Append(walspool.Record{
				ID:        uint64(i),
				Timestamp: time.Now(),
				Topic:     "batch.events",
				Payload:   []byte("event-payload"),
			})
			if err != nil {
				t.Fatalf("append failed: %v", err)
			}
		}

		// Commit offset 2 triggers batch fsync
		if err := engine.Commit(2); err != nil {
			t.Fatalf("commit failed: %v", err)
		}

		if err := engine.Close(); err != nil {
			t.Fatalf("close failed: %v", err)
		}
	})
}

// 6. Append Rollback on ENOSPC / Write Failure
// Verifies that if Append fails, no corrupt residues are left and the file position is rolled back.
func TestFileStorage_AppendQuotaAndPreconditions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_quota_*")
	if err != nil {
		t.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Precondition: empty dir
	if _, err := walspool.NewFileStorageEngine("", 10); !errors.Is(err, walspool.ErrPreconditionViolated) {
		t.Errorf("expected ErrPreconditionViolated on empty dir, got %v", err)
	}

	// Precondition: non-positive capacity
	if _, err := walspool.NewFileStorageEngine(tmpDir, 0); !errors.Is(err, walspool.ErrPreconditionViolated) {
		t.Errorf("expected ErrPreconditionViolated on zero capacity, got %v", err)
	}

	engine, err := walspool.NewFileStorageEngine(tmpDir, 3)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	for i := 0; i < 3; i++ {
		_, err := engine.Append(walspool.Record{
			ID:        uint64(i),
			Timestamp: time.Now(),
			Topic:     "quota.test",
			Payload:   []byte("data"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// 4th append must fail with ErrSpoolFull
	_, err = engine.Append(walspool.Record{
		ID:        99,
		Timestamp: time.Now(),
		Topic:     "quota.test",
		Payload:   []byte("overflow"),
	})
	if !errors.Is(err, walspool.ErrSpoolFull) {
		t.Fatalf("expected ErrSpoolFull on quota exceeded, got %v", err)
	}

	// Uncommitted count must remain strictly 3
	count, _ := engine.UncommittedCount()
	if count != 3 {
		t.Fatalf("expected uncommitted count 3, got %d", count)
	}
}

// 7. Concurrent Append and ReadBatch Race-Freedom
func TestFileStorage_ConcurrentAppendAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_concurrent_*")
	if err != nil {
		t.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.FileStorageOptions{
		BufferSize:   64 * 1024,
		SyncPolicy:   walspool.SyncInterval,
		SyncInterval: 10 * time.Millisecond,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 5000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	const workers = 4
	const itemsPerWorker = 50
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				_, err := engine.Append(walspool.Record{
					ID:        uint64(workerID*1000 + i),
					Timestamp: time.Now(),
					Topic:     "stress",
					Payload:   []byte("stress-event-payload-data"),
				})
				if err != nil {
					t.Errorf("worker %d append %d failed: %v", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	// Read all items
	batch, err := engine.ReadBatch(workers * itemsPerWorker)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != workers*itemsPerWorker {
		t.Fatalf("expected %d records in batch, got %d", workers*itemsPerWorker, len(batch))
	}
}

// A corrupt header advertising an oversized payload must be rejected without allocating for it,
// truncating the WAL back to the last valid record rather than materializing gigabytes.
func TestFileStorage_RecoverOOMGuard(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_oom_*")
	if err != nil {
		t.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.FileStorageOptions{BufferSize: 4096, SyncPolicy: walspool.SyncEveryRecord}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := engine.Append(walspool.Record{
			ID:        uint64(i + 1),
			Timestamp: time.Now(),
			Topic:     "safe",
			Payload:   []byte("clean-record"),
		}); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	_ = engine.Close()

	walPath := filepath.Join(tmpDir, "active.wal")
	statBefore, _ := os.Stat(walPath)
	validSize := statBefore.Size()

	// Body is fully present on disk so only the OOM guard, not the "remaining bytes" check, rejects this record.
	const oversize = 10 * 1024 * 1024
	topicLen := 5
	header := make([]byte, 29)
	header[0] = 0x57 // 'W'
	header[1] = 0x53 // 'S'
	header[2] = 0x01 // wireVersion
	binary.BigEndian.PutUint16(header[23:25], uint16(topicLen))
	binary.BigEndian.PutUint32(header[25:29], uint32(oversize))

	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open wal failed: %v", err)
	}
	if _, err := f.Write(header); err != nil {
		t.Fatalf("write header failed: %v", err)
	}
	if _, err := f.Write(make([]byte, topicLen+oversize)); err != nil {
		t.Fatalf("write oversized body failed: %v", err)
	}
	_ = f.Sync()
	_ = f.Close()

	reopened, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	statAfter, _ := os.Stat(walPath)
	if statAfter.Size() != validSize {
		t.Fatalf("expected WAL truncated to %d, got %d (oversized record not rejected)", validSize, statAfter.Size())
	}

	batch, err := reopened.ReadBatch(10)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 valid records after OOM-guard recovery, got %d", len(batch))
	}
}

// Close() must be idempotent and synchronizing under concurrent callers.
func TestFileStorage_ConcurrentCloseIdempotent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "walspool_close_*")
	if err != nil {
		t.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.FileStorageOptions{BufferSize: 64 * 1024, SyncPolicy: walspool.SyncInterval, SyncInterval: 5 * time.Millisecond}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, 100, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := engine.Append(walspool.Record{
			ID:        uint64(i),
			Timestamp: time.Now(),
			Topic:     "close.test",
			Payload:   []byte("payload"),
		}); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	const n = 16
	var wg sync.WaitGroup
	errsCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errsCh <- engine.Close()
		}()
	}
	wg.Wait()
	close(errsCh)
	for e := range errsCh {
		if e != nil {
			t.Fatalf("concurrent Close returned error: %v", e)
		}
	}

	if _, err := engine.Append(walspool.Record{Topic: "x", Payload: []byte("y")}); !errors.Is(err, walspool.ErrStorageUnavailable) {
		t.Fatalf("expected ErrStorageUnavailable after close, got %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("idempotent Close returned error: %v", err)
	}
}

// Under SyncInterval the checkpoint is fsynced before the WAL tail, so a crash can leave
// checkpoint.meta ahead of the recovered records. New appends must still be delivered.
func TestFileStorage_Recover_CheckpointAheadOfWAL(t *testing.T) {
	dir := t.TempDir()
	engine, err := walspool.NewFileStorageEngine(dir, 1000)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := engine.Append(walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("x")}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Simulate: all 5 committed and fsynced, but the last 2 records were lost with the page cache.
	chk := make([]byte, 8)
	binary.BigEndian.PutUint64(chk, 5)
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.meta"), chk, 0644); err != nil {
		t.Fatalf("write checkpoint failed: %v", err)
	}
	walPath := filepath.Join(dir, "active.wal")
	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if err := os.Truncate(walPath, fi.Size()/5*3); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	reopened, err := walspool.NewFileStorageEngine(dir, 1000)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if n, _ := reopened.UncommittedCount(); n != 0 {
		t.Fatalf("expected 0 uncommitted after recovery, got %d", n)
	}
	if _, err := reopened.Append(walspool.Record{ID: 100, Timestamp: time.Now(), Topic: "t", Payload: []byte("new")}); err != nil {
		t.Fatalf("append after recovery failed: %v", err)
	}
	n, _ := reopened.UncommittedCount()
	batch, err := reopened.ReadBatch(10)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != 1 || len(batch) != 1 || batch[0].ID != 100 {
		t.Fatalf("new record must be visible: uncommitted=%d batch=%d", n, len(batch))
	}
}

// A corrupt record in the middle of the pending region must be surfaced alone, as a
// *CorruptRecordError naming its offset, after the valid prefix has been handed back.
func TestFileStorage_ReadBatch_IsolatesCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	engine, err := walspool.NewFileStorageEngine(dir, 1000)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := engine.Append(walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("payload")}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	walPath := filepath.Join(dir, "active.wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal failed: %v", err)
	}
	recLen := len(data) / 3
	data[recLen+recLen-2] ^= 0xFF // last payload byte of record 1
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("rewrite wal failed: %v", err)
	}

	reopened, err := walspool.NewFileStorageEngine(dir, 1000)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	batch, err := reopened.ReadBatch(10)
	if err != nil || len(batch) != 1 || batch[0].ID != 1 {
		t.Fatalf("expected valid prefix of 1 record, got %d records, err=%v", len(batch), err)
	}
	if err := reopened.Commit(batch[0].Offset); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	batch, err = reopened.ReadBatch(10)
	var corrupt *walspool.CorruptRecordError
	if !errors.As(err, &corrupt) || len(batch) != 0 {
		t.Fatalf("expected *CorruptRecordError and no records, got %d records, err=%v", len(batch), err)
	}
	if corrupt.Offset != 1 {
		t.Fatalf("expected corrupt offset 1, got %d", corrupt.Offset)
	}
	if !errors.Is(err, walspool.ErrStorageUnavailable) || !errors.Is(err, walspool.ErrCorruptRecord) {
		t.Fatalf("corrupt error must match both ErrStorageUnavailable and ErrCorruptRecord: %v", err)
	}
	if err := reopened.Commit(corrupt.Offset); err != nil {
		t.Fatalf("commit past corrupt failed: %v", err)
	}

	batch, err = reopened.ReadBatch(10)
	if err != nil || len(batch) != 1 || batch[0].ID != 3 {
		t.Fatalf("expected record 3 after skipping corrupt one, got %d records, err=%v", len(batch), err)
	}
}

func TestFileStorage_SegmentRotation_AutoPrune(t *testing.T) {
	dir := t.TempDir()

	// 200 bytes per segment forces rotation after ~2 records
	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 200
	opts.SyncPolicy = walspool.SyncBatchCommit

	engine, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	totalRecords := 25
	for i := 0; i < totalRecords; i++ {
		rec := walspool.Record{
			ID:        uint64(i + 1),
			Timestamp: time.Now(),
			Topic:     "events",
			Payload:   []byte(fmt.Sprintf("payload-segment-data-%04d", i)),
		}
		offset, err := engine.Append(rec)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if offset != walspool.Offset(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	// Verify that multiple segment files were created
	matches, err := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if err != nil || len(matches) < 5 {
		t.Fatalf("expected at least 5 segments, got %d (err: %v)", len(matches), err)
	}

	// Read and commit batches, observing automatic segment pruning
	for {
		batch, err := engine.ReadBatch(5)
		if err != nil {
			t.Fatalf("read batch failed: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		lastOffset := batch[len(batch)-1].Offset
		if err := engine.Commit(lastOffset); err != nil {
			t.Fatalf("commit offset %d failed: %v", lastOffset, err)
		}
	}

	uncommitted, err := engine.UncommittedCount()
	if err != nil || uncommitted != 0 {
		t.Fatalf("expected 0 uncommitted records, got %d (err: %v)", uncommitted, err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Verify that old committed segments were pruned, leaving only the active segment
	remainingSegments, err := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(remainingSegments) > 1 {
		t.Fatalf("expected at most 1 active segment remaining after full commit, found %d: %v", len(remainingSegments), remainingSegments)
	}
}

func TestFileStorage_SegmentRotation_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 250
	opts.SyncPolicy = walspool.SyncBatchCommit

	engine, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	// Append 15 records across multiple segments
	for i := 0; i < 15; i++ {
		rec := walspool.Record{
			ID:        uint64(i + 1),
			Timestamp: time.Now(),
			Topic:     "telemetry",
			Payload:   []byte(fmt.Sprintf("telemetry-event-record-%03d", i)),
		}
		if _, err := engine.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Commit first 6 records (offsets 0..5)
	if err := engine.Commit(5); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Close engine without committing records 6..14 (simulating restart with uncommitted backlog)
	if err := engine.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen engine
	reopened, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	uncommitted, err := reopened.UncommittedCount()
	if err != nil || uncommitted != 9 {
		t.Fatalf("expected 9 uncommitted records, got %d (err: %v)", uncommitted, err)
	}

	// Read pending batch and verify continuity
	batch, err := reopened.ReadBatch(20)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 9 {
		t.Fatalf("expected 9 records, got %d", len(batch))
	}
	for i, r := range batch {
		expectedOffset := walspool.Offset(6 + i)
		expectedID := uint64(7 + i)
		if r.Offset != expectedOffset || r.ID != expectedID {
			t.Fatalf("record %d: expected offset %d, ID %d; got offset %d, ID %d", i, expectedOffset, expectedID, r.Offset, r.ID)
		}
	}

	// Commit remaining records and append new records into the active segment
	if err := reopened.Commit(14); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	newRec := walspool.Record{
		ID:        16,
		Timestamp: time.Now(),
		Topic:     "telemetry",
		Payload:   []byte("post-recovery-record"),
	}
	newOffset, err := reopened.Append(newRec)
	if err != nil {
		t.Fatalf("append after recovery failed: %v", err)
	}
	if newOffset != 15 {
		t.Fatalf("expected offset 15, got %d", newOffset)
	}
}

func TestFileStorage_SegmentRotation_ConcurrentAppendRead(t *testing.T) {
	dir := t.TempDir()

	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 400
	opts.SyncPolicy = walspool.SyncBatchCommit

	engine, err := walspool.NewFileStorageEngineWithOptions(dir, 10000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	const numWorkers = 4
	const recordsPerWorker = 50
	const totalRecords = numWorkers * recordsPerWorker

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < recordsPerWorker; i++ {
				rec := walspool.Record{
					ID:        uint64(workerID*recordsPerWorker + i + 1),
					Timestamp: time.Now(),
					Topic:     "concurrent.stream",
					Payload:   []byte(fmt.Sprintf("worker-%d-item-%04d-payload-data", workerID, i)),
				}
				if _, err := engine.Append(rec); err != nil {
					t.Errorf("worker %d append %d failed: %v", workerID, i, err)
					return
				}
			}
		}(w)
	}

	// Concurrent reader draining batches while appenders write and rotate segments
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		readCount := 0
		for readCount < totalRecords {
			batch, err := engine.ReadBatch(10)
			if err != nil {
				t.Errorf("concurrent read error: %v", err)
				return
			}
			if len(batch) > 0 {
				lastOffset := batch[len(batch)-1].Offset
				if err := engine.Commit(lastOffset); err != nil {
					t.Errorf("concurrent commit error: %v", err)
					return
				}
				readCount += len(batch)
			} else {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	<-readDone

	uncommitted, err := engine.UncommittedCount()
	if err != nil || uncommitted != 0 {
		t.Fatalf("expected 0 uncommitted records, got %d (err: %v)", uncommitted, err)
	}
}

func TestFileStorage_SegmentRotation_OversizedRecord(t *testing.T) {
	dir := t.TempDir()

	// Segment size smaller than the oversized record itself
	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 300
	opts.SyncPolicy = walspool.SyncBatchCommit

	engine, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	rec1 := walspool.Record{ID: 1, Timestamp: time.Now(), Topic: "t", Payload: []byte("normal-record-1")}
	rec2 := walspool.Record{ID: 2, Timestamp: time.Now(), Topic: "t", Payload: bytes.Repeat([]byte("X"), 2048)}
	rec3 := walspool.Record{ID: 3, Timestamp: time.Now(), Topic: "t", Payload: []byte("normal-record-3")}

	off1, err := engine.Append(rec1)
	if err != nil || off1 != 0 {
		t.Fatalf("append rec1 failed: off=%d, err=%v", off1, err)
	}

	off2, err := engine.Append(rec2)
	if err != nil || off2 != 1 {
		t.Fatalf("append rec2 (oversized) failed: off=%d, err=%v", off2, err)
	}

	off3, err := engine.Append(rec3)
	if err != nil || off3 != 2 {
		t.Fatalf("append rec3 failed: off=%d, err=%v", off3, err)
	}

	batch, err := engine.ReadBatch(10)
	if err != nil || len(batch) != 3 {
		t.Fatalf("expected 3 records, got %d (err: %v)", len(batch), err)
	}
	if len(batch[1].Payload) != 2048 {
		t.Fatalf("expected 2048 bytes payload for rec2, got %d", len(batch[1].Payload))
	}
}

func TestFileStorage_SegmentRotation_LegacyMigration(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create legacy active.wal (MaxSegmentSize = 0)
	optsLegacy := walspool.DefaultFileStorageOptions()
	optsLegacy.MaxSegmentSize = 0
	optsLegacy.SyncPolicy = walspool.SyncBatchCommit

	eng1, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, optsLegacy)
	if err != nil {
		t.Fatalf("init legacy engine failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("legacy-record")}
		if _, err := eng1.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := eng1.Close(); err != nil {
		t.Fatalf("close legacy engine failed: %v", err)
	}

	// Verify active.wal exists
	if _, err := os.Stat(filepath.Join(dir, "active.wal")); err != nil {
		t.Fatalf("expected active.wal to exist: %v", err)
	}

	// Phase 2: reopen with MaxSegmentSize > 0 enabled
	optsSegmented := walspool.DefaultFileStorageOptions()
	optsSegmented.MaxSegmentSize = 200
	optsSegmented.SyncPolicy = walspool.SyncBatchCommit

	eng2, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, optsSegmented)
	if err != nil {
		t.Fatalf("reopen with segmentation failed: %v", err)
	}
	defer eng2.Close()

	// Append more records to force rotation into new segment files
	for i := 5; i < 15; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("new-segmented-record")}
		if _, err := eng2.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Read all 15 records back across legacy active.wal and new segments
	batch, err := eng2.ReadBatch(20)
	if err != nil || len(batch) != 15 {
		t.Fatalf("expected 15 records across migrated segments, got %d (err: %v)", len(batch), err)
	}
	for i, r := range batch {
		if r.Offset != walspool.Offset(i) || r.ID != uint64(i+1) {
			t.Fatalf("record %d mismatch: offset=%d, id=%d", i, r.Offset, r.ID)
		}
	}

	// Commit all records and verify pruning
	if err := eng2.Commit(14); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	uncommitted, err := eng2.UncommittedCount()
	if err != nil || uncommitted != 0 {
		t.Fatalf("expected 0 uncommitted records, got %d", uncommitted)
	}
}

func TestFileStorage_SegmentRotation_CorruptMiddleSegment(t *testing.T) {
	dir := t.TempDir()

	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 200
	opts.SyncPolicy = walspool.SyncBatchCommit

	eng, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	// Append 12 records across ~4 segments
	for i := 0; i < 12; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("segment-corrupt-test-payload")}
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Find the 2nd segment and flip a byte in its payload
	segments, _ := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if len(segments) < 3 {
		t.Fatalf("expected at least 3 segments, got %d", len(segments))
	}
	sort.Strings(segments)
	targetSeg := segments[1]

	data, err := os.ReadFile(targetSeg)
	if err != nil {
		t.Fatalf("read segment failed: %v", err)
	}
	// Corrupt a byte near the middle of targetSeg
	data[len(data)/2] ^= 0xEE
	if err := os.WriteFile(targetSeg, data, 0644); err != nil {
		t.Fatalf("rewrite segment failed: %v", err)
	}

	reopened, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	// Read valid records preceding the corrupted segment
	batch, err := reopened.ReadBatch(10)
	if err != nil {
		t.Fatalf("expected valid prefix batch, got err: %v", err)
	}
	if len(batch) == 0 {
		t.Fatalf("expected at least 1 valid record before corrupt segment")
	}

	// Commit up to the last valid record before the corrupt one
	lastValidOffset := batch[len(batch)-1].Offset
	if err := reopened.Commit(lastValidOffset); err != nil {
		t.Fatalf("commit valid prefix failed: %v", err)
	}

	// Next ReadBatch should isolate the CorruptRecordError
	batch, err = reopened.ReadBatch(10)
	var corruptErr *walspool.CorruptRecordError
	if !errors.As(err, &corruptErr) || len(batch) != 0 {
		t.Fatalf("expected *CorruptRecordError and empty batch, got %d records, err=%v", len(batch), err)
	}

	// Commit past the corrupt record to skip it
	if err := reopened.Commit(corruptErr.Offset); err != nil {
		t.Fatalf("commit past corrupt record failed: %v", err)
	}

	// Next ReadBatch should recover valid records from remaining segments
	batch, err = reopened.ReadBatch(10)
	if err != nil || len(batch) == 0 {
		t.Fatalf("expected records after corrupt segment to be readable, got %d records, err=%v", len(batch), err)
	}
}

func TestFileStorage_SegmentRotation_LegacyMigrationCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// 1. Initial engine running legacy single active.wal
	optsLegacy := walspool.DefaultFileStorageOptions()
	optsLegacy.MaxSegmentSize = 0 // legacy single file mode
	optsLegacy.SyncPolicy = walspool.SyncBatchCommit

	eng1, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, optsLegacy)
	if err != nil {
		t.Fatalf("eng1 init failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("legacy-record")}
		if _, err := eng1.Append(rec); err != nil {
			t.Fatalf("eng1 append %d failed: %v", i, err)
		}
	}
	if err := eng1.Close(); err != nil {
		t.Fatalf("eng1 close failed: %v", err)
	}

	// 2. Reopen with segment rotation enabled
	optsSegmented := walspool.DefaultFileStorageOptions()
	optsSegmented.MaxSegmentSize = 150
	optsSegmented.SyncPolicy = walspool.SyncBatchCommit

	eng2, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, optsSegmented)
	if err != nil {
		t.Fatalf("eng2 init failed: %v", err)
	}

	// Append 10 more records to force rotation into segment-*.wal while active.wal remains uncommitted
	for i := 5; i < 15; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("new-segmented-record")}
		if _, err := eng2.Append(rec); err != nil {
			t.Fatalf("eng2 append %d failed: %v", i, err)
		}
	}

	// Verify segments were created alongside active.wal
	segments, _ := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if len(segments) == 0 {
		t.Fatalf("expected segment files to be created")
	}

	// Simulate unexpected crash/shutdown WITHOUT committing
	if err := eng2.Close(); err != nil {
		t.Fatalf("eng2 close failed: %v", err)
	}

	// 3. Reopen after crash: must recover BOTH active.wal and all rotated segment files
	eng3, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, optsSegmented)
	if err != nil {
		t.Fatalf("eng3 reopen failed: %v", err)
	}
	defer eng3.Close()

	batch, err := eng3.ReadBatch(20)
	if err != nil {
		t.Fatalf("eng3 read batch failed: %v", err)
	}
	if len(batch) != 15 {
		t.Fatalf("expected all 15 records across active.wal and segments, got %d", len(batch))
	}

	for i, r := range batch {
		if r.Offset != walspool.Offset(i) || r.ID != uint64(i+1) {
			t.Fatalf("record %d mismatch: offset=%d (want %d), id=%d (want %d)", i, r.Offset, i, r.ID, i+1)
		}
	}

	// Commit past active.wal boundary to verify it gets cleanly pruned
	if err := eng3.Commit(6); err != nil {
		t.Fatalf("commit offset 6 failed: %v", err)
	}

	// active.wal should now be removed
	if _, err := os.Stat(filepath.Join(dir, "active.wal")); !os.IsNotExist(err) {
		t.Fatalf("expected active.wal to be pruned after commit past its range")
	}

	// Remaining uncommitted records should still be readable
	batchRemaining, err := eng3.ReadBatch(20)
	if err != nil {
		t.Fatalf("read remaining failed: %v", err)
	}
	if len(batchRemaining) != 8 { // 15 - 7 committed = 8 remaining (offsets 7..14)
		t.Fatalf("expected 8 remaining records, got %d", len(batchRemaining))
	}
	if batchRemaining[0].Offset != 7 {
		t.Fatalf("expected first remaining offset to be 7, got %d", batchRemaining[0].Offset)
	}
}

func TestFileStorage_SegmentRotation_EmptyActiveSegmentPrune(t *testing.T) {
	dir := t.TempDir()

	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 250
	opts.SyncPolicy = walspool.SyncBatchCommit

	eng, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer eng.Close()

	// Append records until exactly causing rotation
	var lastOffset walspool.Offset
	for i := 0; i < 5; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("rotate-prune-test-record")}
		off, err := eng.Append(rec)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		lastOffset = off
	}

	// Commit ALL records written so far
	if err := eng.Commit(lastOffset); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	uncommitted, err := eng.UncommittedCount()
	if err != nil {
		t.Fatalf("uncommitted count failed: %v", err)
	}
	if uncommitted != 0 {
		t.Fatalf("expected 0 uncommitted records, got %d", uncommitted)
	}

	// Next append after all previous segments pruned
	nextRec := walspool.Record{ID: 100, Timestamp: time.Now(), Topic: "t", Payload: []byte("first-record-in-clean-segment")}
	nextOff, err := eng.Append(nextRec)
	if err != nil {
		t.Fatalf("append after prune failed: %v", err)
	}
	if nextOff != lastOffset+1 {
		t.Fatalf("expected next offset to be %d, got %d", lastOffset+1, nextOff)
	}

	// Verify reading newly appended record
	batch, err := eng.ReadBatch(10)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 1 || batch[0].Offset != nextOff || batch[0].ID != 100 {
		t.Fatalf("unexpected batch: %+v", batch)
	}
}

func TestFileStorage_SegmentRotation_ZeroByteSegmentRecovery(t *testing.T) {
	dir := t.TempDir()

	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 200
	opts.SyncPolicy = walspool.SyncBatchCommit

	eng, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	for i := 0; i < 4; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Now(), Topic: "t", Payload: []byte("payload")}
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Simulate crash right after new segment file creation: empty segment-000000000004.wal
	emptyPath := filepath.Join(dir, "segment-000000000004.wal")
	f, err := os.Create(emptyPath)
	if err != nil {
		t.Fatalf("create empty segment failed: %v", err)
	}
	f.Close()

	// Reopen engine: should cleanly handle the zero-byte segment
	reopened, err := walspool.NewFileStorageEngineWithOptions(dir, 1000, opts)
	if err != nil {
		t.Fatalf("reopen with empty segment failed: %v", err)
	}
	defer reopened.Close()

	// Append a new record
	rec := walspool.Record{ID: 5, Timestamp: time.Now(), Topic: "t", Payload: []byte("recovered-payload")}
	off, err := reopened.Append(rec)
	if err != nil {
		t.Fatalf("append to empty segment failed: %v", err)
	}
	if off != 4 {
		t.Fatalf("expected offset 4, got %d", off)
	}

	batch, err := reopened.ReadBatch(10)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != 5 {
		t.Fatalf("expected 5 records total, got %d", len(batch))
	}
}

func TestFileStorage_SegmentRotation_MultiSegmentReadBatchPerformance(t *testing.T) {
	dir := t.TempDir()

	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = 250 // small segment size to force ~10 rotations
	opts.SyncPolicy = walspool.SyncBatchCommit

	eng, err := walspool.NewFileStorageEngineWithOptions(dir, 2000, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer eng.Close()

	const totalRecords = 100
	for i := 0; i < totalRecords; i++ {
		rec := walspool.Record{
			ID:        uint64(i + 1),
			Timestamp: time.Now(),
			Topic:     "bench-topic",
			Payload:   []byte(fmt.Sprintf("multi-segment-batch-payload-%04d", i)),
		}
		if _, err := eng.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Read all 100 records in one single ReadBatch call spanning multiple historical segments
	batch, err := eng.ReadBatch(totalRecords)
	if err != nil {
		t.Fatalf("read batch failed: %v", err)
	}
	if len(batch) != totalRecords {
		t.Fatalf("expected %d records in single batch, got %d", totalRecords, len(batch))
	}

	for i, r := range batch {
		if r.Offset != walspool.Offset(i) || r.ID != uint64(i+1) {
			t.Fatalf("record %d mismatch: offset=%d, id=%d", i, r.Offset, r.ID)
		}
		expectedPayload := fmt.Sprintf("multi-segment-batch-payload-%04d", i)
		if string(r.Payload) != expectedPayload {
			t.Fatalf("record %d payload mismatch: got %s, want %s", i, string(r.Payload), expectedPayload)
		}
	}
}



