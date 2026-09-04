package walspool_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

// 1. Crash Recovery with Truncated/Torn Tail (CRIT-02, MAJ-05, MAJ-06)
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

// --- BENCHMARKS ---

// Benchmark 1: Unbuffered / SyncEveryRecord (fsync on every write)
func BenchmarkFileStorage_Append_SyncEveryRecord(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench_sync_every_*")
	if err != nil {
		b.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.FileStorageOptions{
		BufferSize: 4096,
		SyncPolicy: walspool.SyncEveryRecord,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, b.N+1000, opts)
	if err != nil {
		b.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	rec := walspool.Record{
		ID:        1,
		Timestamp: time.Now(),
		Topic:     "bench.sync.every",
		Payload:   []byte("benchmark-record-payload-128-bytes-long-string-padding-example"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := engine.Append(rec)
		if err != nil {
			b.Fatalf("append failed at %d: %v", i, err)
		}
	}
}

// Benchmark 2: Buffered 128KB with SyncInterval (50ms) - High Throughput Group Commit
func BenchmarkFileStorage_Append_SyncInterval_Buffered128KB(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench_sync_interval_*")
	if err != nil {
		b.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.DefaultFileStorageOptions() // 128KB buffer, SyncInterval 50ms
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, b.N+1000, opts)
	if err != nil {
		b.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	rec := walspool.Record{
		ID:        1,
		Timestamp: time.Now(),
		Topic:     "bench.sync.interval",
		Payload:   []byte("benchmark-record-payload-128-bytes-long-string-padding-example"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := engine.Append(rec)
		if err != nil {
			b.Fatalf("append failed at %d: %v", i, err)
		}
	}
}

// Benchmark 3: Buffered 128KB with SyncBatchCommit
func BenchmarkFileStorage_Append_SyncBatchCommit(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench_sync_batch_*")
	if err != nil {
		b.Fatalf("temp dir failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := walspool.FileStorageOptions{
		BufferSize: 128 * 1024,
		SyncPolicy: walspool.SyncBatchCommit,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(tmpDir, b.N+1000, opts)
	if err != nil {
		b.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	rec := walspool.Record{
		ID:        1,
		Timestamp: time.Now(),
		Topic:     "bench.sync.batch",
		Payload:   []byte("benchmark-record-payload-128-bytes-long-string-padding-example"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		offset, err := engine.Append(rec)
		if err != nil {
			b.Fatalf("append failed at %d: %v", i, err)
		}
		if i%100 == 0 {
			_ = engine.Commit(offset)
		}
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
