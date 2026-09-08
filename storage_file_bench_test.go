package walspool_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

// benchDir returns an isolated directory for disk benchmarks.
// By default, it targets "./tmp/bench_*" within the project filesystem to measure
// physical disk (NVMe/SSD) persistence rather than an in-memory tmpfs (/tmp).
// Can be overridden via the WALSPOOL_BENCH_DIR environment variable.
func benchDir(b *testing.B, prefix string) (string, func()) {
	b.Helper()
	baseDir := os.Getenv("WALSPOOL_BENCH_DIR")
	if baseDir == "" {
		baseDir = filepath.Join(".", "tmp")
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		b.Fatalf("failed to create bench base directory %q: %v", baseDir, err)
	}
	dir, err := os.MkdirTemp(baseDir, prefix+"_*")
	if err != nil {
		b.Fatalf("failed to create temporary bench directory: %v", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	return dir, cleanup
}

// Benchmark 1: Unbuffered / SyncEveryRecord (physical fsync on every write)
func BenchmarkFileStorage_Append_SyncEveryRecord(b *testing.B) {
	dir, cleanup := benchDir(b, "sync_every")
	defer cleanup()

	opts := walspool.FileStorageOptions{
		BufferSize: 4096,
		SyncPolicy: walspool.SyncEveryRecord,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(dir, b.N+1000, opts)
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
	dir, cleanup := benchDir(b, "sync_interval")
	defer cleanup()

	opts := walspool.DefaultFileStorageOptions() // 128KB buffer, SyncInterval 50ms
	engine, err := walspool.NewFileStorageEngineWithOptions(dir, b.N+1000, opts)
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
	dir, cleanup := benchDir(b, "sync_batch")
	defer cleanup()

	opts := walspool.FileStorageOptions{
		BufferSize: 128 * 1024,
		SyncPolicy: walspool.SyncBatchCommit,
	}
	engine, err := walspool.NewFileStorageEngineWithOptions(dir, b.N+1000, opts)
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

// Benchmark 4: Multi-goroutine parallel contention test (b.RunParallel)
func BenchmarkFileStorage_Append_Parallel(b *testing.B) {
	dir, cleanup := benchDir(b, "parallel")
	defer cleanup()

	opts := walspool.DefaultFileStorageOptions()
	engine, err := walspool.NewFileStorageEngineWithOptions(dir, b.N*10+100000, opts)
	if err != nil {
		b.Fatalf("init failed: %v", err)
	}
	defer engine.Close()

	payload := []byte("benchmark-concurrent-wal-record-payload-128-bytes-long-padding-example")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rec := walspool.Record{
			Timestamp: time.Now(),
			Topic:     "bench.parallel",
			Payload:   payload,
		}
		for pb.Next() {
			if _, err := engine.Append(rec); err != nil {
				b.Fatalf("concurrent append failed: %v", err)
			}
		}
	})
}

// Benchmark 5: Throughput across multiple realistic payload sizes (MB/s measurement)
func BenchmarkFileStorage_Append_PayloadSizes(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"1KB", 1024},
		{"16KB", 16 * 1024},
		{"64KB", 64 * 1024},
	}

	for _, sc := range sizes {
		b.Run(sc.name, func(b *testing.B) {
			dir, cleanup := benchDir(b, "payload_"+sc.name)
			defer cleanup()

			opts := walspool.DefaultFileStorageOptions()
			engine, err := walspool.NewFileStorageEngineWithOptions(dir, b.N+10000, opts)
			if err != nil {
				b.Fatalf("init failed: %v", err)
			}
			defer engine.Close()

			payload := make([]byte, sc.size)
			for i := range payload {
				payload[i] = byte('A' + (i % 26))
			}

			rec := walspool.Record{
				Timestamp: time.Now(),
				Topic:     "bench.payload",
				Payload:   payload,
			}

			b.SetBytes(int64(sc.size))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := engine.Append(rec); err != nil {
					b.Fatalf("append %s failed at %d: %v", sc.name, i, err)
				}
			}
		})
	}
}
