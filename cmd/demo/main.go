package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"walspool"
)

// ConsoleSink implements the Outbound Sink port, printing batches.
type ConsoleSink struct{}

func (ConsoleSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	fmt.Printf("📦 [Sink] Delivered batch of %d records:\n", len(batch))
	for _, rec := range batch {
		fmt.Printf("   -> Offset=%d ID=%d Topic=%s Payload=%s\n",
			rec.Offset, rec.ID, rec.Topic, string(rec.Payload))
	}
	return nil
}

func main() {
	tmpDir, err := os.MkdirTemp("", "walspool_demo_*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("🚀 Starting WAL Spooler with disk storage in: %s\n", tmpDir)

	storage, err := walspool.NewFileStorageEngine(tmpDir, 5000)
	if err != nil {
		panic(err)
	}

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 3
	cfg.FlushInterval = 20 * time.Millisecond

	spool, err := walspool.New(cfg, storage, ConsoleSink{}, nil)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	topics := []string{"sensor.telemetry", "security.audit", "metrics.cpu"}

	for i := 1; i <= 6; i++ {
		payload := []byte(fmt.Sprintf("{\"event_id\": %d, \"value\": %d}", i, i*42))
		topic := topics[i%len(topics)]
		if err := spool.Enqueue(ctx, topic, payload); err != nil {
			fmt.Printf("❌ Enqueue failed: %v\n", err)
			return
		}
		fmt.Printf("⚡ Enqueued record %d to %s\n", i, topic)
	}

	// Flush guarantees delivery
	if err := spool.Flush(ctx); err != nil {
		fmt.Printf("❌ Flush failed: %v\n", err)
	}
	_ = spool.Close()

	fmt.Println("\n🔄 Verifying Crash Recovery: re-opening WAL from disk...")
	reopenedStorage, err := walspool.NewFileStorageEngine(tmpDir, 5000)
	if err != nil {
		panic(err)
	}
	defer reopenedStorage.Close()

	count, _ := reopenedStorage.UncommittedCount()
	fmt.Printf("✅ Checkpoint intact. Uncommitted records remaining: %d (0 expected as all were flushed)\n", count)
	fmt.Printf("📁 WAL active log size: %d bytes\n", getFileSize(filepath.Join(tmpDir, "active.wal")))
	fmt.Println("✨ Demonstration finished successfully.")
}

func getFileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return fi.Size()
}
