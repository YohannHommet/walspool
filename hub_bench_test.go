package walspool_test

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

func BenchmarkHub_Ingest(b *testing.B) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	payload := json.RawMessage(`{"user_id":"usr_42","amount":199.99,"status":"ok"}`)
	entry := walspool.LogEntry{
		Topic:   "benchmark",
		Service: "bench-service",
		TraceID: "bench-trace",
		Level:   "INFO",
		Payload: payload,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hub.Ingest(entry)
	}
}

func BenchmarkHub_QueryByTraceID(b *testing.B) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	// Pre-populate with 10,000 logs across 1,000 traces
	for i := 0; i < 10000; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "benchmark",
			Service: fmt.Sprintf("svc-%d", i%10),
			TraceID: fmt.Sprintf("trace-%04d", i%1000),
			Level:   "INFO",
			Payload: json.RawMessage(`{"step":1}`),
		})
	}

	q := walspool.LogQuery{
		TraceID: "trace-0500",
		Limit:   100,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hub.Query(q)
	}
}

func BenchmarkHub_QueryByService(b *testing.B) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	for i := 0; i < 10000; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "benchmark",
			Service: fmt.Sprintf("svc-%d", i%10),
			TraceID: fmt.Sprintf("trace-%04d", i%1000),
			Level:   "INFO",
			Payload: json.RawMessage(`{"step":1}`),
		})
	}

	q := walspool.LogQuery{
		Service: "svc-5",
		Limit:   50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hub.Query(q)
	}
}

func BenchmarkHub_QueryRingBuffer(b *testing.B) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	for i := 0; i < 10000; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "benchmark",
			Service: fmt.Sprintf("svc-%d", i%10),
			TraceID: fmt.Sprintf("trace-%04d", i%1000),
			Level:   "INFO",
			Payload: json.RawMessage(`{"step":1}`),
		})
	}

	q := walspool.LogQuery{
		Limit: 50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hub.Query(q)
	}
}

// OnIngested metadata extraction on the hot path should stay allocation-light.
func BenchmarkHub_OnIngested(b *testing.B) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	rec := walspool.Record{
		Timestamp: time.Now(),
		Topic:     "benchmark",
		Payload:   []byte(`{"trace_id":"tr-bench","service":"billing","level":"info","user_id":"u42","amount":9223372036854775807}`),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec.ID = uint64(i + 1)
		hub.OnIngested(rec)
	}
}

// BenchmarkHub_Ingest_Parallel tests concurrent multi-goroutine ingestion contention (b.RunParallel).
func BenchmarkHub_Ingest_Parallel(b *testing.B) {
	hub := walspool.NewMemoryLogHub(100000)
	defer hub.Close()

	payload := json.RawMessage(`{"user_id":"usr_42","amount":199.99,"status":"ok"}`)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		entry := walspool.LogEntry{
			Topic:   "benchmark.parallel",
			Service: "billing",
			TraceID: "tr-parallel-bench",
			Level:   "INFO",
			Payload: payload,
		}
		for pb.Next() {
			_ = hub.Ingest(entry)
		}
	})
}

// BenchmarkHub_ReadWrite_Parallel tests concurrent mixed workload (80% Ingest, 20% Query) under load.
func BenchmarkHub_ReadWrite_Parallel(b *testing.B) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	// Pre-fill
	for i := 0; i < 5000; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "benchmark",
			Service: fmt.Sprintf("svc-%d", i%5),
			TraceID: fmt.Sprintf("tr-%04d", i%500),
			Level:   "INFO",
			Payload: json.RawMessage(`{"status":"ok"}`),
		})
	}

	var opCount uint64
	payload := json.RawMessage(`{"user_id":"usr_42","amount":99.0}`)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		entry := walspool.LogEntry{
			Topic:   "benchmark",
			Service: "svc-1",
			TraceID: "tr-0042",
			Level:   "INFO",
			Payload: payload,
		}
		query := walspool.LogQuery{
			TraceID: "tr-0042",
			Limit:   10,
		}
		for pb.Next() {
			n := atomic.AddUint64(&opCount, 1)
			if n%5 == 0 {
				_ = hub.Query(query)
			} else {
				_ = hub.Ingest(entry)
			}
		}
	})
}

// BenchmarkHub_Ingest_SaturationWrapAround measures sustained eviction efficiency when the ring buffer
// is 100% full, ensuring zero garbage collector leaks or degradation on continuous O(1) wrap-around.
func BenchmarkHub_Ingest_SaturationWrapAround(b *testing.B) {
	const ringCapacity = 10000
	hub := walspool.NewMemoryLogHub(ringCapacity)
	defer hub.Close()

	payload := json.RawMessage(`{"trace_id":"tr-wrap","service":"billing","amount":42}`)
	entry := walspool.LogEntry{
		Topic:   "bench.wrap",
		Service: "billing",
		TraceID: "tr-wrap",
		Level:   "INFO",
		Payload: payload,
	}

	// Pre-fill to 100% capacity so every iteration in the benchmark triggers O(1) eviction
	for i := 0; i < ringCapacity; i++ {
		_ = hub.Ingest(entry)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hub.Ingest(entry)
	}
}
