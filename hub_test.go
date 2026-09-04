package walspool_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

func TestHub_Preconditions(t *testing.T) {
	hub := walspool.NewMemoryLogHub(100)
	defer hub.Close()

	// Empty topic and empty service must fail with ErrPreconditionViolated
	err := hub.Ingest(walspool.LogEntry{
		Payload: json.RawMessage(`{"msg":"test"}`),
	})
	if !errors.Is(err, walspool.ErrPreconditionViolated) {
		t.Fatalf("expected ErrPreconditionViolated, got %v", err)
	}
}

func TestHub_TraceIDAndServiceIndexing(t *testing.T) {
	hub := walspool.NewMemoryLogHub(1000)
	defer hub.Close()

	now := time.Now().UTC()
	entries := []walspool.LogEntry{
		{
			Timestamp: now.Add(1 * time.Millisecond),
			Topic:     "auth",
			Service:   "auth-service",
			TraceID:   "tr-100",
			Level:     "INFO",
			Payload:   json.RawMessage(`{"msg":"login attempt"}`),
		},
		{
			Timestamp: now.Add(2 * time.Millisecond),
			Topic:     "billing",
			Service:   "billing-service",
			TraceID:   "tr-200",
			Level:     "WARN",
			Payload:   json.RawMessage(`{"msg":"card expiring"}`),
		},
		{
			Timestamp: now.Add(3 * time.Millisecond),
			Topic:     "billing",
			Service:   "billing-service",
			TraceID:   "tr-100",
			Level:     "ERROR",
			Payload:   json.RawMessage(`{"msg":"payment failed"}`),
		},
		{
			Timestamp: now.Add(4 * time.Millisecond),
			Topic:     "auth",
			Service:   "auth-service",
			TraceID:   "tr-100",
			Level:     "DEBUG",
			Payload:   json.RawMessage(`{"msg":"token refreshed"}`),
		},
	}

	for _, e := range entries {
		if err := hub.Ingest(e); err != nil {
			t.Fatalf("unexpected ingest error: %v", err)
		}
	}

	// 1. Query by TraceID: tr-100
	start := time.Now()
	res := hub.Query(walspool.LogQuery{TraceID: "tr-100"})
	queryDuration := time.Since(start)

	if queryDuration > 1*time.Millisecond {
		t.Fatalf("query took %v, must be < 1ms", queryDuration)
	}

	if len(res) != 3 {
		t.Fatalf("expected 3 entries for tr-100, got %d", len(res))
	}
	// Verify chronological ordering
	if res[0].Level != "INFO" || res[1].Level != "ERROR" || res[2].Level != "DEBUG" {
		t.Fatalf("unexpected chronological order: %+v", res)
	}

	// 2. Query by TraceID + Service filter
	res = hub.Query(walspool.LogQuery{TraceID: "tr-100", Service: "billing-service"})
	if len(res) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(res))
	}
	if res[0].TraceID != "tr-100" || res[0].Service != "billing-service" {
		t.Fatalf("unexpected result: %+v", res[0])
	}

	// 3. Query by Service: billing-service
	res = hub.Query(walspool.LogQuery{Service: "billing-service"})
	if len(res) != 2 {
		t.Fatalf("expected 2 entries for billing-service, got %d", len(res))
	}

	// 4. Query by Level: ERROR
	res = hub.Query(walspool.LogQuery{Level: "ERROR"})
	if len(res) != 1 || res[0].Level != "ERROR" {
		t.Fatalf("expected 1 ERROR entry, got %+v", res)
	}
}

func TestHub_EvictionAndNoMemoryLeak(t *testing.T) {
	const capacity = 5
	hub := walspool.NewMemoryLogHub(capacity)
	defer hub.Close()

	// Ingest 15 distinct logs each with unique TraceID
	for i := 1; i <= 15; i++ {
		err := hub.Ingest(walspool.LogEntry{
			Topic:   "logs",
			Service: fmt.Sprintf("svc-%d", i%3),
			TraceID: fmt.Sprintf("trace-%03d", i),
			Level:   "INFO",
			Payload: json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("ingest error: %v", err)
		}
	}

	stats := hub.Stats()
	if stats.CurrentSize != capacity {
		t.Fatalf("expected current_size %d, got %d", capacity, stats.CurrentSize)
	}
	if stats.TotalIngested != 15 {
		t.Fatalf("expected total_ingested 15, got %d", stats.TotalIngested)
	}

	// Because traces 1 to 10 were evicted, their keys MUST be deleted from the index!
	// Exactly 5 traces (trace-011 through trace-015) must remain indexed.
	if stats.IndexedTraces != capacity {
		t.Fatalf("memory leak detected! expected %d indexed traces, got %d", capacity, stats.IndexedTraces)
	}

	// Query evicted trace: must return empty
	evicted := hub.Query(walspool.LogQuery{TraceID: "trace-001"})
	if len(evicted) != 0 {
		t.Fatalf("expected 0 results for evicted trace-001, got %d", len(evicted))
	}

	// Query present trace: must return entry
	present := hub.Query(walspool.LogQuery{TraceID: "trace-015"})
	if len(present) != 1 {
		t.Fatalf("expected 1 result for present trace-015, got %d", len(present))
	}
}

func TestHub_RealtimeStreaming(t *testing.T) {
	hub := walspool.NewMemoryLogHub(100)
	defer hub.Close()

	// Subscriber interested only in "billing" and "ERROR"
	filter := walspool.StreamFilter{
		Service: "billing",
		Level:   "ERROR",
	}
	_, ch, cancel := hub.Subscribe(filter)
	defer cancel()

	// Log 1: billing INFO (should NOT be received)
	_ = hub.Ingest(walspool.LogEntry{
		Service: "billing",
		Level:   "INFO",
		Payload: json.RawMessage(`{"step":1}`),
	})

	// Log 2: auth ERROR (should NOT be received)
	_ = hub.Ingest(walspool.LogEntry{
		Service: "auth",
		Level:   "ERROR",
		Payload: json.RawMessage(`{"step":2}`),
	})

	// Log 3: billing ERROR (SHOULD be received)
	_ = hub.Ingest(walspool.LogEntry{
		Service: "billing",
		Level:   "ERROR",
		Payload: json.RawMessage(`{"step":3,"matched":true}`),
	})

	select {
	case entry := <-ch:
		if entry.Service != "billing" || entry.Level != "ERROR" {
			t.Fatalf("received mismatched entry: %+v", entry)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for matching SSE log entry")
	}

	// Ensure no spurious events were queued
	select {
	case unexpected := <-ch:
		t.Fatalf("received unexpected entry: %+v", unexpected)
	default:
	}

	// Cancel subscription and verify channel is closed
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected channel to be closed after cancel")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for subscription channel closure")
	}
}

func TestHub_ConcurrentRaceFree(t *testing.T) {
	hub := walspool.NewMemoryLogHub(500)
	defer hub.Close()

	var wg sync.WaitGroup

	// 5 concurrent producers
	for p := 0; p < 5; p++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = hub.Ingest(walspool.LogEntry{
					Topic:   "audit",
					Service: fmt.Sprintf("service-%d", workerID),
					TraceID: fmt.Sprintf("trace-%d-%d", workerID, i),
					Level:   "INFO",
					Payload: json.RawMessage(`{"data":"event"}`),
				})
			}
		}(p)
	}

	// 3 concurrent query workers
	for q := 0; q < 3; q++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = hub.Query(walspool.LogQuery{
					Service: fmt.Sprintf("service-%d", workerID),
					Limit:   20,
				})
			}
		}(q)
	}

	// 2 concurrent subscribers
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ch, cancel := hub.Subscribe(walspool.StreamFilter{})
			defer cancel()
			for i := 0; i < 20; i++ {
				select {
				case <-ch:
				case <-time.After(50 * time.Millisecond):
					return
				}
			}
		}()
	}

	wg.Wait()
}

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

func TestHub_SSE_Concurrent100Subscribers_NoLockContention(t *testing.T) {
	hub := walspool.NewMemoryLogHub(10000)
	defer hub.Close()

	const numSubscribers = 100
	type subRecord struct {
		id     uint64
		ch     <-chan walspool.LogEntry
		cancel func()
	}

	subs := make([]subRecord, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		filter := walspool.StreamFilter{}
		if i%2 == 0 {
			filter.Service = "stream-svc"
		}
		id, ch, cancel := hub.Subscribe(filter)
		subs[i] = subRecord{id: id, ch: ch, cancel: cancel}
	}
	defer func() {
		for _, s := range subs {
			s.cancel()
		}
	}()

	var receivedCounts [numSubscribers]int64
	var subWg sync.WaitGroup
	stopCh := make(chan struct{})

	for i := 0; i < numSubscribers; i++ {
		subWg.Add(1)
		go func(idx int, ch <-chan walspool.LogEntry) {
			defer subWg.Done()
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						return
					}
					atomic.AddInt64(&receivedCounts[idx], 1)
				case <-stopCh:
					return
				}
			}
		}(i, subs[i].ch)
	}

	// 10 concurrent producers each ingesting 50 logs under high throughput
	const numProducers = 10
	const logsPerProducer = 50
	var prodWg sync.WaitGroup

	ingestStart := time.Now()
	for p := 0; p < numProducers; p++ {
		prodWg.Add(1)
		go func(pID int) {
			defer prodWg.Done()
			for j := 0; j < logsPerProducer; j++ {
				_ = hub.Ingest(walspool.LogEntry{
					Topic:   "telemetry",
					Service: "stream-svc",
					Level:   "INFO",
					Payload: json.RawMessage(`{"status":"ok"}`),
				})
			}
		}(p)
	}

	prodWg.Wait()
	ingestDuration := time.Since(ingestStart)

	// Ingestion of 500 logs distributed to 100 subscribers must be fast (< 500ms without lock contention)
	if ingestDuration > 500*time.Millisecond {
		t.Fatalf("ingestion took %v, expected < 500ms without lock bottleneck", ingestDuration)
	}

	// Allow subscribers to consume buffered channels
	time.Sleep(50 * time.Millisecond)
	close(stopCh)
	subWg.Wait()

	stats := hub.Stats()
	if stats.ActiveStreams != numSubscribers {
		t.Fatalf("expected %d active streams, got %d", numSubscribers, stats.ActiveStreams)
	}
	if stats.TotalIngested != numProducers*logsPerProducer {
		t.Fatalf("expected %d total ingested, got %d", numProducers*logsPerProducer, stats.TotalIngested)
	}

	// Verify subscribers received logs
	for i := 0; i < numSubscribers; i++ {
		if count := atomic.LoadInt64(&receivedCounts[i]); count == 0 {
			t.Fatalf("subscriber %d received 0 logs", i)
		}
	}
}

func TestHub_QueryPerformanceAndBoundedAllocations(t *testing.T) {
	hub := walspool.NewMemoryLogHub(50000)
	defer hub.Close()

	// Pre-populate 10,000 logs across 10 services
	for i := 0; i < 10000; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "telemetry",
			Service: fmt.Sprintf("svc-%d", i%10),
			TraceID: fmt.Sprintf("tr-%04d", i%500),
			Level:   "INFO",
			Payload: json.RawMessage(`{"step":1}`),
		})
	}

	// Dedicated service with 25 logs to test strict < 5 KB allocation limit for limit=50
	for i := 0; i < 25; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "audit",
			Service: "audit-bounded",
			Level:   "WARN",
			Payload: json.RawMessage(`{"auth":true}`),
		})
	}

	qBounded := walspool.LogQuery{
		Service: "audit-bounded",
		Limit:   50,
	}

	// 1. Warm up
	_ = hub.Query(qBounded)

	// 2. Performance: execution time is < 20 microseconds in normal execution,
	// and bounded under 200 microseconds under the Go race detector (-race adds ~50-100µs instrumentation).
	const iterations = 100
	start := time.Now()
	for i := 0; i < iterations; i++ {
		res := hub.Query(qBounded)
		if len(res) != 25 {
			t.Fatalf("expected 25 entries, got %d", len(res))
		}
	}
	avgDuration := time.Since(start) / iterations
	maxDuration := 200 * time.Microsecond
	if avgDuration > maxDuration {
		t.Fatalf("Query execution took %v, expected < %v", avgDuration, maxDuration)
	}

	// 3. Memory verification: allocations bounded < 5 KB for limit=50 (when returning 25 entries)
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	res := hub.Query(qBounded)
	runtime.ReadMemStats(&m2)
	_ = res

	allocBytes := m2.TotalAlloc - m1.TotalAlloc
	if allocBytes > 5*1024 {
		t.Fatalf("Query allocated %d bytes, expected < 5 KB (5120 bytes)", allocBytes)
	}

	// 4. Test limit=50 on svc-1 (1,000 logs in the index)
	q50 := walspool.LogQuery{
		Service: "svc-1",
		Limit:   50,
	}
	start50 := time.Now()
	res50 := hub.Query(q50)
	dur50 := time.Since(start50)

	if dur50 > maxDuration {
		t.Fatalf("Query for 50 items took %v, expected < %v", dur50, maxDuration)
	}
	if len(res50) != 50 {
		t.Fatalf("expected 50 items, got %d", len(res50))
	}
}

func TestHub_CaseInsensitiveService(t *testing.T) {
	hub := walspool.NewMemoryLogHub(100)
	defer hub.Close()

	entries := []walspool.LogEntry{
		{Topic: "billing", Service: "Billing-Service", Level: "INFO", Payload: json.RawMessage(`{"v":1}`)},
		{Topic: "billing", Service: "BILLING-SERVICE", Level: "WARN", Payload: json.RawMessage(`{"v":2}`)},
		{Topic: "billing", Service: "billing-service", Level: "ERROR", Payload: json.RawMessage(`{"v":3}`)},
		{Topic: "auth", Service: "Auth-Service", Level: "INFO", Payload: json.RawMessage(`{"v":4}`)},
	}

	for _, e := range entries {
		if err := hub.Ingest(e); err != nil {
			t.Fatalf("unexpected ingest error: %v", err)
		}
	}

	// Case variations for billing-service
	cases := []string{
		"billing-service",
		"BILLING-SERVICE",
		"BiLLiNg-SeRvIcE",
		" Billing-Service ",
	}

	for _, tc := range cases {
		res := hub.Query(walspool.LogQuery{Service: tc})
		if len(res) != 3 {
			t.Fatalf("lookup with service %q: expected 3 results, got %d", tc, len(res))
		}
		for _, item := range res {
			if item.Service != "billing-service" {
				t.Fatalf("expected normalized service 'billing-service', got %q", item.Service)
			}
		}
	}

	// Single service lookup
	resAuth := hub.Query(walspool.LogQuery{Service: "AUTH-SERVICE"})
	if len(resAuth) != 1 {
		t.Fatalf("expected 1 result for AUTH-SERVICE, got %d", len(resAuth))
	}
	if resAuth[0].Service != "auth-service" {
		t.Fatalf("expected normalized service 'auth-service', got %q", resAuth[0].Service)
	}

	// Defensive copy verification
	resAuth[0].Payload[0] = 'X'
	resAuthSecond := hub.Query(walspool.LogQuery{Service: "auth-service"})
	if string(resAuthSecond[0].Payload) != `{"v":4}` {
		t.Fatalf("defensive copy violated: original payload was corrupted: %s", string(resAuthSecond[0].Payload))
	}
}

func TestHub_SlowSubscriber_DroppedEvents(t *testing.T) {
	hub := walspool.NewMemoryLogHub(1000)
	defer hub.Close()

	// Channel buffer size is 256. Slow subscriber does not read from channel.
	_, _, cancel := hub.Subscribe(walspool.StreamFilter{})
	defer cancel()

	// Ingest 300 logs (44 more than channel capacity 256)
	const totalLogs = 300
	for i := 0; i < totalLogs; i++ {
		err := hub.Ingest(walspool.LogEntry{
			Topic:   "load",
			Service: "slow-test",
			Level:   "INFO",
			Payload: json.RawMessage(`{"index":1}`),
		})
		if err != nil {
			t.Fatalf("unexpected error during ingest: %v", err)
		}
	}

	stats := hub.Stats()
	if stats.DroppedEvents < 44 {
		t.Fatalf("expected at least 44 dropped events, got %d", stats.DroppedEvents)
	}
}

func TestHub_Subscribe_ClosedHub(t *testing.T) {
	hub := walspool.NewMemoryLogHub(100)

	// Close the hub first
	if err := hub.Close(); err != nil {
		t.Fatalf("failed to close hub: %v", err)
	}

	// Subscribe on closed hub
	subID, ch, cancel := hub.Subscribe(walspool.StreamFilter{})
	if subID != 0 {
		t.Fatalf("expected subID 0 on closed hub, got %d", subID)
	}

	// Channel must be closed immediately
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected channel to be closed, but received value")
		}
	default:
		t.Fatalf("channel was not closed immediately")
	}

	// Cancel must be a safe no-op
	cancel()
	cancel() // idempotent

	// Ingest on closed hub must return ErrSpoolerClosed
	err := hub.Ingest(walspool.LogEntry{
		Topic:   "test",
		Service: "test-svc",
		Payload: json.RawMessage(`{}`),
	})
	if !errors.Is(err, walspool.ErrSpoolerClosed) {
		t.Fatalf("expected ErrSpoolerClosed, got %v", err)
	}

	// Query on closed hub must return nil
	res := hub.Query(walspool.LogQuery{Service: "test-svc"})
	if res != nil {
		t.Fatalf("expected nil results from closed hub, got %+v", res)
	}
}

func TestHub_OnIngested_ObserverContract(t *testing.T) {
	hub := walspool.NewMemoryLogHub(100)
	defer hub.Close()

	rec := walspool.Record{
		ID:        99,
		Offset:    walspool.Offset(5),
		Timestamp: time.Now().UTC(),
		Topic:     "payments",
		Payload:   []byte(`{"trace_id":"tr-obs-1","service":"billing","level":"WARN","val":42}`),
	}

	hub.OnIngested(rec)

	res := hub.Query(walspool.LogQuery{TraceID: "tr-obs-1"})
	if len(res) != 1 {
		t.Fatalf("expected 1 query result for tr-obs-1, got %d", len(res))
	}
	if res[0].Service != "billing" || res[0].Level != "WARN" || res[0].TraceID != "tr-obs-1" {
		t.Fatalf("unexpected entry: %+v", res[0])
	}
	if res[0].ID != 99 {
		t.Fatalf("expected entry ID 99, got %d", res[0].ID)
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
