package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

type benchNoopSink struct{}

func (s *benchNoopSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	return nil
}

func setupBenchServer(b *testing.B) (*httptest.Server, *http.Client, *walspool.MemoryLogHub, func()) {
	b.Helper()

	baseDir := os.Getenv("WALSPOOL_BENCH_DIR")
	if baseDir == "" {
		baseDir = filepath.Join("..", "..", "tmp")
	}
	_ = os.MkdirAll(baseDir, 0755)

	dir, err := os.MkdirTemp(baseDir, "sidecar_bench_*")
	if err != nil {
		b.Fatalf("failed to create temp bench dir: %v", err)
	}

	storage, err := walspool.NewFileStorageEngineWithOptions(dir, b.N*1000+1000000, walspool.DefaultFileStorageOptions())
	if err != nil {
		b.Fatalf("failed to initialize storage: %v", err)
	}

	hub := walspool.NewMemoryLogHub(100000)
	sink := &benchNoopSink{}

	spoolCfg := walspool.DefaultConfig()
	spoolCfg.BatchSize = 100
	spoolCfg.FlushInterval = 50 * time.Millisecond

	spool, err := walspool.New(spoolCfg, storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		b.Fatalf("failed to initialize spooler: %v", err)
	}

	server := NewSidecarServer(spool, hub).WithStorage(storage)
	ts := httptest.NewServer(server.Routes())

	transport := &http.Transport{
		DisableKeepAlives:   false,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	cleanup := func() {
		ts.Close()
		transport.CloseIdleConnections()
		_ = spool.Close()
		_ = os.RemoveAll(dir)
	}

	return ts, client, hub, cleanup
}

// BenchmarkSidecar_HTTP_Enqueue_Sequential measures single-client roundtrip latency
// and throughput through the full HTTP network stack (POST /enqueue) on localhost.
func BenchmarkSidecar_HTTP_Enqueue_Sequential(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/enqueue"
	jsonBody := []byte(`{"topic":"telemetry","payload":{"user_id":42,"status":"ok"},"trace_id":"tr-seq-1","service":"api","level":"INFO"}`)
	b.SetBytes(int64(len(jsonBody)))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(jsonBody))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP request failed at %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusAccepted {
			b.Fatalf("unexpected status %d at iteration %d", resp.StatusCode, i)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// BenchmarkSidecar_HTTP_Enqueue_Parallel measures maximum concurrent HTTP request ingestion
// capacity when multiple goroutines bomb the sidecar simultaneously over persistent connections.
func BenchmarkSidecar_HTTP_Enqueue_Parallel(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/enqueue"
	jsonBody := []byte(`{"topic":"telemetry.parallel","payload":{"user_id":99,"status":"ok"},"trace_id":"tr-par-1","service":"order-svc","level":"INFO"}`)
	b.SetBytes(int64(len(jsonBody)))

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				b.Fatalf("parallel HTTP request failed: %v", err)
			}
			if resp.StatusCode != http.StatusAccepted {
				b.Fatalf("unexpected status: %d", resp.StatusCode)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}

// BenchmarkSidecar_HTTP_QueryLogs_TraceID measures end-to-end HTTP query latency for trace lookups.
func BenchmarkSidecar_HTTP_QueryLogs_TraceID(b *testing.B) {
	ts, client, hub, cleanup := setupBenchServer(b)
	defer cleanup()

	// Pre-populate 5,000 logs into the in-memory hub
	payload := json.RawMessage(`{"item":1}`)
	for i := 0; i < 5000; i++ {
		_ = hub.Ingest(walspool.LogEntry{
			Topic:   "orders",
			Service: "checkout",
			TraceID: "tr-target-42",
			Level:   "INFO",
			Payload: payload,
		})
	}

	queryURL := ts.URL + "/v1/logs?trace_id=tr-target-42&limit=50"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodGet, queryURL, nil)
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("query failed at %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected query status: %d", resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// BenchmarkSidecar_EnqueueBatch_100 measures throughput and latency for batches of 100 records.
func BenchmarkSidecar_EnqueueBatch_100(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/v1/enqueue/batch"
	batch := make([]EnqueueRequest, 100)
	for i := 0; i < 100; i++ {
		batch[i] = EnqueueRequest{
			Topic:   "telemetry.batch",
			Payload: json.RawMessage(`{"user_id":42,"status":"ok"}`),
			TraceID: "tr-batch-100",
			Service: "api",
			Level:   "INFO",
		}
	}
	body, _ := json.Marshal(batch)
	b.SetBytes(int64(len(body)))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected status %d", resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// BenchmarkSidecar_EnqueueBatch_1000 measures maximum throughput for batches of 1000 records.
func BenchmarkSidecar_EnqueueBatch_1000(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/v1/enqueue/batch"
	batch := make([]EnqueueRequest, 1000)
	for i := 0; i < 1000; i++ {
		batch[i] = EnqueueRequest{
			Topic:   "telemetry.batch1000",
			Payload: json.RawMessage(`{"user_id":42,"status":"ok"}`),
			TraceID: "tr-batch-1000",
			Service: "api",
			Level:   "INFO",
		}
	}
	body, _ := json.Marshal(batch)
	b.SetBytes(int64(len(body)))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected status %d", resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// BenchmarkSidecar_HTTP_EnqueueBatch_Sequential measures throughput across various batch sizes.
func BenchmarkSidecar_HTTP_EnqueueBatch_Sequential(b *testing.B) {
	sizes := []int{10, 100, 500, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("BatchSize_%d", size), func(b *testing.B) {
			ts, client, _, cleanup := setupBenchServer(b)
			defer cleanup()

			url := ts.URL + "/v1/enqueue/batch"
			batch := make([]EnqueueRequest, size)
			for i := 0; i < size; i++ {
				batch[i] = EnqueueRequest{
					Topic:   "telemetry.batch",
					Payload: json.RawMessage(`{"user_id":42,"status":"ok"}`),
					TraceID: "tr-batch-seq",
					Service: "api",
					Level:   "INFO",
				}
			}
			body, _ := json.Marshal(batch)
			b.SetBytes(int64(len(body)))

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					b.Fatalf("HTTP request failed: %v", err)
				}
				if resp.StatusCode != http.StatusOK {
					b.Fatalf("unexpected status %d", resp.StatusCode)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
}

// BenchmarkSidecar_HTTP_EnqueueBatch_Parallel measures concurrent batch ingestion capacity.
func BenchmarkSidecar_HTTP_EnqueueBatch_Parallel(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/v1/enqueue/batch"
	batch := make([]EnqueueRequest, 100)
	for i := 0; i < 100; i++ {
		batch[i] = EnqueueRequest{
			Topic:   "telemetry.batch.parallel",
			Payload: json.RawMessage(`{"user_id":99,"status":"ok"}`),
			TraceID: "tr-batch-par",
			Service: "api",
			Level:   "INFO",
		}
	}
	body, _ := json.Marshal(batch)
	b.SetBytes(int64(len(body)))

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				b.Fatalf("parallel batch HTTP request failed: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				b.Fatalf("unexpected status: %d", resp.StatusCode)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}

// BenchmarkSidecar_HTTP_Enqueue_NDJSON_Batch100 measures HTTP streaming ingestion throughput
// and allocations for batches of 100 NDJSON log lines.
func BenchmarkSidecar_HTTP_Enqueue_NDJSON_Batch100(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/v1/enqueue"
	var buf bytes.Buffer
	for i := 0; i < 100; i++ {
		buf.WriteString(`{"topic":"bench.stream","service":"bench-svc","level":"INFO","payload":{"idx":1,"message":"bench event item"}}` + "\n")
	}
	payloadBytes := buf.Bytes()
	b.SetBytes(int64(len(payloadBytes)))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP NDJSON request failed at %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusAccepted {
			b.Fatalf("unexpected status %d at iteration %d", resp.StatusCode, i)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// BenchmarkSidecar_EnqueueNDJSON_1000 measures HTTP streaming ingestion throughput
// for larger batches of 1000 NDJSON log lines.
func BenchmarkSidecar_EnqueueNDJSON_1000(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()
	client.Timeout = 30 * time.Second

	url := ts.URL + "/v1/enqueue"
	var buf bytes.Buffer
	for i := 0; i < 1000; i++ {
		buf.WriteString(`{"topic":"bench.stream.1k","service":"bench-svc","level":"INFO","payload":{"idx":1,"message":"bench event item"}}` + "\n")
	}
	payloadBytes := buf.Bytes()
	b.SetBytes(int64(len(payloadBytes)))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP NDJSON request failed at %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusAccepted {
			b.Fatalf("unexpected status %d at iteration %d", resp.StatusCode, i)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// BenchmarkSidecar_HTTP_Enqueue_NDJSON_LargeLine measures memory re-use efficiency
// when processing large lines (500 KiB) near the safety ceiling.
func BenchmarkSidecar_HTTP_Enqueue_NDJSON_LargeLine(b *testing.B) {
	ts, client, _, cleanup := setupBenchServer(b)
	defer cleanup()

	url := ts.URL + "/v1/enqueue"
	largePayload := make([]byte, 500*1024)
	for i := range largePayload {
		largePayload[i] = 'x'
	}
	largeJSON := `{"topic":"bench.large","payload":{"data":"` + string(largePayload) + `"}}` + "\n"
	payloadBytes := []byte(largeJSON)
	b.SetBytes(int64(len(payloadBytes)))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP NDJSON large line failed at %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusAccepted {
			b.Fatalf("unexpected status %d at iteration %d", resp.StatusCode, i)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}
