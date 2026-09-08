package main

import (
	"bytes"
	"context"
	"encoding/json"
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

	storage, err := walspool.NewFileStorageEngineWithOptions(dir, b.N*10+200000, walspool.DefaultFileStorageOptions())
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
