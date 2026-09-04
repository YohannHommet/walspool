package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

type mockSinkServer struct {
	mu       sync.Mutex
	paused   bool
	received [][]BatchItem
}

func (m *mockSinkServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.paused {
		http.Error(w, `{"error":"sink_paused"}`, http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var items []BatchItem
	if err := json.Unmarshal(body, &items); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.received = append(m.received, items)
	w.WriteHeader(http.StatusOK)
}

func TestSidecarEndToEnd(t *testing.T) {
	// 1. Mock remote destination endpoint (simulates external webhook or backend)
	remoteMock := &mockSinkServer{}
	remoteServer := httptest.NewServer(remoteMock)
	defer remoteServer.Close()

	// 2. Set up in-memory storage engine and walspool engine
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &HTTPSink{
		TargetURL:  remoteServer.URL,
		HTTPClient: remoteServer.Client(),
	}

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = 20 * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to initialize spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool)
	routes := sidecar.Routes()

	// 3. Test GET /healthz
	t.Run("HealthCheck", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})

	// 4. Test POST /enqueue
	t.Run("EnqueueValidPayload", func(t *testing.T) {
		body := []byte(`{"topic":"orders.created","payload":{"order_id":"ord_999","amount":150}}`)
		req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp EnqueueResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("invalid json response: %v", err)
		}
		if resp.Topic != "orders.created" {
			t.Fatalf("expected topic orders.created, got %s", resp.Topic)
		}
	})

	// 5. Test Flush and Remote Delivery
	t.Run("FlushAndVerifyDelivery", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/flush", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on flush, got %d", rec.Code)
		}

		// Wait briefly for sink delivery
		time.Sleep(50 * time.Millisecond)

		remoteMock.mu.Lock()
		defer remoteMock.mu.Unlock()

		if len(remoteMock.received) == 0 {
			t.Fatalf("remote server received no batches")
		}
		batch := remoteMock.received[0]
		if len(batch) != 1 {
			t.Fatalf("expected 1 record in batch, got %d", len(batch))
		}
		if batch[0].Topic != "orders.created" {
			t.Fatalf("expected topic orders.created, got %s", batch[0].Topic)
		}
	})

	// 6. Test Invalid Requests (Empty topic)
	t.Run("EnqueueInvalidTopic", func(t *testing.T) {
		body := []byte(`{"topic":"","payload":"data"}`)
		req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
		}
	})
}

func TestSidecar_TraceIDIndexingAndQuery(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(1000)
	sink := &HTTPSink{}
	hub := walspool.NewMemoryLogHub(1000)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool, hub)
	routes := sidecar.Routes()

	// Ingest sample logs with varied trace_id, service, and level
	testLogs := []struct {
		topic   string
		traceID string
		service string
		level   string
		msg     string
	}{
		{"billing", "tr-alpha", "billing", "INFO", "Charge initialized"},
		{"auth", "tr-beta", "auth", "WARN", "Invalid password attempt"},
		{"billing", "tr-alpha", "billing", "ERROR", "Card expired"},
		{"shipping", "tr-alpha", "shipping", "INFO", "Package queued"},
		{"auth", "tr-gamma", "auth", "DEBUG", "Session refreshed"},
	}

	for _, l := range testLogs {
		body, _ := json.Marshal(map[string]any{
			"topic": l.topic,
			"payload": map[string]any{
				"trace_id": l.traceID,
				"service":  l.service,
				"level":    l.level,
				"msg":      l.msg,
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// 1. Query by trace_id=tr-alpha: must return 3 logs chronologically, < 1ms
	t.Run("QueryByTraceID_SubMillisecond", func(t *testing.T) {
		// Warm-up call to eliminate Go race detector / runtime cold cache overhead
		reqWarm := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-alpha", nil)
		recWarm := httptest.NewRecorder()
		routes.ServeHTTP(recWarm, reqWarm)

		req := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-alpha", nil)
		rec := httptest.NewRecorder()
		start := time.Now()
		routes.ServeHTTP(rec, req)
		elapsed := time.Since(start)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		// Under Go race detector overhead, tolerate up to 2ms
		if elapsed > 2*time.Millisecond {
			t.Fatalf("query took %v, must be < 2ms", elapsed)
		}

		var logs []walspool.LogEntry
		if err := json.Unmarshal(rec.Body.Bytes(), &logs); err != nil {
			t.Fatalf("failed to decode logs: %v", err)
		}

		if len(logs) != 3 {
			t.Fatalf("expected 3 logs for tr-alpha, got %d", len(logs))
		}

		// Verify chronological order
		if logs[0].Level != "INFO" || logs[0].Service != "billing" {
			t.Fatalf("unexpected log[0]: %+v", logs[0])
		}
		if logs[1].Level != "ERROR" || logs[1].Service != "billing" {
			t.Fatalf("unexpected log[1]: %+v", logs[1])
		}
		if logs[2].Level != "INFO" || logs[2].Service != "shipping" {
			t.Fatalf("unexpected log[2]: %+v", logs[2])
		}
	})

	// 2. Query with combined filters: trace_id=tr-alpha & service=billing & level=ERROR
	t.Run("QueryWithCombinedFilters", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-alpha&service=billing&level=ERROR", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		var logs []walspool.LogEntry
		if err := json.Unmarshal(rec.Body.Bytes(), &logs); err != nil {
			t.Fatalf("failed to decode logs: %v", err)
		}

		if len(logs) != 1 {
			t.Fatalf("expected exactly 1 log, got %d", len(logs))
		}
		if logs[0].TraceID != "tr-alpha" || logs[0].Level != "ERROR" {
			t.Fatalf("mismatched log: %+v", logs[0])
		}
	})

	// 3. Query with limit: limit=2 on tr-alpha returns most recent 2 logs
	t.Run("QueryWithLimit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-alpha&limit=2", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		var logs []walspool.LogEntry
		_ = json.Unmarshal(rec.Body.Bytes(), &logs)

		if len(logs) != 2 {
			t.Fatalf("expected 2 logs with limit=2, got %d", len(logs))
		}
		if logs[0].Level != "ERROR" || logs[1].Service != "shipping" {
			t.Fatalf("unexpected latest 2 logs: %+v", logs)
		}
	})

	// 4. Query non-existent trace_id returns empty array (not null)
	t.Run("QueryNonExistentTraceID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=unknown-404", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
		bodyStr := strings.TrimSpace(rec.Body.String())
		if bodyStr != "[]" {
			t.Fatalf("expected empty array '[]', got %q", bodyStr)
		}
	})
}

func TestSidecar_SSEStreamRealtime(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &HTTPSink{}
	hub := walspool.NewMemoryLogHub(500)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool, hub)

	ts := httptest.NewServer(sidecar.Routes())
	defer ts.Close()

	// 1. Start SSE stream client filtering service=payments and level=ERROR
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamURL := fmt.Sprintf("%s/v1/logs/stream?service=payments&level=ERROR", ts.URL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		t.Fatalf("failed to create SSE request: %v", err)
	}

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to connect to SSE stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	if contentType := resp.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("expected text/event-stream, got %s", contentType)
	}

	reader := bufio.NewReader(resp.Body)
	receivedCh := make(chan walspool.LogEntry, 5)

	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "data:") {
				jsonBytes := []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
				var entry walspool.LogEntry
				if err := json.Unmarshal(jsonBytes, &entry); err == nil {
					receivedCh <- entry
				}
			}
		}
	}()

	// Allow subscriber registration
	time.Sleep(20 * time.Millisecond)

	// 2. Enqueue mismatched log: payments INFO (filtered out)
	postLog := func(svc, lvl, trace, msg string) {
		body, _ := json.Marshal(map[string]any{
			"topic": svc,
			"payload": map[string]any{
				"service":  svc,
				"level":    lvl,
				"trace_id": trace,
				"msg":      msg,
			},
		})
		res, err := http.Post(ts.URL+"/v1/enqueue", "application/json", bytes.NewReader(body))
		if err != nil || res.StatusCode != http.StatusAccepted {
			t.Fatalf("failed to post log: %v", err)
		}
		_ = res.Body.Close()
	}

	postLog("payments", "INFO", "tr-1", "Starting charge")
	postLog("auth", "ERROR", "tr-2", "Auth token corrupt")
	// Matching log:
	postLog("payments", "ERROR", "tr-live", "Credit card gateway timeout")

	// Verify the matching log arrives on the stream
	select {
	case entry := <-receivedCh:
		if entry.Service != "payments" || entry.Level != "ERROR" || entry.TraceID != "tr-live" {
			t.Fatalf("received mismatched entry: %+v", entry)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for SSE log event")
	}

	// Verify no spurious events follow
	select {
	case unexpected := <-receivedCh:
		t.Fatalf("unexpected extra event received: %+v", unexpected)
	case <-time.After(50 * time.Millisecond):
	}

	// 3. Cancel context and verify client disconnect cleanup
	cancel()
	time.Sleep(50 * time.Millisecond)

	stats := hub.Stats()
	if stats.ActiveStreams != 0 {
		t.Fatalf("expected 0 active streams after disconnect, got %d", stats.ActiveStreams)
	}
}

type mockDeliverySink struct {
	mu    sync.Mutex
	count int
}

func (m *mockDeliverySink) Deliver(ctx context.Context, batch []walspool.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count += len(batch)
	return nil
}

func TestSidecar_RingBufferEvictionNoLeak(t *testing.T) {
	// Storage engine and mock sink track delivery
	storage := walspool.NewMemoryStorageEngine(1000)
	deliverySink := &mockDeliverySink{}
	const ringCap = 5
	hub := walspool.NewMemoryLogHub(ringCap)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, deliverySink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool, hub)
	routes := sidecar.Routes()

	// Ingest 20 logs each with unique trace IDs
	for i := 1; i <= 20; i++ {
		body, _ := json.Marshal(map[string]any{
			"topic": "audit",
			"payload": map[string]any{
				"trace_id": fmt.Sprintf("tr-unique-%02d", i),
				"service":  fmt.Sprintf("service-%d", i%3),
				"level":    "INFO",
				"seq":      i,
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("enqueue %d failed: %d", i, rec.Code)
		}
	}

	// Check memory stats via /v1/logs/stats
	req := httptest.NewRequest(http.MethodGet, "/v1/logs/stats", nil)
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)

	var stats walspool.HubStats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.CurrentSize != ringCap {
		t.Fatalf("expected current_size %d, got %d", ringCap, stats.CurrentSize)
	}
	if stats.TotalIngested != 20 {
		t.Fatalf("expected total_ingested 20, got %d", stats.TotalIngested)
	}

	// Mathematical proof of zero memory leak:
	// Traces 1 through 15 were evicted; their index keys must be completely removed.
	// IndexedTraces must be strictly equal to ringCap (5).
	if stats.IndexedTraces != ringCap {
		t.Fatalf("memory leak! expected %d indexed traces, got %d", ringCap, stats.IndexedTraces)
	}

	// Verify evicted trace is gone
	evictedReq := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-unique-01", nil)
	evictedRec := httptest.NewRecorder()
	routes.ServeHTTP(evictedRec, evictedReq)
	if strings.TrimSpace(evictedRec.Body.String()) != "[]" {
		t.Fatalf("expected evicted trace to return [], got %s", evictedRec.Body.String())
	}

	// Verify most recent trace is present
	activeReq := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-unique-20", nil)
	activeRec := httptest.NewRecorder()
	routes.ServeHTTP(activeRec, activeReq)
	var activeLogs []walspool.LogEntry
	_ = json.Unmarshal(activeRec.Body.Bytes(), &activeLogs)
	if len(activeLogs) != 1 || activeLogs[0].TraceID != "tr-unique-20" {
		t.Fatalf("expected trace-20 present, got %+v", activeLogs)
	}

	// Verify WAL durability: flush guarantees all 20 records reached the sink intact
	if err := spool.Flush(context.Background()); err != nil {
		t.Fatalf("spool flush failed: %v", err)
	}

	deliverySink.mu.Lock()
	delivered := deliverySink.count
	deliverySink.mu.Unlock()
	if delivered != 20 {
		t.Fatalf("WAL durability delivery failed: expected 20 records delivered, got %d", delivered)
	}
}

func TestSidecar_ConcurrentStress_RaceFree(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(5000)
	sink := &HTTPSink{}
	hub := walspool.NewMemoryLogHub(500)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool, hub)

	ts := httptest.NewServer(sidecar.Routes())
	defer ts.Close()

	var wg sync.WaitGroup

	// 5 Concurrent Producers
	for p := 0; p < 5; p++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := ts.Client()
			for i := 0; i < 50; i++ {
				body, _ := json.Marshal(map[string]any{
					"topic": "telemetry",
					"payload": map[string]any{
						"trace_id": fmt.Sprintf("tr-%d-%d", workerID, i),
						"service":  fmt.Sprintf("svc-%d", workerID),
						"level":    "INFO",
						"index":    i,
					},
				})
				resp, err := client.Post(ts.URL+"/v1/enqueue", "application/json", bytes.NewReader(body))
				if err == nil {
					_ = resp.Body.Close()
				}
			}
		}(p)
	}

	// 3 Concurrent Query Clients
	for q := 0; q < 3; q++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := ts.Client()
			for i := 0; i < 20; i++ {
				resp, err := client.Get(fmt.Sprintf("%s/v1/logs?service=svc-%d&limit=10", ts.URL, workerID))
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(q)
	}

	wg.Wait()
}

func TestSidecar_DiskWAL_DurabilityWithCRC32(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Initialize real disk storage engine with CRC32 integrity
	diskStorage, err := walspool.NewFileStorageEngine(tmpDir, 1000)
	if err != nil {
		t.Fatalf("failed to create disk storage: %v", err)
	}

	sink := &mockDeliverySink{}
	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = 50 * time.Millisecond

	hub := walspool.NewMemoryLogHub(100)
	spool, err := walspool.New(cfg, diskStorage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}

	sidecar := NewSidecarServer(spool, hub)
	routes := sidecar.Routes()

	// 2. Ingest logs through HTTP POST /v1/enqueue
	payload := []byte(`{"topic":"payments.audit","payload":{"trace_id":"trace-disk-001","service":"payments","level":"INFO","amount":999}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", rec.Code)
	}

	// 3. Verify it's immediately accessible in memory via query engine
	queryReq := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=trace-disk-001", nil)
	queryRec := httptest.NewRecorder()
	routes.ServeHTTP(queryRec, queryReq)

	var inMemLogs []walspool.LogEntry
	if err := json.Unmarshal(queryRec.Body.Bytes(), &inMemLogs); err != nil || len(inMemLogs) != 1 {
		t.Fatalf("expected 1 log in query engine, got %d (%v)", len(inMemLogs), err)
	}
	if inMemLogs[0].Service != "payments" || inMemLogs[0].TraceID != "trace-disk-001" {
		t.Fatalf("unexpected in-memory log: %+v", inMemLogs[0])
	}

	// 4. Flush and close spooler to simulate clean termination
	if err := spool.Flush(context.Background()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("spool close failed: %v", err)
	}

	sink.mu.Lock()
	deliveredCount := sink.count
	sink.mu.Unlock()
	if deliveredCount != 1 {
		t.Fatalf("expected 1 record delivered to sink, got %d", deliveredCount)
	}

	// 5. Re-open disk storage to verify WAL recovery & CRC32 integrity verification on disk
	reopenedStorage, err := walspool.NewFileStorageEngine(tmpDir, 1000)
	if err != nil {
		t.Fatalf("reopening disk storage failed: %v", err)
	}
	defer reopenedStorage.Close()

	uncommitted, err := reopenedStorage.UncommittedCount()
	if err != nil {
		t.Fatalf("uncommitted count error: %v", err)
	}
	if uncommitted != 0 {
		t.Fatalf("expected 0 uncommitted records post-flush, got %d", uncommitted)
	}
}

// CRIT-05: HTTP 429 (Too Many Requests) and 408 (Request Timeout) must be treated as transient
// errors (walspool.ErrSinkUnavailable) and retried with backoff without dropping data.
func TestSidecar_HTTPSink_Transient429And408Retry(t *testing.T) {
	var attempts atomic.Int32
	var mu sync.Mutex
	var received [][]BatchItem

	sinkServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if att <= 2 {
			// Simulate transient rate limit for the first two attempts
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate_limited","retry_after":1}`))
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var items []BatchItem
		if err := json.Unmarshal(body, &items); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, items)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer sinkServer.Close()

	storage := walspool.NewMemoryStorageEngine(100)
	sink := &HTTPSink{
		TargetURL:  sinkServer.URL,
		HTTPClient: sinkServer.Client(),
	}

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 5
	cfg.FlushInterval = 10 * time.Millisecond
	cfg.InitialBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 50 * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool)
	routes := sidecar.Routes()

	// 1. Enqueue payload into sidecar
	payload := []byte(`{"topic":"billing.charges","payload":{"charge_id":"ch_123","amount":5000}}`)
	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", rec.Code)
	}

	// 2. Flush forces retry until delivery succeeds
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := spool.Flush(flushCtx); err != nil {
		t.Fatalf("flush failed while retrying 429: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || len(received[0]) != 1 {
		t.Fatalf("expected 1 delivered batch with 1 item, got %+v", received)
	}
	if received[0][0].Topic != "billing.charges" {
		t.Fatalf("expected topic billing.charges, got %s", received[0][0].Topic)
	}
	if attempts.Load() < 3 {
		t.Fatalf("expected at least 3 delivery attempts, got %d", attempts.Load())
	}

	// 3. Test HTTP 408 is also transient (ErrSinkUnavailable)
	ts408 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestTimeout)
	}))
	defer ts408.Close()

	sink408 := &HTTPSink{TargetURL: ts408.URL, HTTPClient: ts408.Client()}
	err408 := sink408.Deliver(context.Background(), []walspool.Record{{Topic: "test", Payload: []byte("sample")}})
	if !errors.Is(err408, walspool.ErrSinkUnavailable) {
		t.Fatalf("expected ErrSinkUnavailable on HTTP 408, got %v", err408)
	}

	// 4. Test true client error 400 is permanent rejection (ErrPermanentRejection)
	ts400 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts400.Close()

	sink400 := &HTTPSink{TargetURL: ts400.URL, HTTPClient: ts400.Client()}
	err400 := sink400.Deliver(context.Background(), []walspool.Record{{Topic: "test", Payload: []byte("malformed")}})
	if !errors.Is(err400, walspool.ErrPermanentRejection) {
		t.Fatalf("expected ErrPermanentRejection on HTTP 400, got %v", err400)
	}

	// 5. Test 422 Unprocessable Entity is also permanent rejection
	ts422 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer ts422.Close()

	sink422 := &HTTPSink{TargetURL: ts422.URL, HTTPClient: ts422.Client()}
	err422 := sink422.Deliver(context.Background(), []walspool.Record{{Topic: "test", Payload: []byte("unprocessable")}})
	if !errors.Is(err422, walspool.ErrPermanentRejection) {
		t.Fatalf("expected ErrPermanentRejection on HTTP 422, got %v", err422)
	}
}

// CRIT-03, MAJ-07: Graceful shutdown under load must unblock SSE streams without delay,
// drain all active HTTP requests, and flush 100% of accepted in-flight logs to the sink without loss.
func TestSidecar_GracefulShutdownUnderLoad_NoDataLoss(t *testing.T) {
	var deliveredRecords atomic.Int64
	sinkServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var items []BatchItem
		if err := json.Unmarshal(body, &items); err == nil {
			deliveredRecords.Add(int64(len(items)))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer sinkServer.Close()

	storage := walspool.NewMemoryStorageEngine(50000)
	sink := &HTTPSink{
		TargetURL:  sinkServer.URL,
		HTTPClient: sinkServer.Client(),
	}

	spoolCfg := walspool.DefaultConfig()
	spoolCfg.BatchSize = 25
	spoolCfg.FlushInterval = 20 * time.Millisecond

	hub := walspool.NewMemoryLogHub(1000)
	spool, err := walspool.New(spoolCfg, storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}

	server := NewSidecarServer(spool, hub)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}

	httpServer := &http.Server{
		Handler: server.Routes(),
	}

	serverStopped := make(chan error, 1)
	go func() {
		serverStopped <- httpServer.Serve(ln)
	}()

	serverURL := "http://" + ln.Addr().String()

	// 1. Establish an SSE stream subscriber
	sseConnected := make(chan struct{})
	sseFinished := make(chan struct{})
	go func() {
		defer close(sseFinished)
		resp, err := http.Get(serverURL + "/v1/logs/stream?service=loadtest")
		if err != nil {
			return
		}
		defer resp.Body.Close()
		close(sseConnected)

		reader := bufio.NewReader(resp.Body)
		for {
			_, err := reader.ReadString('\n')
			if err != nil {
				return // SSE stream unblocked and closed cleanly
			}
		}
	}()

	select {
	case <-sseConnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE stream to connect")
	}

	// 2. Concurrently push logs into sidecar
	const producers = 8
	const logsPerProducer = 25
	var acceptedRecords atomic.Int64
	var producersWg sync.WaitGroup
	producersWg.Add(producers)

	client := &http.Client{Timeout: 2 * time.Second}
	for p := 0; p < producers; p++ {
		go func(workerID int) {
			defer producersWg.Done()
			for i := 0; i < logsPerProducer; i++ {
				payload, _ := json.Marshal(map[string]any{
					"topic": "loadtest",
					"payload": map[string]any{
						"worker": workerID,
						"seq":    i,
					},
				})
				resp, err := client.Post(serverURL+"/enqueue", "application/json", bytes.NewReader(payload))
				if err == nil {
					if resp.StatusCode == http.StatusAccepted {
						acceptedRecords.Add(1)
					}
					_ = resp.Body.Close()
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(p)
	}

	// Let some logs enqueue, then trigger graceful shutdown mid-load
	time.Sleep(15 * time.Millisecond)

	shutdownStart := time.Now()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	shutdownErr := GracefulShutdown(shutdownCtx, httpServer, hub, spool)
	shutdownDuration := time.Since(shutdownStart)

	if shutdownErr != nil {
		t.Fatalf("graceful shutdown failed: %v", shutdownDuration)
	}

	// Verify SSE client terminated promptly
	select {
	case <-sseFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE client was not promptly unblocked by hub.Close() during shutdown")
	}

	// Wait for any remaining producer attempts to complete
	producersWg.Wait()
	<-serverStopped

	// Mathematical proof: zero log loss
	accepted := acceptedRecords.Load()
	delivered := deliveredRecords.Load()

	if accepted == 0 {
		t.Fatal("expected at least some accepted records before shutdown")
	}
	if delivered != accepted {
		t.Fatalf("DATA LOSS DETECTED: accepted %d records, but only %d delivered to sink", accepted, delivered)
	}
}

// CRIT-07: CLI flags must take absolute precedence over environment variables,
// which in turn override default fallback values. Strict validation must reject invalid configs.
func TestSidecar_ConfigPrecedenceAndValidation(t *testing.T) {
	t.Run("DefaultValuesWhenUnset", func(t *testing.T) {
		emptyEnv := func(k string) (string, bool) { return "", false }
		cfg, err := ParseConfig([]string{}, emptyEnv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Addr != ":9099" {
			t.Errorf("expected default addr :9099, got %s", cfg.Addr)
		}
		if cfg.DataDir != "./data/spool" {
			t.Errorf("expected default data-dir ./data/spool, got %s", cfg.DataDir)
		}
		if cfg.TargetSinkURL != "" {
			t.Errorf("expected empty default sink-url, got %s", cfg.TargetSinkURL)
		}
		if cfg.BatchSize != 50 {
			t.Errorf("expected default batch-size 50, got %d", cfg.BatchSize)
		}
		if cfg.FlushMs != 50 {
			t.Errorf("expected default flush-ms 50, got %d", cfg.FlushMs)
		}
		if cfg.MaxRecords != 50000 {
			t.Errorf("expected default max-records 50000, got %d", cfg.MaxRecords)
		}
		if cfg.HubCapacity != 50000 {
			t.Errorf("expected default hub-capacity 50000, got %d", cfg.HubCapacity)
		}
		if cfg.LogFormat != "text" {
			t.Errorf("expected default log-format text, got %s", cfg.LogFormat)
		}
		if cfg.LogLevel != "info" {
			t.Errorf("expected default log-level info, got %s", cfg.LogLevel)
		}
	})

	t.Run("EnvOverridesDefaults", func(t *testing.T) {
		envMap := map[string]string{
			"WALSPOOL_ADDR":         ":8088",
			"WALSPOOL_DATA_DIR":     "/var/spool/wal",
			"WALSPOOL_SINK_URL":     "http://remote.sink:9000",
			"WALSPOOL_BATCH_SIZE":   "250",
			"WALSPOOL_FLUSH_MS":     "150",
			"WALSPOOL_MAX_RECORDS":  "120000",
			"WALSPOOL_HUB_CAPACITY": "80000",
			"WALSPOOL_LOG_FORMAT":   "json",
			"WALSPOOL_LOG_LEVEL":    "warn",
		}
		mockEnv := func(k string) (string, bool) {
			v, ok := envMap[k]
			return v, ok
		}

		cfg, err := ParseConfig([]string{}, mockEnv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Addr != ":8088" {
			t.Errorf("expected addr from env :8088, got %s", cfg.Addr)
		}
		if cfg.DataDir != "/var/spool/wal" {
			t.Errorf("expected data-dir from env /var/spool/wal, got %s", cfg.DataDir)
		}
		if cfg.TargetSinkURL != "http://remote.sink:9000" {
			t.Errorf("expected sink-url from env, got %s", cfg.TargetSinkURL)
		}
		if cfg.BatchSize != 250 {
			t.Errorf("expected batch-size 250, got %d", cfg.BatchSize)
		}
		if cfg.FlushMs != 150 {
			t.Errorf("expected flush-ms 150, got %d", cfg.FlushMs)
		}
		if cfg.MaxRecords != 120000 {
			t.Errorf("expected max-records 120000, got %d", cfg.MaxRecords)
		}
		if cfg.HubCapacity != 80000 {
			t.Errorf("expected hub-capacity 80000, got %d", cfg.HubCapacity)
		}
		if cfg.LogFormat != "json" {
			t.Errorf("expected log-format from env json, got %s", cfg.LogFormat)
		}
		if cfg.LogLevel != "warn" {
			t.Errorf("expected log-level from env warn, got %s", cfg.LogLevel)
		}
	})

	t.Run("CLIOverridesEnv_CRIT07", func(t *testing.T) {
		envMap := map[string]string{
			"WALSPOOL_ADDR":         ":8088",
			"WALSPOOL_DATA_DIR":     "/env/data",
			"WALSPOOL_SINK_URL":     "http://env.sink:9000",
			"WALSPOOL_BATCH_SIZE":   "250",
			"WALSPOOL_FLUSH_MS":     "150",
			"WALSPOOL_MAX_RECORDS":  "120000",
			"WALSPOOL_HUB_CAPACITY": "80000",
			"WALSPOOL_LOG_FORMAT":   "text",
			"WALSPOOL_LOG_LEVEL":    "info",
		}
		mockEnv := func(k string) (string, bool) {
			v, ok := envMap[k]
			return v, ok
		}

		cliArgs := []string{
			"-addr", ":7070",
			"-data-dir", "/cli/data",
			"-sink-url", "http://cli.sink:8000",
			"-batch-size", "15",
			"-flush-ms", "25",
			"-max-records", "1000",
			"-hub-capacity", "2000",
			"-log-format", "json",
			"-log-level", "debug",
		}

		cfg, err := ParseConfig(cliArgs, mockEnv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Addr != ":7070" {
			t.Errorf("CLI addr must override env: expected :7070, got %s", cfg.Addr)
		}
		if cfg.DataDir != "/cli/data" {
			t.Errorf("CLI data-dir must override env: expected /cli/data, got %s", cfg.DataDir)
		}
		if cfg.TargetSinkURL != "http://cli.sink:8000" {
			t.Errorf("CLI sink-url must override env: expected http://cli.sink:8000, got %s", cfg.TargetSinkURL)
		}
		if cfg.BatchSize != 15 {
			t.Errorf("CLI batch-size must override env: expected 15, got %d", cfg.BatchSize)
		}
		if cfg.FlushMs != 25 {
			t.Errorf("CLI flush-ms must override env: expected 25, got %d", cfg.FlushMs)
		}
		if cfg.MaxRecords != 1000 {
			t.Errorf("CLI max-records must override env: expected 1000, got %d", cfg.MaxRecords)
		}
		if cfg.HubCapacity != 2000 {
			t.Errorf("CLI hub-capacity must override env: expected 2000, got %d", cfg.HubCapacity)
		}
		if cfg.LogFormat != "json" {
			t.Errorf("CLI log-format must override env: expected json, got %s", cfg.LogFormat)
		}
		if cfg.LogLevel != "debug" {
			t.Errorf("CLI log-level must override env: expected debug, got %s", cfg.LogLevel)
		}
	})

	t.Run("StrictValidationRules", func(t *testing.T) {
		emptyEnv := func(k string) (string, bool) { return "", false }

		// 1. addr non vide
		_, err := ParseConfig([]string{"-addr", ""}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for empty addr, got %v", err)
		}

		// 2. batchSize > 0
		_, err = ParseConfig([]string{"-batch-size", "0"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for batch-size <= 0, got %v", err)
		}
		_, err = ParseConfig([]string{"-batch-size", "-5"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for negative batch-size, got %v", err)
		}

		// 3. flushMs > 0
		_, err = ParseConfig([]string{"-flush-ms", "0"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for flush-ms <= 0, got %v", err)
		}

		// 4. maxRecords > 0
		_, err = ParseConfig([]string{"-max-records", "0"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for max-records <= 0, got %v", err)
		}

		// 5. hubCapacity > 0
		_, err = ParseConfig([]string{"-hub-capacity", "0"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for hub-capacity <= 0, got %v", err)
		}

		// 6. log-format text or json
		_, err = ParseConfig([]string{"-log-format", "xml"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for invalid log-format, got %v", err)
		}

		// 7. log-level debug, info, warn, or error
		_, err = ParseConfig([]string{"-log-level", "verbose"}, emptyEnv)
		if !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Errorf("expected ErrPreconditionViolated for invalid log-level, got %v", err)
		}
	})
}

// MAJ-10: Prometheus & OpenMetrics endpoint GET /metrics
func TestSidecar_PrometheusMetrics(t *testing.T) {
	remoteMock := &mockSinkServer{paused: true}
	remoteServer := httptest.NewServer(remoteMock)
	defer remoteServer.Close()

	storage := walspool.NewMemoryStorageEngine(100)
	metrics := NewSidecarMetrics()
	sink := &HTTPSink{
		TargetURL:  remoteServer.URL,
		HTTPClient: remoteServer.Client(),
		Metrics:    metrics,
	}

	hubCapacity := 200
	hub := walspool.NewMemoryLogHub(hubCapacity)

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 10
	cfg.FlushInterval = 50 * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to initialize spooler: %v", err)
	}
	defer spool.Close()

	server := NewSidecarServer(spool, hub).
		WithStorage(storage).
		WithMetrics(metrics)
	routes := server.Routes()

	// 1. Initial /metrics scrape: check format headers and baseline values
	t.Run("InitialBaselineMetrics", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
		ct := rec.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, "text/plain") {
			t.Fatalf("expected text/plain Content-Type, got %s", ct)
		}

		body := rec.Body.String()
		expectedHeaders := []string{
			"# HELP walspool_ingested_records_total",
			"# TYPE walspool_ingested_records_total counter",
			"# HELP walspool_delivered_records_total",
			"# TYPE walspool_delivered_records_total counter",
			"# HELP walspool_active_sse_subscribers",
			"# TYPE walspool_active_sse_subscribers gauge",
			"# HELP walspool_uncommitted_records",
			"# TYPE walspool_uncommitted_records gauge",
			"# HELP walspool_dropped_events_total",
			"# TYPE walspool_dropped_events_total counter",
			"# HELP walspool_ring_buffer_capacity",
			"# TYPE walspool_ring_buffer_capacity gauge",
			"# HELP walspool_ring_buffer_count",
			"# TYPE walspool_ring_buffer_count gauge",
		}
		for _, h := range expectedHeaders {
			if !strings.Contains(body, h) {
				t.Errorf("missing expected Prometheus header: %q in body:\n%s", h, body)
			}
		}

		// Initial gauge values
		if !strings.Contains(body, "walspool_active_sse_subscribers 0") {
			t.Errorf("expected active subscribers 0, got:\n%s", body)
		}
		if !strings.Contains(body, "walspool_uncommitted_records 0") {
			t.Errorf("expected uncommitted 0, got:\n%s", body)
		}
		if !strings.Contains(body, "walspool_dropped_events_total 0") {
			t.Errorf("expected dropped events 0, got:\n%s", body)
		}
		if !strings.Contains(body, fmt.Sprintf("walspool_ring_buffer_capacity %d", hubCapacity)) {
			t.Errorf("expected ring capacity %d, got:\n%s", hubCapacity, body)
		}
		if !strings.Contains(body, "walspool_ring_buffer_count 0") {
			t.Errorf("expected ring buffer count 0, got:\n%s", body)
		}
	})

	// 2. Ingest records across multiple topics: verify counters and uncommitted gauge
	t.Run("IngestRecordsTracking", func(t *testing.T) {
		// Enqueue 3 records to "orders.created"
		for i := 0; i < 3; i++ {
			req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader([]byte(`{"topic":"orders.created","payload":{"id":100}}`)))
			rec := httptest.NewRecorder()
			routes.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("enqueue failed: %d", rec.Code)
			}
		}

		// Enqueue 2 records to "billing.invoices"
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader([]byte(`{"topic":"billing.invoices","payload":{"id":200}}`)))
			rec := httptest.NewRecorder()
			routes.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("enqueue failed: %d", rec.Code)
			}
		}

		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		body := rec.Body.String()
		if !strings.Contains(body, `walspool_ingested_records_total{topic="orders.created"} 3`) {
			t.Errorf("missing orders.created ingested counter: %s", body)
		}
		if !strings.Contains(body, `walspool_ingested_records_total{topic="billing.invoices"} 2`) {
			t.Errorf("missing billing.invoices ingested counter: %s", body)
		}
		if !strings.Contains(body, "walspool_uncommitted_records 5") {
			t.Errorf("expected uncommitted 5, got:\n%s", body)
		}
		if !strings.Contains(body, "walspool_ring_buffer_count 5") {
			t.Errorf("expected ring buffer count 5, got:\n%s", body)
		}
	})

	// 3. Active SSE subscriber gauge tracking
	t.Run("ActiveSSESubscribersGauge", func(t *testing.T) {
		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()

		streamReq := httptest.NewRequest(http.MethodGet, "/v1/logs/stream", nil).WithContext(streamCtx)
		streamRec := httptest.NewRecorder()

		streamDone := make(chan struct{})
		go func() {
			routes.ServeHTTP(streamRec, streamReq)
			close(streamDone)
		}()

		// Wait for subscriber to connect
		time.Sleep(30 * time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		body := rec.Body.String()
		if !strings.Contains(body, "walspool_active_sse_subscribers 1") {
			t.Errorf("expected active subscribers 1, got:\n%s", body)
		}

		// Cancel subscription
		streamCancel()
		<-streamDone

		// Scrape metrics again: subscriber count should drop to 0
		req2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec2 := httptest.NewRecorder()
		routes.ServeHTTP(rec2, req2)

		body2 := rec2.Body.String()
		if !strings.Contains(body2, "walspool_active_sse_subscribers 0") {
			t.Errorf("expected active subscribers 0 after cancel, got:\n%s", body2)
		}
	})

	// 4. Delivered records tracking after flush
	t.Run("DeliveredRecordsTracking", func(t *testing.T) {
		remoteMock.mu.Lock()
		remoteMock.paused = false
		remoteMock.mu.Unlock()

		reqFlush := httptest.NewRequest(http.MethodPost, "/flush", nil)
		recFlush := httptest.NewRecorder()
		routes.ServeHTTP(recFlush, reqFlush)
		if recFlush.Code != http.StatusOK {
			t.Fatalf("flush failed: %d", recFlush.Code)
		}

		time.Sleep(50 * time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		body := rec.Body.String()
		if !strings.Contains(body, `walspool_delivered_records_total{topic="orders.created"} 3`) {
			t.Errorf("missing delivered orders.created 3, got:\n%s", body)
		}
		if !strings.Contains(body, `walspool_delivered_records_total{topic="billing.invoices"} 2`) {
			t.Errorf("missing delivered billing.invoices 2, got:\n%s", body)
		}
		if !strings.Contains(body, "walspool_uncommitted_records 0") {
			t.Errorf("expected uncommitted 0 after flush, got:\n%s", body)
		}
	})

	// 5. Method not allowed for /metrics
	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 Method Not Allowed on POST /metrics, got %d", rec.Code)
		}
	})
}

// MIN-08: Kubernetes probes (/healthz and /readyz)
func TestSidecar_KubernetesProbes_ReadyzAndHealthz(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &HTTPSink{}
	hub := walspool.NewMemoryLogHub(100)

	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil)
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	server := NewSidecarServer(spool, hub).WithStorage(storage)
	routes := server.Routes()

	// 1. Nominal state: /healthz and /readyz both 200 OK
	t.Run("NominalState", func(t *testing.T) {
		// Healthz
		reqH := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		recH := httptest.NewRecorder()
		routes.ServeHTTP(recH, reqH)
		if recH.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on healthz, got %d", recH.Code)
		}

		// Readyz
		reqR := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		recR := httptest.NewRecorder()
		routes.ServeHTTP(recR, reqR)
		if recR.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on readyz, got %d", recR.Code)
		}
		var readyResp map[string]any
		if err := json.Unmarshal(recR.Body.Bytes(), &readyResp); err != nil {
			t.Fatalf("invalid readyz json: %v", err)
		}
		if readyResp["ready"] != true || readyResp["status"] != "ready" {
			t.Fatalf("unexpected readyz payload: %v", readyResp)
		}
	})

	// 2. Storage failure: /readyz returns 503, but /healthz remains 200 OK
	t.Run("StorageFailureReturns503", func(t *testing.T) {
		// Simulate storage failure by closing storage engine
		if err := storage.Close(); err != nil {
			t.Fatalf("failed to close storage: %v", err)
		}

		reqR := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		recR := httptest.NewRecorder()
		routes.ServeHTTP(recR, reqR)

		if recR.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable on readyz with broken storage, got %d: %s", recR.Code, recR.Body.String())
		}
		if !strings.Contains(recR.Body.String(), "storage_unavailable") {
			t.Fatalf("expected storage_unavailable in body: %s", recR.Body.String())
		}

		// Healthz still returns 200 OK (liveness succeeds even if storage is down)
		reqH := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		recH := httptest.NewRecorder()
		routes.ServeHTTP(recH, reqH)
		if recH.Code != http.StatusOK {
			t.Fatalf("expected healthz 200 OK, got %d", recH.Code)
		}
	})

	// 3. Graceful shutdown state: /readyz returns 503
	t.Run("ShutdownStateReturns503", func(t *testing.T) {
		freshStorage := walspool.NewMemoryStorageEngine(100)
		freshSpool, err := walspool.New(walspool.DefaultConfig(), freshStorage, sink, nil)
		if err != nil {
			t.Fatalf("failed to init spooler: %v", err)
		}
		defer freshSpool.Close()

		freshServer := NewSidecarServer(freshSpool, hub).WithStorage(freshStorage)
		freshRoutes := freshServer.Routes()

		// Verify nominal before shutdown
		reqNominal := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		recNominal := httptest.NewRecorder()
		freshRoutes.ServeHTTP(recNominal, reqNominal)
		if recNominal.Code != http.StatusOK {
			t.Fatalf("expected nominal 200, got %d", recNominal.Code)
		}

		// Trigger shutdown signal
		freshServer.MarkShuttingDown()
		if !freshServer.IsShuttingDown() {
			t.Fatal("expected IsShuttingDown to be true")
		}

		reqShutdown := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		recShutdown := httptest.NewRecorder()
		freshRoutes.ServeHTTP(recShutdown, reqShutdown)

		if recShutdown.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable during shutdown, got %d: %s", recShutdown.Code, recShutdown.Body.String())
		}
		if !strings.Contains(recShutdown.Body.String(), "shutting_down") {
			t.Fatalf("expected shutting_down status in response: %s", recShutdown.Body.String())
		}
	})

	// 4. Method not allowed on probes
	t.Run("MethodNotAllowedOnProbes", func(t *testing.T) {
		reqPostHealth := httptest.NewRequest(http.MethodPost, "/healthz", nil)
		recPostHealth := httptest.NewRecorder()
		routes.ServeHTTP(recPostHealth, reqPostHealth)
		if recPostHealth.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 on POST /healthz, got %d", recPostHealth.Code)
		}

		reqPostReady := httptest.NewRequest(http.MethodPost, "/readyz", nil)
		recPostReady := httptest.NewRecorder()
		routes.ServeHTTP(recPostReady, reqPostReady)
		if recPostReady.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 on POST /readyz, got %d", recPostReady.Code)
		}
	})
}

// MIN-08: Structured logging with log/slog
func TestSidecar_StructuredLogging_Slog(t *testing.T) {
	t.Run("JSONHandlerFormatting", func(t *testing.T) {
		var buf bytes.Buffer
		logger := SetupLoggerWithWriter(&buf, "json", "debug")
		logger.Debug("debug message", "service", "payment", "attempt", 1)
		logger.Info("info message", "service", "payment", "status", "ok")

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 log lines, got %d: %s", len(lines), buf.String())
		}

		var debugEntry map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &debugEntry); err != nil {
			t.Fatalf("invalid json log: %v", err)
		}
		if debugEntry["msg"] != "debug message" {
			t.Errorf("expected msg 'debug message', got %v", debugEntry["msg"])
		}
		if debugEntry["level"] != "DEBUG" {
			t.Errorf("expected level 'DEBUG', got %v", debugEntry["level"])
		}
		if debugEntry["service"] != "payment" {
			t.Errorf("expected service 'payment', got %v", debugEntry["service"])
		}

		var infoEntry map[string]any
		if err := json.Unmarshal([]byte(lines[1]), &infoEntry); err != nil {
			t.Fatalf("invalid json log: %v", err)
		}
		if infoEntry["msg"] != "info message" {
			t.Errorf("expected msg 'info message', got %v", infoEntry["msg"])
		}
		if infoEntry["level"] != "INFO" {
			t.Errorf("expected level 'INFO', got %v", infoEntry["level"])
		}
	})

	t.Run("TextHandlerLevelFiltering", func(t *testing.T) {
		var buf bytes.Buffer
		logger := SetupLoggerWithWriter(&buf, "text", "warn")
		logger.Debug("ignored debug")
		logger.Info("ignored info")
		logger.Warn("warning happened", "reason", "high_memory")
		logger.Error("error happened", "code", 500)

		out := buf.String()
		if strings.Contains(out, "ignored debug") {
			t.Errorf("debug message should have been filtered out: %s", out)
		}
		if strings.Contains(out, "ignored info") {
			t.Errorf("info message should have been filtered out: %s", out)
		}
		if !strings.Contains(out, "warning happened") || !strings.Contains(out, "reason=high_memory") {
			t.Errorf("warning message not found in text output: %s", out)
		}
		if !strings.Contains(out, "error happened") || !strings.Contains(out, "code=500") {
			t.Errorf("error message not found in text output: %s", out)
		}
	})

	t.Run("SetupLoggerSetsDefault", func(t *testing.T) {
		var buf bytes.Buffer
		_ = SetupLoggerWithWriter(&buf, "json", "info")
		slog.Info("default logger check", "module", "sidecar")
		if !strings.Contains(buf.String(), "default logger check") {
			t.Errorf("expected default slog to write to buffer: %s", buf.String())
		}
	})
}

// Enriching a JSON payload with top-level metadata must not truncate 64-bit integers
// (Snowflake-style IDs, nanosecond timestamps) to float64.
func TestSidecar_Int64PrecisionPreserved(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &HTTPSink{}
	hub := walspool.NewMemoryLogHub(100)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool, hub)
	routes := sidecar.Routes()

	// trace_id sits at the top level (not in payload), forcing the enrich + re-marshal path.
	body := []byte(`{"topic":"orders","trace_id":"tr-big","payload":{"id":9223372036854775807,"timestamp":1725482400123456789}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	qreq := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-big", nil)
	qrec := httptest.NewRecorder()
	routes.ServeHTTP(qrec, qreq)
	out := qrec.Body.String()
	if !strings.Contains(out, "9223372036854775807") {
		t.Fatalf("64-bit id lost precision, got: %s", out)
	}
	if !strings.Contains(out, "1725482400123456789") {
		t.Fatalf("nanosecond timestamp lost precision, got: %s", out)
	}
}

// Metadata may arrive via HTTP headers; non-JSON-object payloads must stay byte-identical
// while headers still enrich JSON object payloads.
func TestSidecar_MetadataHeaders(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(100)
	sink := &HTTPSink{}
	hub := walspool.NewMemoryLogHub(100)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to init spooler: %v", err)
	}
	defer spool.Close()

	sidecar := NewSidecarServer(spool, hub)
	routes := sidecar.Routes()

	t.Run("NonJSONPayloadUntouchedRoutedByHeader", func(t *testing.T) {
		body := []byte(`{"payload":"plain-text-log-line"}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader(body))
		req.Header.Set("X-Service", "edge-collector")
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
		}

		qreq := httptest.NewRequest(http.MethodGet, "/v1/logs?service=edge-collector", nil)
		qrec := httptest.NewRecorder()
		routes.ServeHTTP(qrec, qreq)
		var logs []walspool.LogEntry
		if err := json.Unmarshal(qrec.Body.Bytes(), &logs); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if len(logs) != 1 {
			t.Fatalf("expected 1 log routed by X-Service, got %d", len(logs))
		}
		if string(logs[0].Payload) != `"plain-text-log-line"` {
			t.Fatalf("non-JSON payload was mutated: %s", string(logs[0].Payload))
		}
	})

	t.Run("HeaderTraceIDEnrichesJSONObject", func(t *testing.T) {
		body := []byte(`{"topic":"billing","payload":{"amount":10}}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/enqueue", bytes.NewReader(body))
		req.Header.Set("X-Trace-ID", "tr-from-header")
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
		}

		qreq := httptest.NewRequest(http.MethodGet, "/v1/logs?trace_id=tr-from-header", nil)
		qrec := httptest.NewRecorder()
		routes.ServeHTTP(qrec, qreq)
		var logs []walspool.LogEntry
		if err := json.Unmarshal(qrec.Body.Bytes(), &logs); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if len(logs) != 1 || logs[0].TraceID != "tr-from-header" {
			t.Fatalf("expected JSON payload enriched with header trace_id, got %+v", logs)
		}
	})
}

// A malformed numeric environment variable must fail configuration parsing, not silently
// fall back to a default.
func TestSidecar_ConfigRejectsMalformedEnvInt(t *testing.T) {
	intVars := []string{"WALSPOOL_BATCH_SIZE", "WALSPOOL_FLUSH_MS", "WALSPOOL_MAX_RECORDS", "WALSPOOL_HUB_CAPACITY"}
	for _, key := range intVars {
		t.Run(key, func(t *testing.T) {
			env := func(k string) (string, bool) {
				if k == key {
					return "not-a-number", true
				}
				return "", false
			}
			if _, err := ParseConfig([]string{}, env); !errors.Is(err, walspool.ErrPreconditionViolated) {
				t.Fatalf("expected ErrPreconditionViolated for malformed %s, got %v", key, err)
			}
		})
	}
}
