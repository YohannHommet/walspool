package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

type mockSinkServer struct {
	mu       sync.Mutex
	received [][]BatchItem
}

func (m *mockSinkServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
