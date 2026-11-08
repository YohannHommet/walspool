package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YohannHommet/walspool"
)

// bugBatchNoopSink accepts every batch without doing any I/O.
type bugBatchNoopSink struct{}

func (s *bugBatchNoopSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	return nil
}

// TestBugBatchEnrich500 verifies that a batch item whose raw JSON payload is
// just under the 1 MB limit but whose metadata enrichment pushes it over the
// limit is rejected as a client error (400), not surfaced as a 500.
func TestBugBatchEnrich500(t *testing.T) {
	storage := walspool.NewMemoryStorageEngine(1000)
	spool, err := walspool.New(walspool.DefaultConfig(), storage, &bugBatchNoopSink{}, nil)
	if err != nil {
		t.Fatalf("failed to initialize spooler: %v", err)
	}
	defer spool.Close()

	server := NewSidecarServer(spool)
	routes := server.Routes()

	// A JSON object 10 bytes below MaxPayloadSize. Enrichment adds
	// `,"service":"svc"` (16 bytes), pushing the payload over the limit.
	payload := []byte(`{"msg":"` + strings.Repeat("a", MaxPayloadSize-20) + `"}`)
	if len(payload) > MaxPayloadSize {
		t.Fatalf("test setup error: raw payload is %d bytes, limit is %d", len(payload), MaxPayloadSize)
	}
	if len(payload)+len(`,"service":"svc"`) <= MaxPayloadSize {
		t.Fatalf("test setup error: enriched payload would not exceed the limit")
	}

	batch := []EnqueueRequest{{
		Topic:   "svc",
		Service: "svc",
		Payload: json.RawMessage(payload),
	}}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("failed to marshal batch request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/enqueue/batch", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for enriched-oversized batch item, got %d: %s", rec.Code, rec.Body.String())
	}
}
