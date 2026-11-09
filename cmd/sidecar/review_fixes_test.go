package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YohannHommet/walspool"
)

// reviewSpooler counts Enqueue calls so tests can prove whether Phase 1
// rejected a batch before any spool write.
type reviewSpooler struct {
	calls   int
	success int
	failAt  int
	failErr error
}

func (s *reviewSpooler) Enqueue(context.Context, string, []byte) error {
	s.calls++
	if s.calls == s.failAt {
		return fmt.Errorf("%w: forced test failure", s.failErr)
	}
	s.success++
	return nil
}

func (s *reviewSpooler) Flush(context.Context) error { return nil }
func (s *reviewSpooler) Close() error                { return nil }

func TestSidecarMetrics_CardinalityBounded(t *testing.T) {
	metrics := NewSidecarMetrics()
	const unique = 5000
	for i := 0; i < unique; i++ {
		topic := fmt.Sprintf("topic-%04d", i)
		metrics.RecordIngested(topic)
		metrics.RecordDelivered(topic)
	}

	if got := len(metrics.ingested); got > maxTrackedTopics {
		t.Fatalf("ingested cardinality = %d, want <= %d", got, maxTrackedTopics)
	}
	if got := len(metrics.delivered); got > maxTrackedTopics {
		t.Fatalf("delivered cardinality = %d, want <= %d", got, maxTrackedTopics)
	}
	if metrics.ingested[overflowTopic] == 0 {
		t.Fatalf("expected overflow bucket %q to absorb excess topics", overflowTopic)
	}
	var total uint64
	for _, count := range metrics.ingested {
		total += count
	}
	if total != unique {
		t.Fatalf("ingested counts sum = %d, want %d (no lost observations)", total, unique)
	}

	// A huge topic must not become an unbounded label key.
	metrics.RecordIngested(strings.Repeat("x", 10_000))
	for topic := range metrics.ingested {
		if len(topic) > maxTrackedTopicLength {
			t.Fatalf("metric label length = %d, want <= %d", len(topic), maxTrackedTopicLength)
		}
	}

	output := metrics.FormatPrometheus(nil, nil)
	if !strings.Contains(output, overflowTopic) {
		t.Fatalf("Prometheus output must expose the overflow bucket")
	}
	if lines := strings.Count(output, "\n"); lines > 4_000 {
		t.Fatalf("Prometheus output has %d lines, want bounded by cardinality cap", lines)
	}
}

func TestSidecarBatch_Phase2ProgressIncluded(t *testing.T) {
	spool := &reviewSpooler{
		failAt:  2,
		failErr: walspool.ErrPreconditionViolated,
	}
	server := NewSidecarServer(spool)
	body, err := json.Marshal([]EnqueueRequest{
		{Topic: "a", Payload: json.RawMessage(`"one"`)},
		{Topic: "b", Payload: json.RawMessage(`"two"`)},
	})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/enqueue/batch", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	if got := response["enqueued"]; got != float64(1) {
		t.Fatalf("response enqueued = %v, want 1; body=%s", got, rec.Body.String())
	}
	if got := response["total"]; got != float64(2) {
		t.Fatalf("response total = %v, want 2; body=%s", got, rec.Body.String())
	}
	if spool.calls != 2 || spool.success != 1 {
		t.Fatalf("spool calls/success = %d/%d, want 2/1", spool.calls, spool.success)
	}
}

func TestSidecarBatch_RejectsOversizedTopicBeforeSpooling(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body, err := json.Marshal([]EnqueueRequest{
		{Topic: "ok", Payload: json.RawMessage(`"one"`)},
		{Topic: strings.Repeat("t", walspool.MaxTopicBytes+1), Payload: json.RawMessage(`"two"`)},
	})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/enqueue/batch", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if spool.calls != 0 {
		t.Fatalf("spool received %d calls for an invalid batch, want 0", spool.calls)
	}
	var response batchErrorItemResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Index != 1 || response.Error != "invalid_batch_item" {
		t.Fatalf("unexpected error response: %+v", response)
	}
}

func TestSidecarBatch_AcceptsMaxTopicLength(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body, err := json.Marshal([]EnqueueRequest{
		{Topic: strings.Repeat("t", walspool.MaxTopicBytes), Payload: json.RawMessage(`"one"`)},
	})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/enqueue/batch", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if spool.calls != 1 {
		t.Fatalf("spool calls = %d, want 1", spool.calls)
	}
}

func TestSidecarMetrics_OverflowLabelIsReserved(t *testing.T) {
	metrics := NewSidecarMetrics()
	metrics.RecordIngested(overflowTopic)
	metrics.RecordIngested(overflowTopic)
	if got := len(metrics.ingested); got != 1 {
		t.Fatalf("reserved overflow label created %d keys, want 1", got)
	}
	if got := metrics.ingested[overflowTopic]; got != 2 {
		t.Fatalf("overflow count = %d, want 2", got)
	}
}
