package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YohannHommet/walspool"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func doRequest(handler http.Handler, method, path, contentType string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return body
}

func TestNDJSON_ResilientOversizedTopicIsPartialSuccess(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body := strings.Join([]string{
		`{"topic":"ok","payload":{"message":"one"}}`,
		`{"topic":"` + strings.Repeat("x", walspool.MaxTopicBytes+1) + `","payload":{"message":"two"}}`,
	}, "\n")

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/enqueue", "application/x-ndjson", []byte(body))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON(t, rec)
	if resp["status"] != "partial_success" {
		t.Fatalf("status = %v, want partial_success", resp["status"])
	}
	if resp["ingested"] != float64(1) || resp["errors"] != float64(1) {
		t.Fatalf("ingested/errors = %v/%v, want 1/1; body=%s", resp["ingested"], resp["errors"], rec.Body.String())
	}
	first, ok := resp["first_error"].(map[string]any)
	if !ok {
		t.Fatalf("missing first_error: %s", rec.Body.String())
	}
	if first["line"] != float64(2) || first["code"] != "topic_too_long" {
		t.Fatalf("first_error = %v", first)
	}
	if spool.calls != 1 {
		t.Fatalf("spool calls = %d, want 1", spool.calls)
	}
}

func TestNDJSON_StrictOversizedTopicFailsWithProgress(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body := strings.Join([]string{
		`{"topic":"ok","payload":{"message":"one"}}`,
		`{"topic":"` + strings.Repeat("x", walspool.MaxTopicBytes+1) + `","payload":{"message":"two"}}`,
	}, "\n")

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/enqueue?strict=true", "application/x-ndjson", []byte(body))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON(t, rec)
	if resp["ingested"] != float64(1) || resp["line"] != float64(2) || resp["error"] != "topic_too_long" {
		t.Fatalf("progress = %v, want ingested=1 line=2 error=topic_too_long", resp)
	}
}

func TestNDJSON_SpoolFullReturnsRetryAfterAndProgress(t *testing.T) {
	spool := &reviewSpooler{failAt: 2, failErr: walspool.ErrSpoolFull}
	server := NewSidecarServer(spool)
	body := strings.Join([]string{
		`{"topic":"a","payload":{"n":1}}`,
		`{"topic":"b","payload":{"n":2}}`,
	}, "\n")

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/enqueue", "application/x-ndjson", []byte(body))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	resp := decodeJSON(t, rec)
	if resp["enqueued"] != float64(1) || resp["line"] != float64(2) {
		t.Fatalf("progress = %v, want enqueued=1 line=2", resp)
	}
}

func TestSingleEnqueue_SpoolFullReturnsRetryAfterAndProgress(t *testing.T) {
	spool := &reviewSpooler{failAt: 1, failErr: walspool.ErrSpoolFull}
	server := NewSidecarServer(spool)
	body := []byte(`{"topic":"a","payload":{"n":1}}`)

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/enqueue", "application/json", body)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	resp := decodeJSON(t, rec)
	if resp["enqueued"] != float64(0) || resp["total"] != float64(1) {
		t.Fatalf("progress = %v, want enqueued=0 total=1", resp)
	}
}

func otlpRequest(serviceNames ...string) *collogspb.ExportLogsServiceRequest {
	req := &collogspb.ExportLogsServiceRequest{}
	for _, service := range serviceNames {
		req.ResourceLogs = append(req.ResourceLogs, &logspb.ResourceLogs{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
				Key:   "service.name",
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: service}},
			}}},
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
				Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "body"}},
			}}}},
		})
	}
	return req
}

func TestOTLP_OversizedTopicRejectedBeforeAnySpool(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body, err := proto.Marshal(otlpRequest(strings.Repeat("s", walspool.MaxTopicBytes+1)))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/logs", "application/x-protobuf", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if spool.calls != 0 {
		t.Fatalf("spool calls = %d, want 0 (no partial OTLP spool)", spool.calls)
	}
	resp := decodeJSON(t, rec)
	if resp["error"] != "topic_too_long" {
		t.Fatalf("error = %v, want topic_too_long", resp["error"])
	}
}

func TestOTLP_SpoolFullReturnsProgress(t *testing.T) {
	spool := &reviewSpooler{failAt: 2, failErr: walspool.ErrSpoolFull}
	server := NewSidecarServer(spool)
	body, err := proto.Marshal(otlpRequest("a", "b"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/logs", "application/x-protobuf", body)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	resp := decodeJSON(t, rec)
	if resp["enqueued"] != float64(1) || resp["total"] != float64(2) {
		t.Fatalf("progress = %v, want enqueued=1 total=2", resp)
	}
}

func TestOTLP_OversizedBodyReturns413(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body := bytes.Repeat([]byte{0}, MaxOTLPRequestBodyLimit+1)

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/logs", "application/x-protobuf", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", rec.Code, rec.Body.String())
	}
}

func TestSingleEnqueue_OversizedBodyReturns413(t *testing.T) {
	spool := &reviewSpooler{}
	server := NewSidecarServer(spool)
	body := bytes.Repeat([]byte{' '}, maxSingleBodyBytes+1)

	rec := doRequest(server.Routes(), http.MethodPost, "/v1/enqueue", "application/json", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", rec.Code, rec.Body.String())
	}
}
