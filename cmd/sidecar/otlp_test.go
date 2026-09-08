package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// mockOTLPSink is a lightweight test double for walspool.Sink.
type mockOTLPSink struct {
	deliverFunc func(ctx context.Context, batch []walspool.Record) error
}

func (m *mockOTLPSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	if m.deliverFunc != nil {
		return m.deliverFunc(ctx, batch)
	}
	return nil
}

// mockSpooler is a lightweight test double for walspool.Spooler.
type mockSpooler struct {
	enqueueFunc func(ctx context.Context, topic string, payload []byte) error
	flushFunc   func(ctx context.Context) error
	closeFunc   func() error
}

func (m *mockSpooler) Enqueue(ctx context.Context, topic string, payload []byte) error {
	if m.enqueueFunc != nil {
		return m.enqueueFunc(ctx, topic, payload)
	}
	return nil
}

func (m *mockSpooler) Flush(ctx context.Context) error {
	if m.flushFunc != nil {
		return m.flushFunc(ctx)
	}
	return nil
}

func (m *mockSpooler) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestOTLP_Protobuf_Ingestion(t *testing.T) {
	memStore := walspool.NewMemoryStorageEngine(10000)
	hub := walspool.NewMemoryLogHub(1000)
	sink := &mockOTLPSink{}

	spoolCfg := walspool.DefaultConfig()
	spool, err := walspool.New(spoolCfg, memStore, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to create spooler: %v", err)
	}
	defer spool.Close()

	server := NewSidecarServer(spool, hub)
	handler := server.Routes()

	traceIDHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceIDBytes, _ := hex.DecodeString(traceIDHex)
	spanIDHex := "00f067aa0ba902b7"
	spanIDBytes, _ := hex.DecodeString(spanIDHex)

	now := time.Now().UTC()
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key:   "service.name",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "billing-service"}},
						},
						{
							Key:   "deployment.environment",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "production"}},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "github.com/myorg/billing",
							Version: "v1.2.3",
						},
						LogRecords: []*logspb.LogRecord{
							{
								TimeUnixNano:   uint64(now.UnixNano()),
								SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
								SeverityText:   "ERROR",
								Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Payment gateway timeout after 5000ms"}},
								TraceId:        traceIDBytes,
								SpanId:         spanIDBytes,
								Attributes: []*commonpb.KeyValue{
									{
										Key:   "customer_id",
										Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "cust_998877"}},
									},
									{
										Key:   "retry_count",
										Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 3}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	protoBody, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal otlp proto: %v", err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(protoBody))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, httpReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/x-protobuf") {
		t.Errorf("expected Content-Type application/x-protobuf, got %s", rec.Header().Get("Content-Type"))
	}

	// Verify that the record was indexed immediately in MemoryLogHub
	query := walspool.LogQuery{TraceID: traceIDHex}
	results := hub.Query(query)
	if len(results) != 1 {
		t.Fatalf("expected 1 log entry indexed by trace_id, got %d", len(results))
	}

	entry := results[0]
	if entry.Service != "billing-service" {
		t.Errorf("expected service 'billing-service', got %q", entry.Service)
	}
	if entry.Level != "ERROR" {
		t.Errorf("expected level 'ERROR', got %q", entry.Level)
	}
	if entry.TraceID != traceIDHex {
		t.Errorf("expected trace_id %q, got %q", traceIDHex, entry.TraceID)
	}

	// Check payload body contains expected text
	if !strings.Contains(string(entry.Payload), "Payment gateway timeout after 5000ms") {
		t.Errorf("expected payload to contain error message, got: %s", string(entry.Payload))
	}
}

func TestOTLP_JSON_Ingestion(t *testing.T) {
	memStore := walspool.NewMemoryStorageEngine(10000)
	hub := walspool.NewMemoryLogHub(1000)
	sink := &mockOTLPSink{}

	spoolCfg := walspool.DefaultConfig()
	spool, err := walspool.New(spoolCfg, memStore, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		t.Fatalf("failed to create spooler: %v", err)
	}
	defer spool.Close()

	server := NewSidecarServer(spool, hub)
	handler := server.Routes()

	jsonBody := `{
		"resourceLogs": [
			{
				"resource": {
					"attributes": [
						{
							"key": "service.name",
							"value": {"stringValue": "auth-service"}
						}
					]
				},
				"scopeLogs": [
					{
						"scope": {"name": "oauth-handler"},
						"logRecords": [
							{
								"timeUnixNano": "1725835200000000000",
								"severityNumber": 9,
								"severityText": "WARN",
								"body": {"stringValue": "Token refresh attempt with expired grant"},
								"traceId": "0102030405060708090a0b0c0d0e0f10",
								"spanId": "0102030405060708",
								"attributes": [
									{
										"key": "user_id",
										"value": {"stringValue": "usr_42"}
									}
								]
							}
						]
					}
				]
			}
		]
	}`

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(jsonBody))
	httpReq.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, httpReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	// Verify query by service
	query := walspool.LogQuery{Service: "auth-service"}
	results := hub.Query(query)
	if len(results) != 1 {
		t.Fatalf("expected 1 log entry for auth-service, got %d", len(results))
	}

	entry := results[0]
	if entry.Level != "WARN" {
		t.Errorf("expected level 'WARN', got %q", entry.Level)
	}
	if entry.TraceID != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("expected trace_id '0102030405060708090a0b0c0d0e0f10', got %q", entry.TraceID)
	}
}

func TestOTLP_SeverityMapping(t *testing.T) {
	tests := []struct {
		num      logspb.SeverityNumber
		expected string
	}{
		{logspb.SeverityNumber_SEVERITY_NUMBER_TRACE, "DEBUG"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG4, "DEBUG"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"},
		{logspb.SeverityNumber_SEVERITY_NUMBER_FATAL, "FATAL"},
		{0, "INFO"},
	}

	for _, tc := range tests {
		got := severityNumberToText(tc.num)
		if got != tc.expected {
			t.Errorf("severityNumberToText(%v) = %q, expected %q", tc.num, got, tc.expected)
		}
	}
}

func TestOTLP_Backpressure_ErrSpoolFull(t *testing.T) {
	mockSpool := &mockSpooler{
		enqueueFunc: func(ctx context.Context, topic string, payload []byte) error {
			return walspool.ErrSpoolFull
		},
	}

	server := NewSidecarServer(mockSpool)
	handler := server.Routes()

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log"}}},
						},
					},
				},
			},
		},
	}
	protoBody, _ := proto.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(protoBody))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, httpReq)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 on ErrSpoolFull, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Errorf("expected Retry-After: 1 header, got %q", rec.Header().Get("Retry-After"))
	}
}

func TestOTLP_Disabled(t *testing.T) {
	memStore := walspool.NewMemoryStorageEngine(10000)
	spool, _ := walspool.New(walspool.DefaultConfig(), memStore, &mockOTLPSink{}, nil)
	defer spool.Close()

	server := NewSidecarServer(spool).WithOTLP(false, "otel_logs")
	handler := server.Routes()

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(`{}`))
	httpReq.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, httpReq)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 Forbidden when OTLP disabled, got %d", rec.Code)
	}
}

func TestOTLP_InvalidDataAndUnsupportedType(t *testing.T) {
	memStore := walspool.NewMemoryStorageEngine(10000)
	spool, _ := walspool.New(walspool.DefaultConfig(), memStore, &mockOTLPSink{}, nil)
	defer spool.Close()

	server := NewSidecarServer(spool)
	handler := server.Routes()

	// 1. Invalid JSON
	reqBadJSON := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(`{corrupt`))
	reqBadJSON.Header.Set("Content-Type", "application/json")
	recBadJSON := httptest.NewRecorder()
	handler.ServeHTTP(recBadJSON, reqBadJSON)
	if recBadJSON.Code != http.StatusBadRequest {
		t.Errorf("expected HTTP 400 on bad JSON, got %d", recBadJSON.Code)
	}

	// 2. Unsupported Content-Type
	reqBadType := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(`<xml>not supported</xml>`))
	reqBadType.Header.Set("Content-Type", "application/xml")
	recBadType := httptest.NewRecorder()
	handler.ServeHTTP(recBadType, reqBadType)
	if recBadType.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected HTTP 415 on unsupported media type, got %d", recBadType.Code)
	}
}

func BenchmarkOTLP_Protobuf_Ingestion(b *testing.B) {
	memStore := walspool.NewMemoryStorageEngine(500000)
	hub := walspool.NewMemoryLogHub(10000)
	spoolCfg := walspool.DefaultConfig()
	spool, err := walspool.New(spoolCfg, memStore, &mockOTLPSink{}, nil, walspool.WithObserver(hub))
	if err != nil {
		b.Fatalf("failed to create spooler: %v", err)
	}
	defer spool.Close()

	server := NewSidecarServer(spool, hub)
	handler := server.Routes()

	traceIDHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceIDBytes, _ := hex.DecodeString(traceIDHex)

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key:   "service.name",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "benchmark-service"}},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								TimeUnixNano: uint64(time.Now().UnixNano()),
								SeverityText: "INFO",
								Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Benchmarking OTLP high throughput log message"}},
								TraceId:      traceIDBytes,
							},
						},
					},
				},
			},
		},
	}
	protoBytes, _ := proto.Marshal(req)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		httpReq := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(protoBytes))
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httpReq)
		if rec.Code != http.StatusOK {
			b.Fatalf("unexpected status: %d", rec.Code)
		}
	}
}
