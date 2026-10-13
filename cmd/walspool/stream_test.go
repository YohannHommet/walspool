package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestMatchesFilter_SeverityRank(t *testing.T) {
	cfg := &streamConfig{level: "WARN"}
	minRank := parseLevelRank(cfg.level)

	events := []struct {
		level string
		want  bool
	}{
		{"DEBUG", false},
		{"INFO", false},
		{"WARN", true},
		{"ERROR", true},
		{"FATAL", true},
	}

	for _, tt := range events {
		ev := &streamEvent{Level: tt.level}
		got := matchesFilter(ev, cfg, minRank)
		if got != tt.want {
			t.Errorf("level %q: got %v, want %v", tt.level, got, tt.want)
		}
	}
}

func TestMatchesFilter_GrepRegex(t *testing.T) {
	re := regexp.MustCompile(`order_\d+_failed`)
	cfg := &streamConfig{grepRegex: re}

	ev1 := &streamEvent{Payload: json.RawMessage(`{"msg": "order_456_failed"}`)}
	ev2 := &streamEvent{Payload: json.RawMessage(`{"msg": "order_success"}`)}

	if !matchesFilter(ev1, cfg, 0) {
		t.Errorf("expected ev1 to match grep regex")
	}
	if matchesFilter(ev2, cfg, 0) {
		t.Errorf("expected ev2 NOT to match grep regex")
	}
}

func TestMatchesFilter_ServiceAndTraceID(t *testing.T) {
	cfg := &streamConfig{
		service: "Payment-API",
		traceID: "trace-abc-123",
	}

	match := &streamEvent{
		Service: "payment-api",
		TraceID: "trace-abc-123",
	}
	diffService := &streamEvent{
		Service: "auth-service",
		TraceID: "trace-abc-123",
	}
	diffTrace := &streamEvent{
		Service: "payment-api",
		TraceID: "trace-xyz-999",
	}

	if !matchesFilter(match, cfg, 0) {
		t.Errorf("expected match to pass filter")
	}
	if matchesFilter(diffService, cfg, 0) {
		t.Errorf("expected diffService to be rejected")
	}
	if matchesFilter(diffTrace, cfg, 0) {
		t.Errorf("expected diffTrace to be rejected")
	}
}

func TestPrintEvent_Formats(t *testing.T) {
	ev := &streamEvent{
		ID:        1,
		Timestamp: time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC),
		Topic:     "logs",
		Service:   "test-svc",
		TraceID:   "tr-123",
		Level:     "ERROR",
		Payload:   json.RawMessage(`{"status":"error"}`),
	}

	// 1. Raw format
	var bufRaw bytes.Buffer
	if err := printEvent(&bufRaw, ev, "raw", true, false); err != nil {
		t.Fatalf("printEvent raw failed: %v", err)
	}
	if strings.TrimSpace(bufRaw.String()) != `{"status":"error"}` {
		t.Fatalf("unexpected raw output: %s", bufRaw.String())
	}

	// 2. JSON format
	var bufJSON bytes.Buffer
	if err := printEvent(&bufJSON, ev, "json", true, false); err != nil {
		t.Fatalf("printEvent json failed: %v", err)
	}
	var parsed streamEvent
	if err := json.Unmarshal(bufJSON.Bytes(), &parsed); err != nil {
		t.Fatalf("json format produced invalid JSON: %v", err)
	}
	if parsed.Service != "test-svc" || parsed.TraceID != "tr-123" {
		t.Fatalf("parsed json mismatch: %+v", parsed)
	}

	// 3. Pretty format with ANSI injection attempt in service
	evMalicious := &streamEvent{
		ID:        2,
		Timestamp: time.Now(),
		Topic:     "logs",
		Service:   "svc\x1b[31mHACK",
		Level:     "INFO",
		Payload:   json.RawMessage(`"clean payload"`),
	}
	var bufPretty bytes.Buffer
	if err := printEvent(&bufPretty, evMalicious, "pretty", true, false); err != nil {
		t.Fatalf("printEvent pretty failed: %v", err)
	}
	outPretty := bufPretty.String()
	if strings.Contains(outPretty, "\x1b[31mHACK") {
		t.Fatalf("pretty output must sanitize raw escape bytes, got: %q", outPretty)
	}
	if !strings.Contains(outPretty, `svc\x1b[31mHACK`) {
		t.Fatalf("expected sanitized escaped sequence in output, got: %q", outPretty)
	}
}

func TestStream_IntegrationMockSSE(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatalf("server does not support Flusher")
		}

		for i := 1; i <= 5; i++ {
			ev := streamEvent{
				ID:        uint64(i),
				Timestamp: time.Now(),
				Topic:     "test",
				Service:   "auth-service",
				Level:     "INFO",
				Payload:   json.RawMessage(fmt.Sprintf(`{"msg":"event-%d"}`, i)),
			}
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer ts.Close()

	args := []string{
		"--addr", ts.URL,
		"--format", "json",
		"--limit", "3",
	}

	err := runStream(args)
	if err != nil {
		t.Fatalf("runStream failed: %v", err)
	}
}
