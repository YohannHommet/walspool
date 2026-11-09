package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewClient_URLNormalization(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"127.0.0.1:8080", "http://127.0.0.1:8080", false},
		{"localhost:9090/", "http://localhost:9090", false},
		{"https://logs.internal:443", "https://logs.internal:443", false},
		{"", "http://127.0.0.1:9099", false},
		{"ftp://invalid.com", "", true},
		{"file:///etc/passwd", "", true},
	}

	for _, tt := range tests {
		client, err := NewClient(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("expected error for input %q, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for input %q: %v", tt.input, err)
		}
		if client.BaseURL != tt.want {
			t.Fatalf("expected BaseURL %q, got %q", tt.want, client.BaseURL)
		}
	}
}

func TestClient_FetchStatsAndMetrics(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/logs/stats":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"capacity":5000,"current_size":42,"total_ingested":1000,"active_streams":1,"dropped_events":0}`)
		case "/metrics":
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, "# HELP walspool_uncommitted_records ...\nwalspool_uncommitted_records 42\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()

	stats, err := client.FetchStats(ctx)
	if err != nil {
		t.Fatalf("FetchStats failed: %v", err)
	}
	if stats.Capacity != 5000 || stats.CurrentSize != 42 || stats.TotalIngested != 1000 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	metrics, err := client.FetchMetrics(ctx)
	if err != nil {
		t.Fatalf("FetchMetrics failed: %v", err)
	}
	if metrics == "" || !strings.Contains(metrics, "walspool_uncommitted_records 42") {
		t.Fatalf("unexpected metrics: %s", metrics)
	}
}

func containsSubstr(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestDefaultAddr(t *testing.T) {
	// Without env override
	t.Setenv("WALSPOOL_ADDR", "")
	if got := defaultAddr(); got != "http://127.0.0.1:9099" {
		t.Fatalf("expected defaultAddr() without env to be %q, got %q", "http://127.0.0.1:9099", got)
	}

	// With env override
	t.Setenv("WALSPOOL_ADDR", "http://custom-sidecar:9199")
	if got := defaultAddr(); got != "http://custom-sidecar:9199" {
		t.Fatalf("expected defaultAddr() with env to be %q, got %q", "http://custom-sidecar:9199", got)
	}
}
