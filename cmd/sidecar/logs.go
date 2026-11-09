package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/YohannHommet/walspool"
)

func (s *SidecarServer) handleLogsRoute(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleLogsQuery(w, r)
	case http.MethodPost:
		if !s.otlpEnabled {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"otlp_disabled","message":"OTLP ingestion is disabled"}` + "\n"))
			return
		}
		s.handleOTLPLogs(w, r)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (s *SidecarServer) handleLogsQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	traceID := strings.TrimSpace(query.Get("trace_id"))
	service := strings.TrimSpace(query.Get("service"))
	level := strings.TrimSpace(query.Get("level"))
	limitStr := strings.TrimSpace(query.Get("limit"))

	limit := 100
	if limitStr != "" {
		if val, err := strconv.Atoi(limitStr); err == nil && val > 0 {
			limit = val
		}
	}

	q := walspool.LogQuery{
		TraceID: traceID,
		Service: service,
		Level:   level,
		Limit:   limit,
	}

	var logs []walspool.LogEntry
	if s.hub != nil {
		logs = s.hub.Query(q)
	}
	if logs == nil {
		logs = make([]walspool.LogEntry, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(logs)
}

func (s *SidecarServer) handleLogsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	if s.hub == nil {
		http.Error(w, "Hub unavailable", http.StatusServiceUnavailable)
		return
	}

	service := strings.TrimSpace(r.URL.Query().Get("service"))
	level := strings.TrimSpace(r.URL.Query().Get("level"))

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	filter := walspool.StreamFilter{
		Service: service,
		Level:   level,
	}

	_, logCh, cancel := s.hub.Subscribe(filter)
	defer cancel()

	ctx := r.Context()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial comment to immediately flush headers and confirm stream
	if _, err := fmt.Fprintf(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case entry, ok := <-logCh:
			if !ok {
				return
			}
			data, err := json.Marshal(entry)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *SidecarServer) handleLogsStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var stats walspool.HubStats
	if s.hub != nil {
		stats = s.hub.Stats()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *SidecarServer) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.spooler.Flush(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"flush_failed","message":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"flushed"}` + "\n"))
}

// SidecarConfig encapsulates validated runtime configuration for the sidecar process.
