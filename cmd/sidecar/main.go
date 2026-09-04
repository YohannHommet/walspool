package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/YohannHommet/walspool"
)

// Build metadata injected via -ldflags during GoReleaser or docker builds.
var (
	Version   = "1.0.0"
	GitCommit = "HEAD"
	BuildDate = "unknown"
)

// HTTPSink forwards drained batches to an external target HTTP endpoint.
type HTTPSink struct {
	TargetURL  string
	HTTPClient *http.Client
	Metrics    *SidecarMetrics
}

type BatchItem struct {
	Offset    uint64          `json:"offset"`
	ID        uint64          `json:"id"`
	Timestamp int64           `json:"timestamp_nano"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
}

func (s *HTTPSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	if s.TargetURL == "" {
		// If no target sink configured, log to stdout for testing/demo
		for _, rec := range batch {
			slog.Info("[SINK CONSOLE] Shipped", "offset", rec.Offset, "topic", rec.Topic, "payload", string(rec.Payload))
			if s.Metrics != nil {
				s.Metrics.RecordDelivered(rec.Topic)
			}
		}
		return nil
	}

	items := make([]BatchItem, len(batch))
	for i, r := range batch {
		// Try to preserve raw JSON if payload is valid JSON, else quote as string
		var raw json.RawMessage
		if json.Valid(r.Payload) {
			raw = json.RawMessage(r.Payload)
		} else {
			quoted, _ := json.Marshal(string(r.Payload))
			raw = json.RawMessage(quoted)
		}

		items[i] = BatchItem{
			Offset:    uint64(r.Offset),
			ID:        r.ID,
			Timestamp: r.Timestamp.UnixNano(),
			Topic:     r.Topic,
			Payload:   raw,
		}
	}

	body, err := json.Marshal(items)
	if err != nil {
		return fmt.Errorf("%w: failed to encode batch: %v", walspool.ErrPermanentRejection, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.TargetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: invalid request: %v", walspool.ErrPermanentRejection, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "walspool-sidecar/1.0")

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: network transport fault: %v", walspool.ErrSinkUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if s.Metrics != nil {
			for _, rec := range batch {
				s.Metrics.RecordDelivered(rec.Topic)
			}
		}
		return nil
	}

	// HTTP 429 (Too Many Requests) and HTTP 408 (Request Timeout) are transient conditions -> retry with backoff
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		slog.Warn("[SINK TRANSIENT] Downstream transient error", "status", resp.StatusCode, "body", string(respBody))
		return fmt.Errorf("%w: destination returned transient HTTP %d", walspool.ErrSinkUnavailable, resp.StatusCode)
	}

	// 4xx client errors mean downstream rejected this payload format permanently (400, 422, etc.)
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		slog.Error("[SINK 4XX] Downstream permanently rejected batch", "status", resp.StatusCode, "body", string(respBody))
		return walspool.ErrPermanentRejection
	}

	// 5xx server errors indicate transient downstream failure -> trigger exponential backoff
	return fmt.Errorf("%w: destination returned HTTP %d", walspool.ErrSinkUnavailable, resp.StatusCode)
}

// SidecarMetrics collects operational counters and gauges for Prometheus exposition.
type SidecarMetrics struct {
	mu        sync.RWMutex
	ingested  map[string]uint64
	delivered map[string]uint64
}

// NewSidecarMetrics constructs a new thread-safe SidecarMetrics registry.
func NewSidecarMetrics() *SidecarMetrics {
	return &SidecarMetrics{
		ingested:  make(map[string]uint64),
		delivered: make(map[string]uint64),
	}
}

// RecordIngested increments the counter for a given topic.
func (m *SidecarMetrics) RecordIngested(topic string) {
	m.mu.Lock()
	m.ingested[topic]++
	m.mu.Unlock()
}

// RecordDelivered increments the delivered counter for a given topic.
func (m *SidecarMetrics) RecordDelivered(topic string) {
	m.mu.Lock()
	m.delivered[topic]++
	m.mu.Unlock()
}

// Ingested returns the current ingested count for a topic.
func (m *SidecarMetrics) Ingested(topic string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ingested[topic]
}

// Delivered returns the current delivered count for a topic.
func (m *SidecarMetrics) Delivered(topic string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.delivered[topic]
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// FormatPrometheus formats all collected metrics according to the Prometheus text-based exposition standard.
func (m *SidecarMetrics) FormatPrometheus(hub walspool.LogHub, storage walspool.StorageEngine) string {
	var activeSSE int
	var droppedEvents uint64
	var ringCap int
	var ringCount int
	if hub != nil {
		stats := hub.Stats()
		activeSSE = stats.ActiveStreams
		droppedEvents = stats.DroppedEvents
		ringCap = stats.Capacity
		ringCount = stats.CurrentSize
	}

	uncommitted := 0
	if storage != nil {
		if count, err := storage.UncommittedCount(); err == nil {
			uncommitted = count
		}
	}

	m.mu.RLock()
	ingestedKeys := make([]string, 0, len(m.ingested))
	for k := range m.ingested {
		ingestedKeys = append(ingestedKeys, k)
	}
	sort.Strings(ingestedKeys)

	deliveredKeys := make([]string, 0, len(m.delivered))
	for k := range m.delivered {
		deliveredKeys = append(deliveredKeys, k)
	}
	sort.Strings(deliveredKeys)

	var buf bytes.Buffer

	// walspool_ingested_records_total
	buf.WriteString("# HELP walspool_ingested_records_total Total number of records ingested by topic\n")
	buf.WriteString("# TYPE walspool_ingested_records_total counter\n")
	for _, k := range ingestedKeys {
		fmt.Fprintf(&buf, "walspool_ingested_records_total{topic=\"%s\"} %d\n", escapeLabelValue(k), m.ingested[k])
	}

	// walspool_delivered_records_total
	buf.WriteString("# HELP walspool_delivered_records_total Total number of delivered records to sink by topic\n")
	buf.WriteString("# TYPE walspool_delivered_records_total counter\n")
	for _, k := range deliveredKeys {
		fmt.Fprintf(&buf, "walspool_delivered_records_total{topic=\"%s\"} %d\n", escapeLabelValue(k), m.delivered[k])
	}
	m.mu.RUnlock()

	// walspool_active_sse_subscribers
	buf.WriteString("# HELP walspool_active_sse_subscribers Current number of active SSE streaming subscribers\n")
	buf.WriteString("# TYPE walspool_active_sse_subscribers gauge\n")
	fmt.Fprintf(&buf, "walspool_active_sse_subscribers %d\n", activeSSE)

	// walspool_uncommitted_records
	buf.WriteString("# HELP walspool_uncommitted_records Current number of uncommitted records pending delivery in storage\n")
	buf.WriteString("# TYPE walspool_uncommitted_records gauge\n")
	fmt.Fprintf(&buf, "walspool_uncommitted_records %d\n", uncommitted)

	// walspool_dropped_events_total
	buf.WriteString("# HELP walspool_dropped_events_total Total dropped events due to hub ring buffer overflow\n")
	buf.WriteString("# TYPE walspool_dropped_events_total counter\n")
	fmt.Fprintf(&buf, "walspool_dropped_events_total %d\n", droppedEvents)

	// walspool_ring_buffer_capacity
	buf.WriteString("# HELP walspool_ring_buffer_capacity Maximum capacity of log hub ring buffer\n")
	buf.WriteString("# TYPE walspool_ring_buffer_capacity gauge\n")
	fmt.Fprintf(&buf, "walspool_ring_buffer_capacity %d\n", ringCap)

	// walspool_ring_buffer_count
	buf.WriteString("# HELP walspool_ring_buffer_count Current count of log entries in ring buffer\n")
	buf.WriteString("# TYPE walspool_ring_buffer_count gauge\n")
	fmt.Fprintf(&buf, "walspool_ring_buffer_count %d\n", ringCount)

	return buf.String()
}

// SidecarServer exposes an HTTP API for polyglot services (Python, Node, Ruby, Rust, etc.).
// It integrates disk WAL spooling with in-memory log indexing and real-time SSE streaming.
type SidecarServer struct {
	spooler      walspool.Spooler
	hub          walspool.LogHub
	storage      walspool.StorageEngine
	metrics      *SidecarMetrics
	shuttingDown atomic.Bool
}

// NewSidecarServer constructs a SidecarServer. Optional LogHub can be injected (e.g. for testing).
func NewSidecarServer(spooler walspool.Spooler, hub ...walspool.LogHub) *SidecarServer {
	var h walspool.LogHub
	if len(hub) > 0 && hub[0] != nil {
		h = hub[0]
	} else {
		h = walspool.NewMemoryLogHub(walspool.DefaultHubCapacity)
	}
	return &SidecarServer{
		spooler: spooler,
		hub:     h,
		metrics: NewSidecarMetrics(),
	}
}

// WithStorage sets the underlying storage engine for readiness checks and metrics collection.
func (s *SidecarServer) WithStorage(storage walspool.StorageEngine) *SidecarServer {
	s.storage = storage
	return s
}

// WithMetrics injects a custom metrics registry into the server.
func (s *SidecarServer) WithMetrics(metrics *SidecarMetrics) *SidecarServer {
	if metrics != nil {
		s.metrics = metrics
	}
	return s
}

// Hub returns the underlying LogHub instance.
func (s *SidecarServer) Hub() walspool.LogHub {
	return s.hub
}

// Metrics returns the underlying SidecarMetrics instance.
func (s *SidecarServer) Metrics() *SidecarMetrics {
	return s.metrics
}

// Storage returns the underlying StorageEngine instance.
func (s *SidecarServer) Storage() walspool.StorageEngine {
	return s.storage
}

// MarkShuttingDown marks the sidecar as undergoing graceful shutdown.
func (s *SidecarServer) MarkShuttingDown() {
	s.shuttingDown.Store(true)
}

// IsShuttingDown returns true if graceful shutdown has started.
func (s *SidecarServer) IsShuttingDown() bool {
	return s.shuttingDown.Load()
}

type EnqueueRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
	TraceID string          `json:"trace_id,omitempty"`
	Service string          `json:"service,omitempty"`
	Level   string          `json:"level,omitempty"`
}

type EnqueueResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
	Size   int    `json:"size_bytes"`
}

func (s *SidecarServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/enqueue", s.handleEnqueue)
	mux.HandleFunc("/v1/enqueue", s.handleEnqueue)
	mux.HandleFunc("/flush", s.handleFlush)
	mux.HandleFunc("/v1/logs", s.handleLogsQuery)
	mux.HandleFunc("/v1/logs/stream", s.handleLogsStream)
	mux.HandleFunc("/v1/logs/stats", s.handleLogsStats)
	// Convenience aliases
	mux.HandleFunc("/logs", s.handleLogsQuery)
	mux.HandleFunc("/logs/stream", s.handleLogsStream)
	mux.HandleFunc("/logs/stats", s.handleLogsStats)
	mux.HandleFunc("/v1/metrics", s.handleMetrics)
	return mux
}

func (s *SidecarServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","engine":"walspool"}` + "\n"))
}

func (s *SidecarServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	if s.IsShuttingDown() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"shutting_down","ready":false}` + "\n"))
		return
	}

	if s.storage != nil {
		if _, err := s.storage.UncommittedCount(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"storage_unavailable","ready":false,"error":%q}`+"\n", err.Error())))
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready","ready":true}` + "\n"))
}

func (s *SidecarServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if s.metrics != nil {
		output := s.metrics.FormatPrometheus(s.hub, s.storage)
		_, _ = w.Write([]byte(output))
	}
}

func (s *SidecarServer) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 2*1024*1024))
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}

	var req EnqueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":"invalid json payload"}`, http.StatusBadRequest)
		return
	}

	// Header trace_id/service/level are hub-indexed only for JSON-object payloads; non-JSON payloads stay byte-identical and unindexed.
	if req.TraceID == "" {
		req.TraceID = strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	}
	if req.Service == "" {
		req.Service = strings.TrimSpace(r.Header.Get("X-Service"))
	}
	if req.Level == "" {
		req.Level = strings.TrimSpace(r.Header.Get("X-Log-Level"))
	}

	if req.Topic == "" && req.Service != "" {
		req.Topic = req.Service
	}

	if req.Topic == "" {
		http.Error(w, `{"error":"topic cannot be empty"}`, http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		http.Error(w, `{"error":"payload cannot be empty"}`, http.StatusBadRequest)
		return
	}

	// If top-level metadata was supplied in EnqueueRequest and payload is a JSON object,
	// ensure payload retains it so WAL log and IngestionObserver receive complete metadata.
	payloadBytes := []byte(req.Payload)
	if req.TraceID != "" || req.Service != "" || req.Level != "" {
		trimmed := bytes.TrimSpace(payloadBytes)
		if len(trimmed) > 0 && trimmed[0] == '{' {
			// UseNumber avoids float64 rounding of 64-bit trace IDs / nanosecond timestamps on re-marshal.
			decoder := json.NewDecoder(bytes.NewReader(trimmed))
			decoder.UseNumber()
			var obj map[string]any
			if decoder.Decode(&obj) == nil && obj != nil {
				modified := false
				if req.TraceID != "" && obj["trace_id"] == nil && obj["traceId"] == nil {
					obj["trace_id"] = req.TraceID
					modified = true
				}
				if req.Service != "" && obj["service"] == nil {
					obj["service"] = req.Service
					modified = true
				}
				if req.Level != "" && obj["level"] == nil && obj["severity"] == nil {
					obj["level"] = req.Level
					modified = true
				}
				if modified {
					if merged, err := json.Marshal(obj); err == nil {
						payloadBytes = merged
					}
				}
			}
		}
	}

	// 1. Sub-microsecond append to local disk WAL with CRC32 integrity
	// Persists on WAL and automatically notifies registered IngestionObserver (e.g. MemoryLogHub)
	err = s.spooler.Enqueue(r.Context(), req.Topic, payloadBytes)
	if err != nil {
		if errors.Is(err, walspool.ErrSpoolFull) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"spool_full","message":"storage quota exceeded, backpressure active"}` + "\n"))
			return
		}
		if errors.Is(err, walspool.ErrPreconditionViolated) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"precondition_failed","message":%q}`+"\n", err.Error())))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"internal_error","message":%q}`+"\n", err.Error())))
		return
	}

	if s.metrics != nil {
		s.metrics.RecordIngested(req.Topic)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(EnqueueResponse{
		Status: "accepted",
		Topic:  req.Topic,
		Size:   len(payloadBytes),
	})
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
type SidecarConfig struct {
	Addr          string
	DataDir       string
	TargetSinkURL string
	BatchSize     int
	FlushMs       int
	MaxRecords    int
	HubCapacity   int
	LogFormat     string
	LogLevel      string
	ShowVersion   bool
}

// Validate enforces strict invariants across configuration values.
func (c *SidecarConfig) Validate() error {
	if c.ShowVersion {
		return nil
	}
	if strings.TrimSpace(c.Addr) == "" {
		return fmt.Errorf("%w: addr must not be empty", walspool.ErrPreconditionViolated)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("%w: batch-size must be > 0 (got %d)", walspool.ErrPreconditionViolated, c.BatchSize)
	}
	if c.FlushMs <= 0 {
		return fmt.Errorf("%w: flush-ms must be > 0 (got %d)", walspool.ErrPreconditionViolated, c.FlushMs)
	}
	if c.MaxRecords <= 0 {
		return fmt.Errorf("%w: max-records must be > 0 (got %d)", walspool.ErrPreconditionViolated, c.MaxRecords)
	}
	if c.HubCapacity <= 0 {
		return fmt.Errorf("%w: hub-capacity must be > 0 (got %d)", walspool.ErrPreconditionViolated, c.HubCapacity)
	}
	switch strings.ToLower(strings.TrimSpace(c.LogFormat)) {
	case "text", "json":
		// valid
	default:
		return fmt.Errorf("%w: invalid log-format %q (must be text or json)", walspool.ErrPreconditionViolated, c.LogFormat)
	}
	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
	case "debug", "info", "warn", "error":
		// valid
	default:
		return fmt.Errorf("%w: invalid log-level %q (must be debug, info, warn, or error)", walspool.ErrPreconditionViolated, c.LogLevel)
	}
	return nil
}

// ParseConfig resolves configuration by prioritizing CLI flags over environment variables,
// which in turn take precedence over hardcoded defaults (CRIT-07).
func ParseConfig(args []string, lookupEnv func(string) (string, bool)) (*SidecarConfig, error) {
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}

	getEnvStr := func(key, defaultVal string) string {
		if val, ok := lookupEnv(key); ok && val != "" {
			return val
		}
		return defaultVal
	}

	// Fail fast on a malformed numeric env var (operator typo) instead of silently falling back to a default.
	getEnvInt := func(key string, defaultVal int) (int, error) {
		if val, ok := lookupEnv(key); ok && val != "" {
			v, err := strconv.Atoi(val)
			if err != nil {
				return 0, fmt.Errorf("%w: env %s must be an integer (got %q)", walspool.ErrPreconditionViolated, key, val)
			}
			return v, nil
		}
		return defaultVal, nil
	}

	batchSize, err := getEnvInt("WALSPOOL_BATCH_SIZE", 50)
	if err != nil {
		return nil, err
	}
	flushMs, err := getEnvInt("WALSPOOL_FLUSH_MS", 50)
	if err != nil {
		return nil, err
	}
	maxRecords, err := getEnvInt("WALSPOOL_MAX_RECORDS", 50000)
	if err != nil {
		return nil, err
	}
	hubCapacity, err := getEnvInt("WALSPOOL_HUB_CAPACITY", 50000)
	if err != nil {
		return nil, err
	}

	fs := flag.NewFlagSet("walspool-sidecar", flag.ContinueOnError)

	var cfg SidecarConfig
	fs.StringVar(&cfg.Addr, "addr", getEnvStr("WALSPOOL_ADDR", ":9099"), "HTTP bind address")
	fs.StringVar(&cfg.DataDir, "data-dir", getEnvStr("WALSPOOL_DATA_DIR", "./data/spool"), "Spool directory for WAL files")
	fs.StringVar(&cfg.TargetSinkURL, "sink-url", getEnvStr("WALSPOOL_SINK_URL", ""), "Target HTTP URL to deliver batches to")
	fs.IntVar(&cfg.BatchSize, "batch-size", batchSize, "Batch size for background drain")
	fs.IntVar(&cfg.FlushMs, "flush-ms", flushMs, "Flush interval in milliseconds")
	fs.IntVar(&cfg.MaxRecords, "max-records", maxRecords, "Maximum records quota before backpressure reject")
	fs.IntVar(&cfg.HubCapacity, "hub-capacity", hubCapacity, "In-memory ring buffer capacity for logs hub")
	fs.StringVar(&cfg.LogFormat, "log-format", getEnvStr("WALSPOOL_LOG_FORMAT", "text"), "Log output format (text|json)")
	fs.StringVar(&cfg.LogLevel, "log-level", getEnvStr("WALSPOOL_LOG_LEVEL", "info"), "Log level (debug|info|warn|error)")
	fs.BoolVar(&cfg.ShowVersion, "version", false, "Show version information and exit")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// SetupLogger configures the default slog logger based on format and level.
func SetupLogger(format, levelStr string) *slog.Logger {
	return SetupLoggerWithWriter(os.Stdout, format, levelStr)
}

// SetupLoggerWithWriter configures the default slog logger directing output to the given writer.
func SetupLoggerWithWriter(w io.Writer, format, levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	case "info":
		fallthrough
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	if strings.ToLower(strings.TrimSpace(format)) == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// HTTPServer abstracts the shutdown method of an HTTP server for testability.
type HTTPServer interface {
	Shutdown(ctx context.Context) error
}

// ShutdownSignaler notifies an entity that a shutdown sequence has commenced.
type ShutdownSignaler interface {
	MarkShuttingDown()
}

// GracefulShutdown executes the secure 4-step shutdown sequence (CRIT-03, MAJ-07):
// 0. Signal shutting down state to signalers (e.g. SidecarServer readyz probe)
// 1. hub.Close() (closes subscriber channels and immediately unblocks open SSE streams)
// 2. httpServer.Shutdown(ctx) (stops accepting new connections, drains in-flight HTTP requests)
// 3. spool.Flush(ctx) (forces shipment and drain of ALL in-flight logs to the Sink)
// 4. spool.Close() (closes storage engine and stops background dispatcher)
func GracefulShutdown(ctx context.Context, httpServer HTTPServer, hub walspool.LogHub, spool walspool.Spooler, signalers ...ShutdownSignaler) error {
	for _, s := range signalers {
		if s != nil {
			s.MarkShuttingDown()
		}
	}
	if sig, ok := httpServer.(ShutdownSignaler); ok && sig != nil {
		sig.MarkShuttingDown()
	}

	var errs []error

	// 1. Close hub first to unblock open SSE streaming handlers immediately
	if hub != nil {
		if err := hub.Close(); err != nil {
			errs = append(errs, fmt.Errorf("hub close: %w", err))
		}
	}

	// 2. Shutdown HTTP server (will not hang on SSE streams since hub is closed)
	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("http server shutdown: %w", err))
		}
	}

	// 3. Flush spooler to deliver all pending in-flight records to the sink
	if spool != nil {
		if err := spool.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("spooler flush: %w", err))
		}

		// 4. Close spooler storage and dispatcher
		if err := spool.Close(); err != nil {
			errs = append(errs, fmt.Errorf("spooler close: %w", err))
		}
	}

	return errors.Join(errs...)
}

func main() {
	cfg, err := ParseConfig(os.Args[1:], os.LookupEnv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Fatal: invalid configuration: %v\n", err)
		os.Exit(1)
	}

	if cfg.ShowVersion {
		fmt.Printf("walspool-sidecar version %s (commit %s, built %s)\n", Version, GitCommit, BuildDate)
		return
	}

	SetupLogger(cfg.LogFormat, cfg.LogLevel)

	slog.Info("Starting walspool sidecar daemon",
		"version", Version,
		"commit", GitCommit,
		"addr", cfg.Addr,
		"data_dir", cfg.DataDir,
		"target_sink", cfg.TargetSinkURL,
		"batch_size", cfg.BatchSize,
		"flush_ms", cfg.FlushMs,
		"max_records", cfg.MaxRecords,
		"hub_capacity", cfg.HubCapacity,
		"log_format", cfg.LogFormat,
		"log_level", cfg.LogLevel,
	)

	// Initialize disk storage
	storage, err := walspool.NewFileStorageEngine(cfg.DataDir, cfg.MaxRecords)
	if err != nil {
		slog.Error("Fatal: failed to initialize FileStorageEngine", "error", err)
		os.Exit(1)
	}

	metrics := NewSidecarMetrics()

	sink := &HTTPSink{
		TargetURL:  cfg.TargetSinkURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		Metrics:    metrics,
	}

	hub := walspool.NewMemoryLogHub(cfg.HubCapacity)

	spoolCfg := walspool.DefaultConfig()
	spoolCfg.BatchSize = cfg.BatchSize
	spoolCfg.FlushInterval = time.Duration(cfg.FlushMs) * time.Millisecond

	spool, err := walspool.New(spoolCfg, storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		slog.Error("Fatal: failed to initialize walspool engine", "error", err)
		os.Exit(1)
	}

	server := NewSidecarServer(spool, hub).
		WithStorage(storage).
		WithMetrics(metrics)

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		// WriteTimeout is intentionally 0 (disabled) to support long-lived Server-Sent Events (SSE)
	}

	// Handle graceful shutdown on SIGTERM / SIGINT
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Shutting down walspool sidecar (flushing in-flight batches)...")
		server.MarkShuttingDown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := GracefulShutdown(shutdownCtx, httpServer, hub, spool, server); err != nil {
			slog.Error("Error during graceful shutdown", "error", err)
		}
		slog.Info("Sidecar stopped safely.")
		os.Exit(0)
	}()

	slog.Info("walspool sidecar listening",
		"url", fmt.Sprintf("http://localhost%s", cfg.Addr),
		"endpoints", "POST /enqueue, GET /healthz, GET /readyz, GET /metrics, POST /flush, GET /v1/logs, GET /v1/logs/stream",
	)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Server error", "error", err)
		os.Exit(1)
	}
}
