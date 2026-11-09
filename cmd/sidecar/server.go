package main

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/YohannHommet/walspool"
)

type SidecarServer struct {
	spooler          walspool.Spooler
	hub              walspool.LogHub
	storage          walspool.StorageEngine
	metrics          *SidecarMetrics
	shuttingDown     atomic.Bool
	otlpEnabled      bool
	otlpDefaultTopic string
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
		spooler:          spooler,
		hub:              h,
		metrics:          NewSidecarMetrics(),
		otlpEnabled:      true,
		otlpDefaultTopic: "otel_logs",
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

// WithOTLP configures OpenTelemetry ingestion settings.
func (s *SidecarServer) WithOTLP(enabled bool, defaultTopic string) *SidecarServer {
	s.otlpEnabled = enabled
	if defaultTopic != "" {
		s.otlpDefaultTopic = defaultTopic
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

func (s *SidecarServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/enqueue", s.handleEnqueue)
	mux.HandleFunc("/v1/enqueue", s.handleEnqueue)
	mux.HandleFunc("/enqueue/batch", s.handleEnqueueBatch)
	mux.HandleFunc("/v1/enqueue/batch", s.handleEnqueueBatch)
	mux.HandleFunc("/flush", s.handleFlush)
	mux.HandleFunc("/v1/logs", s.handleLogsRoute)
	mux.HandleFunc("/v1/logs/stream", s.handleLogsStream)
	mux.HandleFunc("/v1/logs/stats", s.handleLogsStats)
	// Convenience aliases
	mux.HandleFunc("/logs", s.handleLogsRoute)
	mux.HandleFunc("/logs/stream", s.handleLogsStream)
	mux.HandleFunc("/logs/stats", s.handleLogsStats)
	mux.HandleFunc("/v1/metrics", s.handleMetrics)
	return s.corsMiddleware(mux)
}

func (s *SidecarServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Trace-ID, X-Service, X-Log-Level")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *SidecarServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok","engine":"walspool"}` + "\n"))
}

func (s *SidecarServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	if s.IsShuttingDown() {
		w.WriteHeader(http.StatusServiceUnavailable)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(`{"status":"shutting_down","ready":false}` + "\n"))
		}
		return
	}

	if s.storage != nil {
		if _, err := s.storage.UncommittedCount(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			if r.Method != http.MethodHead {
				_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"storage_unavailable","ready":false,"error":%q}`+"\n", err.Error())))
			}
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(`{"status":"ready","ready":true}` + "\n"))
	}
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
		if provider, ok := s.spooler.(interface{ Stats() walspool.EngineStats }); ok {
			output += FormatEngineStats(provider.Stats())
		}
		_, _ = w.Write([]byte(output))
	}
}

// enrichPayloadMetadata injects trace_id, service, and level metadata into an existing JSON object payload
// if those keys are not already present. If the payload is not a JSON object or if no metadata
// is supplied, the payload is returned unchanged.
