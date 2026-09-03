package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/YohannHommet/walspool"
)

// HTTPSink forwards drained batches to an external target HTTP endpoint.
type HTTPSink struct {
	TargetURL  string
	HTTPClient *http.Client
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
			log.Printf("[SINK CONSOLE] Shipped Offset=%d Topic=%s Payload=%s", rec.Offset, rec.Topic, string(rec.Payload))
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
		return nil
	}

	// 4xx client errors mean downstream rejected this payload format permanently
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		log.Printf("[SINK 4XX] Downstream permanently rejected batch (%d): %s", resp.StatusCode, string(respBody))
		return walspool.ErrPermanentRejection
	}

	// 5xx server errors indicate transient downstream failure -> trigger exponential backoff
	return fmt.Errorf("%w: destination returned HTTP %d", walspool.ErrSinkUnavailable, resp.StatusCode)
}

// SidecarServer exposes an HTTP API for polyglot services (Python, Node, Ruby, Rust, etc.).
type SidecarServer struct {
	spooler walspool.Spooler
}

func NewSidecarServer(spooler walspool.Spooler) *SidecarServer {
	return &SidecarServer{spooler: spooler}
}

type EnqueueRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

type EnqueueResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
	Size   int    `json:"size_bytes"`
}

func (s *SidecarServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/enqueue", s.handleEnqueue)
	mux.HandleFunc("/v1/enqueue", s.handleEnqueue)
	mux.HandleFunc("/flush", s.handleFlush)
	return mux
}

func (s *SidecarServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","engine":"walspool"}
`))
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

	if req.Topic == "" {
		http.Error(w, `{"error":"topic cannot be empty"}`, http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		http.Error(w, `{"error":"payload cannot be empty"}`, http.StatusBadRequest)
		return
	}

	// Sub-microsecond append to local disk WAL
	err = s.spooler.Enqueue(r.Context(), req.Topic, []byte(req.Payload))
	if err != nil {
		if errors.Is(err, walspool.ErrSpoolFull) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"spool_full","message":"storage quota exceeded, backpressure active"}
`))
			return
		}
		if errors.Is(err, walspool.ErrPreconditionViolated) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"precondition_failed","message":%q}
`, err.Error())))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"internal_error","message":%q}
`, err.Error())))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(EnqueueResponse{
		Status: "accepted",
		Topic:  req.Topic,
		Size:   len(req.Payload),
	})
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
	_, _ = w.Write([]byte(`{"status":"flushed"}
`))
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func main() {
	var (
		addr          = flag.String("addr", getEnv("WALSPOOL_ADDR", ":9099"), "HTTP bind address")
		dataDir       = flag.String("data-dir", getEnv("WALSPOOL_DATA_DIR", "./data/spool"), "Spool directory for WAL files")
		targetSinkURL = flag.String("sink-url", getEnv("WALSPOOL_SINK_URL", ""), "Target HTTP URL to deliver batches to")
		batchSize     = flag.Int("batch-size", 50, "Batch size for background drain")
		flushMs       = flag.Int("flush-ms", 50, "Flush interval in milliseconds")
		maxRecords    = flag.Int("max-records", 50000, "Maximum records quota before backpressure reject")
	)
	flag.Parse()

	if envBatch := os.Getenv("WALSPOOL_BATCH_SIZE"); envBatch != "" {
		if v, err := strconv.Atoi(envBatch); err == nil {
			*batchSize = v
		}
	}
	if envFlush := os.Getenv("WALSPOOL_FLUSH_MS"); envFlush != "" {
		if v, err := strconv.Atoi(envFlush); err == nil {
			*flushMs = v
		}
	}

	log.Printf("Starting walspool sidecar daemon on %s (data-dir: %s, target-sink: %s)", *addr, *dataDir, *targetSinkURL)

	// Initialize disk storage
	storage, err := walspool.NewFileStorageEngine(*dataDir, *maxRecords)
	if err != nil {
		log.Fatalf("Fatal: failed to initialize FileStorageEngine: %v", err)
	}

	sink := &HTTPSink{
		TargetURL:  *targetSinkURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}

	cfg := walspool.DefaultConfig()
	cfg.BatchSize = *batchSize
	cfg.FlushInterval = time.Duration(*flushMs) * time.Millisecond

	spool, err := walspool.New(cfg, storage, sink, nil)
	if err != nil {
		log.Fatalf("Fatal: failed to initialize walspool engine: %v", err)
	}

	server := NewSidecarServer(spool)
	httpServer := &http.Server{
		Addr:         *addr,
		Handler:      server.Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Handle graceful shutdown on SIGTERM / SIGINT
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down walspool sidecar (flushing in-flight batches)...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = httpServer.Shutdown(ctx)
		_ = spool.Close()
		log.Println("Sidecar stopped safely.")
		os.Exit(0)
	}()

	log.Printf("walspool sidecar listening at http://localhost%s (endpoints: POST /enqueue, GET /healthz, POST /flush)", *addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
