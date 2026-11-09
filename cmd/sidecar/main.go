package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YohannHommet/walspool"
)

// Build metadata injected via -ldflags during GoReleaser or docker builds.
var (
	Version   = "1.1.0"
	GitCommit = "HEAD"
	BuildDate = "unknown"
)

// HTTPSink forwards drained batches to an external target HTTP endpoint.
const (
	// maxNDJSONLineBytes plafonne strictement chaque ligne a 1 Mo pour eliminer tout risque d'attaque DoS OOM.
	maxNDJSONLineBytes = 1024 * 1024 // 1 MiB

	// ndjsonReaderSize definit la taille du tampon interne du bufio.Reader pour le fast path zero-alloc.
	ndjsonReaderSize = 64 * 1024 // 64 KiB
)

// ndjsonReaderPool mutualise les instances de bufio.Reader dimensionnees a 64 Ko.
type HTTPServer interface {
	Shutdown(ctx context.Context) error
}

// ShutdownSignaler notifies an entity that a shutdown sequence has commenced.
type ShutdownSignaler interface {
	MarkShuttingDown()
}

// GracefulShutdown executes the deterministic 4-step shutdown sequence:
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
		fmt.Printf("walspool community edition version %s (commit %s, built %s) [powered by meow labs]\n", Version, GitCommit, BuildDate)
		return
	}

	SetupLogger(cfg.LogFormat, cfg.LogLevel)

	slog.Info("Starting walspool community sidecar daemon",
		"version", Version,
		"edition", "community",
		"vendor", "meow labs",
		"commit", GitCommit,
		"addr", cfg.Addr,
		"data_dir", cfg.DataDir,
		"target_sink", cfg.TargetSinkURL,
		"batch_size", cfg.BatchSize,
		"flush_ms", cfg.FlushMs,
		"max_records", cfg.MaxRecords,
		"max_spool_bytes", cfg.MaxSpoolBytes,
		"hub_capacity", cfg.HubCapacity,
		"log_format", cfg.LogFormat,
		"log_level", cfg.LogLevel,
		"otlp_enabled", cfg.OTLPEnabled,
		"otlp_default_topic", cfg.OTLPDefaultTopic,
	)

	// Initialize disk storage
	storage, err := walspool.NewFileStorageEngineWithOptions(cfg.DataDir, cfg.MaxRecords, sidecarStorageOptions(cfg.MaxSpoolBytes))
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
	spoolCfg.MaxCapacity = cfg.MaxRecords

	spool, err := walspool.New(spoolCfg, storage, sink, nil, walspool.WithObserver(hub))
	if err != nil {
		slog.Error("Fatal: failed to initialize walspool engine", "error", err)
		os.Exit(1)
	}

	server := NewSidecarServer(spool, hub).
		WithStorage(storage).
		WithMetrics(metrics).
		WithOTLP(cfg.OTLPEnabled, cfg.OTLPDefaultTopic)

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
	shutdownDone := make(chan struct{})

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
		close(shutdownDone)
	}()

	listenURL := cfg.Addr
	if strings.HasPrefix(listenURL, ":") {
		listenURL = "http://localhost" + listenURL
	} else if !strings.HasPrefix(listenURL, "http://") && !strings.HasPrefix(listenURL, "https://") {
		listenURL = "http://" + listenURL
	}

	slog.Info("walspool sidecar listening",
		"url", listenURL,
		"edition", "community",
		"endpoints", "POST /v1/logs (OTLP), GET /v1/logs, GET /v1/logs/stream, POST /enqueue, GET /healthz, GET /readyz, GET /metrics, POST /flush",
	)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Server error", "error", err)
		os.Exit(1)
	}

	// Wait for GracefulShutdown goroutine to finish flushing buffers and closing storage
	<-shutdownDone
}
