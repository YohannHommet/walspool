package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/YohannHommet/walspool"
)

type SidecarConfig struct {
	Addr             string
	DataDir          string
	TargetSinkURL    string
	BatchSize        int
	FlushMs          int
	MaxRecords       int
	MaxSpoolBytes    int64
	HubCapacity      int
	LogFormat        string
	LogLevel         string
	OTLPEnabled      bool
	OTLPDefaultTopic string
	ShowVersion      bool
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
	// Unlike the library option, 0 does not mean unlimited: disabling the byte quota
	// would reintroduce the unbounded-outage-backlog risk this guard exists to close.
	if c.MaxSpoolBytes <= 0 {
		return fmt.Errorf("%w: max-spool-bytes must be > 0 (got %d)", walspool.ErrPreconditionViolated, c.MaxSpoolBytes)
	}
	if c.HubCapacity <= 0 {
		return fmt.Errorf("%w: hub-capacity must be > 0 (got %d)", walspool.ErrPreconditionViolated, c.HubCapacity)
	}
	if c.OTLPEnabled && strings.TrimSpace(c.OTLPDefaultTopic) == "" {
		return fmt.Errorf("%w: otlp-default-topic must not be empty", walspool.ErrPreconditionViolated)
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
// which in turn take precedence over hardcoded defaults.
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

	// 64-bit variant for byte quotas, which can exceed the int range on 32-bit platforms.
	getEnvInt64 := func(key string, defaultVal int64) (int64, error) {
		if val, ok := lookupEnv(key); ok && val != "" {
			v, err := strconv.ParseInt(val, 10, 64)
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
	maxSpoolBytes, err := getEnvInt64("WALSPOOL_MAX_SPOOL_BYTES", 1<<30)
	if err != nil {
		return nil, err
	}

	otlpDefaultTopic := getEnvStr("WALSPOOL_OTLP_DEFAULT_TOPIC", "otel_logs")
	otlpEnabledStr := getEnvStr("WALSPOOL_OTLP_ENABLED", "true")
	otlpEnabled := true
	if val, err := strconv.ParseBool(otlpEnabledStr); err == nil {
		otlpEnabled = val
	}

	defaultAddr := ":9099"
	if p := getEnvStr("PORT", ""); p != "" {
		if !strings.HasPrefix(p, ":") {
			defaultAddr = ":" + p
		} else {
			defaultAddr = p
		}
	}
	defaultDataDir := getEnvStr("SPOOL_DIR", "./data/spool")
	defaultLogLevel := getEnvStr("LOG_LEVEL", "info")

	fs := flag.NewFlagSet("walspool-sidecar", flag.ContinueOnError)

	var cfg SidecarConfig
	fs.StringVar(&cfg.Addr, "addr", getEnvStr("WALSPOOL_ADDR", defaultAddr), "HTTP bind address")
	fs.StringVar(&cfg.DataDir, "data-dir", getEnvStr("WALSPOOL_DATA_DIR", defaultDataDir), "Spool directory for WAL files")
	fs.StringVar(&cfg.TargetSinkURL, "sink-url", getEnvStr("WALSPOOL_SINK_URL", ""), "Target HTTP URL to deliver batches to")
	fs.IntVar(&cfg.BatchSize, "batch-size", batchSize, "Batch size for background drain")
	fs.IntVar(&cfg.FlushMs, "flush-ms", flushMs, "Flush interval in milliseconds")
	fs.IntVar(&cfg.MaxRecords, "max-records", maxRecords, "Maximum records quota before backpressure reject")
	fs.Int64Var(&cfg.MaxSpoolBytes, "max-spool-bytes", maxSpoolBytes, "Maximum uncommitted WAL bytes before backpressure reject")
	fs.IntVar(&cfg.HubCapacity, "hub-capacity", hubCapacity, "In-memory ring buffer capacity for logs hub")
	fs.StringVar(&cfg.LogFormat, "log-format", getEnvStr("WALSPOOL_LOG_FORMAT", "text"), "Log output format (text|json)")
	fs.StringVar(&cfg.LogLevel, "log-level", getEnvStr("WALSPOOL_LOG_LEVEL", defaultLogLevel), "Log level (debug|info|warn|error)")
	fs.BoolVar(&cfg.OTLPEnabled, "otlp-enabled", otlpEnabled, "Enable OpenTelemetry (OTLP) HTTP log ingestion on POST /v1/logs")
	fs.StringVar(&cfg.OTLPDefaultTopic, "otlp-default-topic", otlpDefaultTopic, "Default topic for OTLP logs when service.name is absent")
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

const (
	// MaxBatchSize defines the maximum number of items allowed in a single batch request.
	MaxBatchSize = 1000

	// MaxBatchBytes defines the maximum raw HTTP body size allowed (10MB).
	MaxBatchBytes = 10 * 1024 * 1024

	// MaxPayloadSize is the shared default per-record payload bound.
	MaxPayloadSize = walspool.DefaultMaxPayloadSize

	// maxSingleBodyBytes bounds the whole single-record HTTP body, including JSON overhead.
	maxSingleBodyBytes = 2 * 1024 * 1024

	// maxTrackedTopics caps per-topic Prometheus series. One slot is reserved
	// for the overflow bucket so attacker-controlled topic cardinality cannot
	// grow the maps without bound.
	maxTrackedTopics = 1000

	// maxTrackedTopicLength caps label memory per topic. Longer topics are
	// aggregated into overflowTopic instead of becoming full-size map keys.
	maxTrackedTopicLength = 128

	overflowTopic = "__walspool_overflow__"
)

// defaultSidecarMaxSegmentSize is the WAL segment rotation threshold used by the sidecar.
// It bounds committed retention and recovery time without adding a -max-segment-size flag;
// the library default stays 0 for ADR-001 compatibility. See ADR-004.
const defaultSidecarMaxSegmentSize int64 = 64 * 1024 * 1024

// sidecarStorageOptions builds the file storage options used by the sidecar daemon: 64 MiB
// segments for committed retention plus a cap on uncommitted serialized WAL frame bytes.
func sidecarStorageOptions(maxSpoolBytes int64) walspool.FileStorageOptions {
	opts := walspool.DefaultFileStorageOptions()
	opts.MaxSegmentSize = defaultSidecarMaxSegmentSize
	opts.MaxUncommittedBytes = maxSpoolBytes
	return opts
}

// BatchEnqueueResponse represents the formal response returned upon successful batch ingestion.
