package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

func TestParsePrometheusMetrics(t *testing.T) {
	sample := `# HELP walspool_uncommitted_records Current number
# TYPE walspool_uncommitted_records gauge
walspool_uncommitted_records 342
# HELP walspool_storage_disk_bytes Total size
# TYPE walspool_storage_disk_bytes gauge
walspool_storage_disk_bytes 10485760
# HELP walspool_delivered_records_total Delivered count
walspool_delivered_records_total{topic="t1"} 500
walspool_delivered_records_total{topic="t2"} 700
# HELP walspool_sink_flushes_total Flushes
walspool_sink_flushes_total 85
# HELP walspool_sink_errors_total Errors
walspool_sink_errors_total 2
# HELP walspool_sink_retries_total Retries
walspool_sink_retries_total 5
`

	snap := parsePrometheusMetrics(sample)
	if snap.uncommittedRecords != 342 {
		t.Errorf("uncommittedRecords mismatch: got %d, want 342", snap.uncommittedRecords)
	}
	if snap.storageDiskBytes != 10485760 {
		t.Errorf("storageDiskBytes mismatch: got %d, want 10485760", snap.storageDiskBytes)
	}
	if snap.deliveredTotal != 1200 { // 500 + 700
		t.Errorf("deliveredTotal mismatch: got %d, want 1200", snap.deliveredTotal)
	}
	if snap.sinkFlushesTotal != 85 {
		t.Errorf("sinkFlushesTotal mismatch: got %d, want 85", snap.sinkFlushesTotal)
	}
	if snap.sinkErrorsTotal != 2 {
		t.Errorf("sinkErrorsTotal mismatch: got %d, want 2", snap.sinkErrorsTotal)
	}
	if snap.sinkRetriesTotal != 5 {
		t.Errorf("sinkRetriesTotal mismatch: got %d, want 5", snap.sinkRetriesTotal)
	}
}

func TestRenderSparkline(t *testing.T) {
	if s := RenderSparkline(nil, 10); s != "" {
		t.Errorf("expected empty string for nil samples, got %q", s)
	}

	samples := []float64{0, 10, 20, 30, 40, 50, 60, 70}
	spark := RenderSparkline(samples, 8)
	if len([]rune(spark)) != 8 {
		t.Errorf("expected 8 runes in sparkline, got %d", len([]rune(spark)))
	}
	// Sparkline should ascend from lower block to upper block
	runes := []rune(spark)
	if runes[0] != ' ' || runes[len(runes)-1] != '█' {
		t.Errorf("expected ascending sparkline from ' ' to '█', got %q", spark)
	}

	// Flat samples (all equal)
	flat := []float64{50, 50, 50, 50}
	sparkFlat := RenderSparkline(flat, 4)
	if len([]rune(sparkFlat)) != 4 {
		t.Errorf("expected 4 runes, got %d", len([]rune(sparkFlat)))
	}
}

func TestRenderProgressBar(t *testing.T) {
	// 50% on width 20
	p50 := RenderProgressBar(500, 1000, 20)
	if !strings.Contains(p50, "50.0% (500 / 1000)") {
		t.Errorf("progress bar 50%% mismatch: %s", p50)
	}

	// 100%
	p100 := RenderProgressBar(1000, 1000, 20)
	if !strings.Contains(p100, "100.0% (1000 / 1000)") {
		t.Errorf("progress bar 100%% mismatch: %s", p100)
	}

	// Zero capacity defense
	pZero := RenderProgressBar(0, 0, 20)
	if !strings.Contains(pZero, "0.0%") {
		t.Errorf("zero capacity mismatch: %s", pZero)
	}
}

func TestFormatBytesAndNumber(t *testing.T) {
	if s := FormatBytes(512); s != "512 B" {
		t.Errorf("512 bytes mismatch: %s", s)
	}
	if s := FormatBytes(1536); s != "1.5 KB" {
		t.Errorf("1536 bytes mismatch: %s", s)
	}
	if s := FormatBytes(10485760); s != "10.0 MB" {
		t.Errorf("10MB mismatch: %s", s)
	}

	if s := FormatNumber(0); s != "0" {
		t.Errorf("0 mismatch: %s", s)
	}
	if s := FormatNumber(999); s != "999" {
		t.Errorf("999 mismatch: %s", s)
	}
	if s := FormatNumber(1234567); s != "1,234,567" {
		t.Errorf("1234567 mismatch: %s", s)
	}
}

func TestRenderFrame_HealthyAndDegraded(t *testing.T) {
	cfg := &monitorConfig{noColor: true}
	var buf bytes.Buffer

	// 1. Degraded when statsErr != nil
	renderFrame(&buf, cfg, "http://localhost:8080", time.Now(), nil, errors.New("connection refused"), nil, 0, 0, nil, false, 80)
	outDegraded := buf.String()
	if !strings.Contains(outDegraded, "DISCONNECTED") || !strings.Contains(outDegraded, "Cannot connect to sidecar") {
		t.Fatalf("expected disconnected state in degraded frame, got: %s", outDegraded)
	}

	// 2. Healthy frame
	buf.Reset()
	stats := &walspool.HubStats{
		Capacity:      5000,
		CurrentSize:   1200,
		TotalIngested: 85000,
		ActiveStreams: 2,
		IndexedTraces: 450,
	}
	snap := &metricsSnapshot{
		uncommittedRecords: 1200,
		storageDiskBytes:   2048576,
		deliveredTotal:     83800,
		sinkFlushesTotal:   210,
	}
	samples := []float64{100, 200, 300, 400}
	renderFrame(&buf, cfg, "http://localhost:8080", time.Now(), stats, nil, snap, 350.5, 345.0, samples, false, 80)
	outHealthy := buf.String()

	if !strings.Contains(outHealthy, "HEALTHY") {
		t.Errorf("expected HEALTHY badge")
	}
	if !strings.Contains(outHealthy, "[INBOUND INGESTION]") {
		t.Errorf("expected [INBOUND INGESTION] section")
	}
	if !strings.Contains(outHealthy, "[BUFFER & STORAGE]") {
		t.Errorf("expected [BUFFER & STORAGE] section")
	}
	if !strings.Contains(outHealthy, "[OUTBOUND DISPATCHER & SINK]") {
		t.Errorf("expected [OUTBOUND DISPATCHER & SINK] section")
	}
}
