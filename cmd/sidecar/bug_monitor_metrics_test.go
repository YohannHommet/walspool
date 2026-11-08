package main

import (
	"strings"
	"testing"

	"github.com/YohannHommet/walspool"
)

type bugMetricStorage struct {
	*walspool.MemoryStorageEngine
}

func (bugMetricStorage) DiskBytes() (int64, error) { return 1234, nil }

func TestBugMonitorMetrics(t *testing.T) {
	metrics := NewSidecarMetrics()
	metrics.RecordSinkFlush()
	metrics.RecordSinkError()
	metrics.RecordSinkRetry()

	storage := bugMetricStorage{walspool.NewMemoryStorageEngine(10)}
	output := metrics.FormatPrometheus(nil, storage)

	required := []string{
		"walspool_storage_disk_bytes",
		"walspool_sink_flushes_total",
		"walspool_sink_errors_total",
		"walspool_sink_retries_total",
		"walspool_storage_disk_bytes 1234",
		"walspool_sink_flushes_total 1",
		"walspool_sink_errors_total 1",
		"walspool_sink_retries_total 1",
	}
	for _, want := range required {
		if !strings.Contains(output, want) {
			t.Errorf("sidecar /metrics output is missing %q, which cmd/walspool/monitor.go parses and renders", want)
		}
	}
}
