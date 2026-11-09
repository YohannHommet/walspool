package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/YohannHommet/walspool"
)

type SidecarMetrics struct {
	mu        sync.RWMutex
	ingested  map[string]uint64
	delivered map[string]uint64
	sinkFlush uint64
	sinkError uint64
	sinkRetry uint64
}

// NewSidecarMetrics constructs a new thread-safe SidecarMetrics registry.
func NewSidecarMetrics() *SidecarMetrics {
	return &SidecarMetrics{
		ingested:  make(map[string]uint64),
		delivered: make(map[string]uint64),
	}
}

// metricTopicLabel bounds metric label memory and cardinality. Very long
// attacker-controlled topics are aggregated rather than retained as map keys.
func metricTopicLabel(topic string) string {
	if len(topic) > maxTrackedTopicLength {
		return overflowTopic
	}
	return topic
}

// recordTopic inserts or increments one bounded topic counter. One slot is
// reserved for overflowTopic, so the map never exceeds maxTrackedTopics.
func recordTopic(counts map[string]uint64, topic string) {
	label := metricTopicLabel(topic)
	if _, exists := counts[label]; !exists && len(counts) >= maxTrackedTopics-1 {
		label = overflowTopic
	}
	counts[label]++
}

// RecordIngested increments the counter for a given topic.
func (m *SidecarMetrics) RecordIngested(topic string) {
	m.mu.Lock()
	recordTopic(m.ingested, topic)
	m.mu.Unlock()
}

// RecordDelivered increments the delivered counter for a given topic.
func (m *SidecarMetrics) RecordDelivered(topic string) {
	m.mu.Lock()
	recordTopic(m.delivered, topic)
	m.mu.Unlock()
}

// RecordSinkFlush increments the number of outbound delivery attempts.
func (m *SidecarMetrics) RecordSinkFlush() {
	m.mu.Lock()
	m.sinkFlush++
	m.mu.Unlock()
}

// RecordSinkError increments the number of failed outbound delivery attempts.
func (m *SidecarMetrics) RecordSinkError() {
	m.mu.Lock()
	m.sinkError++
	m.mu.Unlock()
}

// RecordSinkRetry increments the number of failures that trigger a dispatcher retry.
func (m *SidecarMetrics) RecordSinkRetry() {
	m.mu.Lock()
	m.sinkRetry++
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

	diskBytes := int64(0)
	uncommittedBytes := int64(0)
	if storage != nil {
		if sizer, ok := storage.(walspool.StorageStats); ok {
			if n, err := sizer.DiskBytes(); err == nil {
				diskBytes = n
			}
		}
		if sizer, ok := storage.(walspool.StorageByteStats); ok {
			uncommittedBytes = sizer.UncommittedBytes()
		}
	}

	type topicCount struct {
		name  string
		count uint64
	}

	m.mu.RLock()
	ingested := make([]topicCount, 0, len(m.ingested))
	for name, count := range m.ingested {
		ingested = append(ingested, topicCount{name: name, count: count})
	}
	delivered := make([]topicCount, 0, len(m.delivered))
	for name, count := range m.delivered {
		delivered = append(delivered, topicCount{name: name, count: count})
	}
	sinkFlush := m.sinkFlush
	sinkError := m.sinkError
	sinkRetry := m.sinkRetry
	m.mu.RUnlock()

	// Sort and render from the snapshot, never while holding the metrics lock.
	sort.Slice(ingested, func(i, j int) bool { return ingested[i].name < ingested[j].name })
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].name < delivered[j].name })

	var buf bytes.Buffer

	// walspool_ingested_records_total
	buf.WriteString("# HELP walspool_ingested_records_total Total number of records ingested by topic\n")
	buf.WriteString("# TYPE walspool_ingested_records_total counter\n")
	for _, topic := range ingested {
		fmt.Fprintf(&buf, "walspool_ingested_records_total{topic=\"%s\"} %d\n", escapeLabelValue(topic.name), topic.count)
	}

	// walspool_delivered_records_total
	buf.WriteString("# HELP walspool_delivered_records_total Total number of delivered records to sink by topic\n")
	buf.WriteString("# TYPE walspool_delivered_records_total counter\n")
	for _, topic := range delivered {
		fmt.Fprintf(&buf, "walspool_delivered_records_total{topic=\"%s\"} %d\n", escapeLabelValue(topic.name), topic.count)
	}

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

	// walspool_storage_disk_bytes
	buf.WriteString("# HELP walspool_storage_disk_bytes Total size of WAL files on disk in bytes\n")
	buf.WriteString("# TYPE walspool_storage_disk_bytes gauge\n")
	fmt.Fprintf(&buf, "walspool_storage_disk_bytes %d\n", diskBytes)

	// walspool_uncommitted_bytes
	buf.WriteString("# HELP walspool_uncommitted_bytes Serialized WAL bytes not yet committed\n")
	buf.WriteString("# TYPE walspool_uncommitted_bytes gauge\n")
	fmt.Fprintf(&buf, "walspool_uncommitted_bytes %d\n", uncommittedBytes)

	// walspool_sink_flushes_total
	buf.WriteString("# HELP walspool_sink_flushes_total Total outbound sink delivery attempts\n")
	buf.WriteString("# TYPE walspool_sink_flushes_total counter\n")
	fmt.Fprintf(&buf, "walspool_sink_flushes_total %d\n", sinkFlush)

	// walspool_sink_errors_total
	buf.WriteString("# HELP walspool_sink_errors_total Total failed outbound sink delivery attempts\n")
	buf.WriteString("# TYPE walspool_sink_errors_total counter\n")
	fmt.Fprintf(&buf, "walspool_sink_errors_total %d\n", sinkError)

	// walspool_sink_retries_total
	buf.WriteString("# HELP walspool_sink_retries_total Total sink failures that triggered a dispatcher retry\n")
	buf.WriteString("# TYPE walspool_sink_retries_total counter\n")
	fmt.Fprintf(&buf, "walspool_sink_retries_total %d\n", sinkRetry)

	return buf.String()
}

// SidecarServer exposes an HTTP API for polyglot services (Python, Node, Ruby, Rust, etc.).
// It integrates disk WAL spooling with in-memory log indexing and real-time SSE streaming.

// FormatEngineStats renders additive core dispatcher counters for /metrics.
func FormatEngineStats(stats walspool.EngineStats) string {
	var buf bytes.Buffer
	write := func(name, help, mtype string, value uint64) {
		fmt.Fprintf(&buf, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, mtype, name, value)
	}
	write("walspool_engine_enqueued_total", "Records accepted by the engine", "counter", stats.Enqueued)
	write("walspool_engine_append_errors_total", "Storage append errors", "counter", stats.AppendErrors)
	write("walspool_engine_spool_full_total", "Enqueue requests rejected by capacity", "counter", stats.SpoolFull)
	write("walspool_engine_committed_records_total", "Records committed to storage", "counter", stats.CommittedRecords)
	write("walspool_engine_delivery_attempts_total", "Sink delivery batches attempted", "counter", stats.DeliveryAttempts)
	write("walspool_engine_retries_total", "Dispatcher retry cycles", "counter", stats.Retries)
	write("walspool_engine_sink_errors_total", "Sink delivery errors", "counter", stats.SinkErrors)
	write("walspool_engine_permanent_rejections_total", "Batches permanently rejected by the sink", "counter", stats.PermanentRejections)
	write("walspool_engine_corrupt_skips_total", "Corrupt WAL records skipped", "counter", stats.CorruptSkips)
	return buf.String()
}
