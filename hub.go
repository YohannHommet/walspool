package walspool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LogEntry represents an observable, indexed log event in the hub.
type LogEntry struct {
	ID        uint64          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Topic     string          `json:"topic"`
	Service   string          `json:"service"`
	TraceID   string          `json:"trace_id,omitempty"`
	Level     string          `json:"level,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

// LogQuery defines the filtering criteria for log retrieval.
type LogQuery struct {
	TraceID string
	Service string
	Level   string
	Limit   int
}

// StreamFilter defines criteria for real-time log event streaming.
type StreamFilter struct {
	Service string
	Level   string
}

// HubStats reports operational metrics for sensing, monitoring, and leak detection.
type HubStats struct {
	Capacity        int    `json:"capacity"`
	CurrentSize     int    `json:"current_size"`
	TotalIngested   uint64 `json:"total_ingested"`
	ActiveStreams   int    `json:"active_streams"`
	IndexedTraces   int    `json:"indexed_traces"`
	IndexedServices int    `json:"indexed_services"`
	DroppedEvents   uint64 `json:"dropped_events"`
}

// LogHub is the Driving (Inbound) Port for in-memory log indexing, fast querying, and streaming.
type LogHub interface {
	// Ingest indexes a log entry into the thread-safe circular ring buffer
	// and broadcasts it to matching stream subscribers.
	// Precondition: topic or service must not be empty.
	Ingest(entry LogEntry) error

	// Query returns a chronologically ordered slice of logs matching query criteria.
	// Postcondition: executes under 1ms.
	Query(q LogQuery) []LogEntry

	// Subscribe creates a real-time event subscription matching the given filter.
	// Returns a unique subscriber ID, a receive-only channel, and an idempotent cancel function.
	Subscribe(filter StreamFilter) (subID uint64, ch <-chan LogEntry, cancel func())

	// Stats returns internal metrics for observability and memory verification.
	Stats() HubStats

	// Close terminates the hub and cleans up active subscriptions.
	Close() error
}

type subscriber struct {
	mu     sync.RWMutex
	id     uint64
	filter StreamFilter
	ch     chan LogEntry
	closed bool
}

// slotLinks stores bidirectional integral cursors for each slot of the ring buffer.
// Memory size: exactly 16 bytes (4 x int32).
// Optimal cache density: exactly 4 structures per 64-byte CPU cache line (64 / 16 = 4).
// Sentinel value: -1 (identifying absence of link or chain boundary).
type slotLinks struct {
	prevTrace   int32
	nextTrace   int32
	prevService int32
	nextService int32
}

// indexSpan maintains navigation boundaries (head and tail) and active count for a key.
// Memory size: exactly 12 bytes (3 x int32), aligned on 4/8 bytes.
// Stored directly by value in Go hash map buckets without heap escape pointer.
type indexSpan struct {
	head  int32
	tail  int32
	count int32
}

// MemoryLogHub is the high-performance in-memory implementation of LogHub.
// It maintains a fixed-size ring buffer with O(1) append and eviction,
// alongside secondary indices by trace_id and service with zero-leak pruning.
type MemoryLogHub struct {
	mu sync.RWMutex

	capacity int
	head     int
	count    int

	seqID         uint64
	totalIngested uint64
	droppedEvents uint64

	entries []*LogEntry
	links   []slotLinks

	byTraceID map[string]indexSpan
	byService map[string]indexSpan

	subscribers map[uint64]*subscriber
	nextSubID   uint64
	closed      bool
}

// DefaultHubCapacity defines the standard ring buffer quota (50,000 logs).
const DefaultHubCapacity = 50000

// NewMemoryLogHub constructs an in-memory thread-safe LogHub.
func NewMemoryLogHub(capacity int) *MemoryLogHub {
	if capacity <= 0 {
		capacity = DefaultHubCapacity
	}
	links := make([]slotLinks, capacity)
	for i := range links {
		links[i] = slotLinks{
			prevTrace:   -1,
			nextTrace:   -1,
			prevService: -1,
			nextService: -1,
		}
	}
	return &MemoryLogHub{
		capacity:    capacity,
		entries:     make([]*LogEntry, capacity),
		links:       links,
		byTraceID:   make(map[string]indexSpan),
		byService:   make(map[string]indexSpan),
		subscribers: make(map[uint64]*subscriber),
	}
}

func copyEntry(item *LogEntry) LogEntry {
	cp := *item
	if len(item.Payload) > 0 {
		cp.Payload = make(json.RawMessage, len(item.Payload))
		copy(cp.Payload, item.Payload)
	}
	return cp
}

func (h *MemoryLogHub) linkTrace(pos int32, traceID string) {
	if traceID == "" {
		h.links[pos].prevTrace = -1
		h.links[pos].nextTrace = -1
		return
	}
	span, exists := h.byTraceID[traceID]
	if !exists {
		h.links[pos].prevTrace = -1
		h.links[pos].nextTrace = -1
		h.byTraceID[traceID] = indexSpan{head: pos, tail: pos, count: 1}
		return
	}
	oldTail := span.tail
	h.links[oldTail].nextTrace = pos
	h.links[pos].prevTrace = oldTail
	h.links[pos].nextTrace = -1
	span.tail = pos
	span.count++
	h.byTraceID[traceID] = span
}

func (h *MemoryLogHub) linkService(pos int32, service string) {
	if service == "" {
		h.links[pos].prevService = -1
		h.links[pos].nextService = -1
		return
	}
	span, exists := h.byService[service]
	if !exists {
		h.links[pos].prevService = -1
		h.links[pos].nextService = -1
		h.byService[service] = indexSpan{head: pos, tail: pos, count: 1}
		return
	}
	oldTail := span.tail
	h.links[oldTail].nextService = pos
	h.links[pos].prevService = oldTail
	h.links[pos].nextService = -1
	span.tail = pos
	span.count++
	h.byService[service] = span
}

func (h *MemoryLogHub) unlinkTrace(pos int32, traceID string) {
	if traceID == "" {
		return
	}
	span, ok := h.byTraceID[traceID]
	if !ok {
		return
	}
	if span.count <= 1 || (span.head == pos && span.tail == pos) {
		delete(h.byTraceID, traceID)
		h.links[pos].prevTrace = -1
		h.links[pos].nextTrace = -1
		return
	}

	prev := h.links[pos].prevTrace
	next := h.links[pos].nextTrace

	if prev != -1 {
		h.links[prev].nextTrace = next
	} else {
		span.head = next
	}

	if next != -1 {
		h.links[next].prevTrace = prev
	} else {
		span.tail = prev
	}

	span.count--
	h.byTraceID[traceID] = span

	h.links[pos].prevTrace = -1
	h.links[pos].nextTrace = -1
}

func (h *MemoryLogHub) unlinkService(pos int32, service string) {
	if service == "" {
		return
	}
	span, ok := h.byService[service]
	if !ok {
		return
	}
	if span.count <= 1 || (span.head == pos && span.tail == pos) {
		delete(h.byService, service)
		h.links[pos].prevService = -1
		h.links[pos].nextService = -1
		return
	}

	prev := h.links[pos].prevService
	next := h.links[pos].nextService

	if prev != -1 {
		h.links[prev].nextService = next
	} else {
		span.head = next
	}

	if next != -1 {
		h.links[next].prevService = prev
	} else {
		span.tail = prev
	}

	span.count--
	h.byService[service] = span

	h.links[pos].prevService = -1
	h.links[pos].nextService = -1
}

// Ingest indexes a log entry into the ring buffer and distributes to matching SSE subscribers.
func (h *MemoryLogHub) Ingest(entry LogEntry) error {
	// Precondition validation (Meyer DbC - caller fault)
	if entry.Topic == "" && entry.Service == "" {
		return fmt.Errorf("%w: entry must specify topic or service", ErrPreconditionViolated)
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrHubClosed
	}

	// Invariant assignment
	if entry.ID == 0 {
		entry.ID = atomic.AddUint64(&h.seqID, 1)
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	if entry.Service == "" {
		entry.Service = entry.Topic
	}
	if entry.Topic == "" {
		entry.Topic = entry.Service
	}
	entry.Level = strings.ToUpper(strings.TrimSpace(entry.Level))
	if entry.Level == "" {
		entry.Level = "INFO"
	}
	entry.Service = strings.ToLower(strings.TrimSpace(entry.Service))
	entry.TraceID = strings.TrimSpace(entry.TraceID)

	// Defensive copy of payload to ensure immutability across subscribers and circular eviction
	if len(entry.Payload) > 0 {
		payloadCopy := make(json.RawMessage, len(entry.Payload))
		copy(payloadCopy, entry.Payload)
		entry.Payload = payloadCopy
	}

	item := &entry
	h.totalIngested++

	// Circular Ring Buffer Eviction: O(1) strict, zero copy, zero allocation
	var pos int32
	if h.count == h.capacity {
		pos = int32(h.head)
		oldItem := h.entries[pos]
		h.entries[pos] = nil // Immediate pointer release for GC

		// Prune secondary indices without slice copy or reallocation
		if oldItem != nil && oldItem.TraceID != "" {
			h.unlinkTrace(pos, oldItem.TraceID)
		}
		if oldItem != nil && oldItem.Service != "" {
			h.unlinkService(pos, oldItem.Service)
		}

		h.entries[pos] = item
		h.head = (h.head + 1) % h.capacity
	} else {
		pos = int32((h.head + h.count) % h.capacity)
		h.entries[pos] = item
		h.count++
	}

	// Update secondary indices in O(1)
	if item.TraceID != "" {
		h.linkTrace(pos, item.TraceID)
	}
	if item.Service != "" {
		h.linkService(pos, item.Service)
	}

	// Copy subscriber snapshot under lock
	var subs []*subscriber
	if len(h.subscribers) > 0 {
		subs = make([]*subscriber, 0, len(h.subscribers))
		for _, sub := range h.subscribers {
			subs = append(subs, sub)
		}
	}

	// Release lock IMMEDIATELY
	h.mu.Unlock()

	// Distribute log outside exclusive lock (non-blocking)
	for _, sub := range subs {
		if sub.filter.Service != "" && !strings.EqualFold(sub.filter.Service, entry.Service) {
			continue
		}
		if sub.filter.Level != "" && !strings.EqualFold(sub.filter.Level, entry.Level) {
			continue
		}

		sub.mu.RLock()
		if !sub.closed {
			select {
			case sub.ch <- entry:
			default:
				atomic.AddUint64(&h.droppedEvents, 1)
			}
		}
		sub.mu.RUnlock()
	}

	return nil
}

// Query retrieves logs matching criteria in chronological order with < 1ms response.
func (h *MemoryLogHub) Query(q LogQuery) []LogEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return nil
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}

	qTraceID := strings.TrimSpace(q.TraceID)
	qService := strings.ToLower(strings.TrimSpace(q.Service))
	qLevel := strings.ToUpper(strings.TrimSpace(q.Level))

	// Determine bounded allocation capacity based on available candidate count and limit
	allocCap := limit
	if qTraceID != "" {
		if span, ok := h.byTraceID[qTraceID]; ok {
			if int(span.count) < allocCap {
				allocCap = int(span.count)
			}
		} else {
			return make([]LogEntry, 0)
		}
	} else if qService != "" {
		if span, ok := h.byService[qService]; ok {
			if int(span.count) < allocCap {
				allocCap = int(span.count)
			}
		} else {
			return make([]LogEntry, 0)
		}
	} else {
		if h.count < allocCap {
			allocCap = h.count
		}
	}

	// Stack buffer for bounded candidate matching (avoids heap allocation for limit <= 128)
	var stackBuf [128]*LogEntry
	var matched []*LogEntry
	if allocCap <= len(stackBuf) {
		matched = stackBuf[:0]
	} else {
		matched = make([]*LogEntry, 0, allocCap)
	}

	if qTraceID != "" {
		span := h.byTraceID[qTraceID]
		for curr := span.tail; curr != -1 && len(matched) < limit; curr = h.links[curr].prevTrace {
			item := h.entries[curr]
			if item == nil {
				continue
			}
			if qService != "" && item.Service != qService {
				continue
			}
			if qLevel != "" && item.Level != qLevel {
				continue
			}
			matched = append(matched, item)
		}
	} else if qService != "" {
		span := h.byService[qService]
		for curr := span.tail; curr != -1 && len(matched) < limit; curr = h.links[curr].prevService {
			item := h.entries[curr]
			if item == nil {
				continue
			}
			if qLevel != "" && item.Level != qLevel {
				continue
			}
			matched = append(matched, item)
		}
	} else {
		// Ring buffer reverse iteration from newest to oldest
		for i := 0; i < h.count && len(matched) < limit; i++ {
			idx := (h.head + h.count - 1 - i + h.capacity) % h.capacity
			item := h.entries[idx]
			if item == nil {
				continue
			}
			if qLevel != "" && item.Level != qLevel {
				continue
			}
			matched = append(matched, item)
		}
	}

	// Copy defensively directly in chronological order (oldest first, newest last)
	results := make([]LogEntry, len(matched))
	for i, j := 0, len(matched)-1; j >= 0; i, j = i+1, j-1 {
		results[i] = copyEntry(matched[j])
	}

	return results
}

// Subscribe registers an SSE or streaming client.
func (h *MemoryLogHub) Subscribe(filter StreamFilter) (uint64, <-chan LogEntry, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		closedCh := make(chan LogEntry)
		close(closedCh)
		return 0, closedCh, func() {}
	}

	filter.Level = strings.ToUpper(strings.TrimSpace(filter.Level))
	filter.Service = strings.ToLower(strings.TrimSpace(filter.Service))

	h.nextSubID++
	id := h.nextSubID
	ch := make(chan LogEntry, 256)

	sub := &subscriber{
		id:     id,
		filter: filter,
		ch:     ch,
	}
	h.subscribers[id] = sub

	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			h.mu.Lock()
			delete(h.subscribers, id)
			h.mu.Unlock()

			sub.mu.Lock()
			if !sub.closed {
				sub.closed = true
				close(sub.ch)
			}
			sub.mu.Unlock()
		})
	}

	return id, ch, cancel
}

// Stats returns operational metrics for external monitoring and assertion.
func (h *MemoryLogHub) Stats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return HubStats{
		Capacity:        h.capacity,
		CurrentSize:     h.count,
		TotalIngested:   h.totalIngested,
		ActiveStreams:   len(h.subscribers),
		IndexedTraces:   len(h.byTraceID),
		IndexedServices: len(h.byService),
		DroppedEvents:   atomic.LoadUint64(&h.droppedEvents),
	}
}

// Close gracefully closes subscriber channels and halts the hub.
func (h *MemoryLogHub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true

	subs := make([]*subscriber, 0, len(h.subscribers))
	for id, sub := range h.subscribers {
		delete(h.subscribers, id)
		subs = append(subs, sub)
	}
	h.mu.Unlock()

	for _, sub := range subs {
		sub.mu.Lock()
		if !sub.closed {
			sub.closed = true
			close(sub.ch)
		}
		sub.mu.Unlock()
	}

	return nil
}

var _ IngestionObserver = (*MemoryLogHub)(nil)

// OnIngested satisfies the IngestionObserver interface port.
// It extracts metadata (service, trace_id, level) from the Record's payload (or uses defaults if non-JSON),
// constructs a LogEntry, and indexes it via Ingest().
func (h *MemoryLogHub) OnIngested(rec Record) {
	traceID, service, level := extractMetadataFromPayload(rec.Topic, rec.Payload)

	var rawPayload json.RawMessage
	if json.Valid(rec.Payload) {
		rawPayload = json.RawMessage(rec.Payload)
	} else {
		quoted, _ := json.Marshal(string(rec.Payload))
		rawPayload = json.RawMessage(quoted)
	}

	_ = h.Ingest(LogEntry{
		ID:        rec.ID,
		Timestamp: rec.Timestamp,
		Topic:     rec.Topic,
		Service:   service,
		TraceID:   traceID,
		Level:     level,
		Payload:   rawPayload,
	})
}

func extractMetadataFromPayload(topic string, payload []byte) (traceID, service, level string) {
	b := bytes.TrimSpace(payload)
	if len(b) >= 2 && b[0] == '{' {
		traceID, service, level = scanTopLevelMeta(b)
	}

	if service == "" {
		service = topic
	}
	if level == "" {
		level = "INFO"
	}
	return traceID, service, level
}

// scanTopLevelMeta extracts trace_id/traceId, service, and level/severity from the top level of a
// JSON object without a full unmarshal, keeping the hot path alloc-light. Nested objects/arrays are
// skipped so an inner key never shadows a top-level one; malformed input bails with whatever was parsed so far.
func scanTopLevelMeta(b []byte) (traceID, service, level string) {
	var snake, camel, lvl, sev string
	i, n := 1, len(b) // skip '{'
	for i < n {
		i = skipWS(b, i)
		if i >= n || b[i] == '}' || b[i] != '"' {
			break
		}
		ks, ke, ki, _, ok := scanString(b, i)
		if !ok {
			break
		}
		i = skipWS(b, ki)
		if i >= n || b[i] != ':' {
			break
		}
		i = skipWS(b, i+1)
		if i >= n {
			break
		}

		// string(b[ks:ke]) in a switch is compiler-optimized to byte comparisons, so classifying the key never allocates.
		field := 0 // 1 trace_id, 2 traceId, 3 service, 4 level, 5 severity
		switch string(b[ks:ke]) {
		case "trace_id":
			field = 1
		case "traceId":
			field = 2
		case "service":
			field = 3
		case "level":
			field = 4
		case "severity":
			field = 5
		}

		switch b[i] {
		case '"':
			vs, ve, vi, esc, vok := scanString(b, i)
			if !vok {
				return firstNonEmpty(snake, camel), service, firstNonEmpty(lvl, sev)
			}
			if field != 0 {
				val := materializeString(b, vs, ve, esc)
				switch field {
				case 1:
					snake = val
				case 2:
					camel = val
				case 3:
					service = val
				case 4:
					lvl = val
				case 5:
					sev = val
				}
			}
			i = vi
		case '{', '[':
			if i = skipContainer(b, i); i < 0 {
				return firstNonEmpty(snake, camel), service, firstNonEmpty(lvl, sev)
			}
		default:
			i = skipScalar(b, i)
		}

		i = skipWS(b, i)
		if i < n && b[i] == ',' {
			i++
		}
	}
	return firstNonEmpty(snake, camel), service, firstNonEmpty(lvl, sev)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func skipWS(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanString locates a JSON string starting at b[i]=='"' without allocating: it returns the content
// byte range [start,end), the index just past the closing quote, and whether the value held escapes.
func scanString(b []byte, i int) (start, end, next int, hasEscape, ok bool) {
	i++ // skip opening quote
	start = i
	for i < len(b) {
		switch b[i] {
		case '\\':
			hasEscape = true
			i += 2
		case '"':
			return start, i, i + 1, hasEscape, true
		default:
			i++
		}
	}
	return 0, 0, i, false, false
}

// materializeString turns a scanned string range [start,end) into a Go string. The common escape-free
// case is a single copy; an escaped value is decoded via strconv.Unquote on the full quoted token
// b[start-1:end+1]: start-1/end index the opening/closing quotes, which scanString guarantees exist.
func materializeString(b []byte, start, end int, hasEscape bool) string {
	if !hasEscape {
		return string(b[start:end])
	}
	if unq, err := strconv.Unquote(string(b[start-1 : end+1])); err == nil {
		return unq
	}
	return string(b[start:end])
}

// skipContainer advances past a balanced object/array (b[i] is '{' or '['), respecting strings.
// Returns -1 on malformed input.
func skipContainer(b []byte, i int) int {
	depth := 0
	for i < len(b) {
		switch b[i] {
		case '"':
			_, _, ni, _, ok := scanString(b, i)
			if !ok {
				return -1
			}
			i = ni
		case '{', '[':
			depth++
			i++
		case '}', ']':
			depth--
			i++
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return -1
}

// skipScalar advances past a bare number/true/false/null until the next structural delimiter.
func skipScalar(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ',', '}', ']', ' ', '\t', '\n', '\r':
			return i
		default:
			i++
		}
	}
	return i
}
