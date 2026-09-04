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

// MemoryLogHub is the high-performance in-memory implementation of LogHub.
// It maintains a fixed-size ring buffer with O(1) append and eviction,
// alongside secondary indices by trace_id and service with zero-leak pruning.
type MemoryLogHub struct {
	mu sync.RWMutex

	capacity int
	entries  []*LogEntry
	head     int
	count    int

	seqID         uint64
	totalIngested uint64
	droppedEvents uint64

	byTraceID map[string][]*LogEntry
	byService map[string][]*LogEntry

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
	return &MemoryLogHub{
		capacity:    capacity,
		entries:     make([]*LogEntry, capacity),
		byTraceID:   make(map[string][]*LogEntry),
		byService:   make(map[string][]*LogEntry),
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

// Ingest indexes a log entry into the ring buffer and distributes to matching SSE subscribers.
func (h *MemoryLogHub) Ingest(entry LogEntry) error {
	// Precondition validation (Meyer DbC - caller fault)
	if entry.Topic == "" && entry.Service == "" {
		return fmt.Errorf("%w: entry must specify topic or service", ErrPreconditionViolated)
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrSpoolerClosed
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

	item := &entry
	h.totalIngested++

	// Circular Ring Buffer Eviction: O(1)
	if h.count == h.capacity {
		oldItem := h.entries[h.head]
		h.entries[h.head] = nil // Release pointer for immediate GC eligibility

		// Prune from byTraceID index without memory leak
		if oldItem != nil && oldItem.TraceID != "" {
			traceList := h.byTraceID[oldItem.TraceID]
			if len(traceList) > 0 && traceList[0] == oldItem {
				// O(1) head advance: the evicted pointer is nil'd for GC, avoiding an O(n) sliding copy on this hot path.
				traceList[0] = nil
				traceList = traceList[1:]
			} else {
				for i, it := range traceList {
					if it == oldItem {
						traceList[i] = nil
						copy(traceList[i:], traceList[i+1:])
						traceList[len(traceList)-1] = nil
						traceList = traceList[:len(traceList)-1]
						break
					}
				}
			}
			if len(traceList) == 0 {
				delete(h.byTraceID, oldItem.TraceID)
			} else {
				h.byTraceID[oldItem.TraceID] = traceList
			}
		}

		// Prune from byService index without memory leak
		if oldItem != nil && oldItem.Service != "" {
			svcList := h.byService[oldItem.Service]
			if len(svcList) > 0 && svcList[0] == oldItem {
				// O(1) head advance (see byTraceID above); no O(n) sliding copy on the hot path.
				svcList[0] = nil
				svcList = svcList[1:]
			} else {
				for i, it := range svcList {
					if it == oldItem {
						svcList[i] = nil
						copy(svcList[i:], svcList[i+1:])
						svcList[len(svcList)-1] = nil
						svcList = svcList[:len(svcList)-1]
						break
					}
				}
			}
			if len(svcList) == 0 {
				delete(h.byService, oldItem.Service)
			} else {
				h.byService[oldItem.Service] = svcList
			}
		}

		h.entries[h.head] = item
		h.head = (h.head + 1) % h.capacity
	} else {
		pos := (h.head + h.count) % h.capacity
		h.entries[pos] = item
		h.count++
	}

	// Update secondary indices
	if item.TraceID != "" {
		h.byTraceID[item.TraceID] = append(h.byTraceID[item.TraceID], item)
	}
	if item.Service != "" {
		h.byService[item.Service] = append(h.byService[item.Service], item)
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
		if c := len(h.byTraceID[qTraceID]); c < allocCap {
			allocCap = c
		}
	} else if qService != "" {
		if c := len(h.byService[qService]); c < allocCap {
			allocCap = c
		}
	} else {
		if h.count < allocCap {
			allocCap = h.count
		}
	}

	// Stack buffer for bounded candidate matching (avoids swapping large LogEntry structs)
	var stackBuf [128]*LogEntry
	var matched []*LogEntry
	if allocCap <= len(stackBuf) {
		matched = stackBuf[:0]
	} else {
		matched = make([]*LogEntry, 0, allocCap)
	}

	if qTraceID != "" {
		candidates := h.byTraceID[qTraceID]
		for i := len(candidates) - 1; i >= 0 && len(matched) < limit; i-- {
			item := candidates[i]
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
		candidates := h.byService[qService]
		for i := len(candidates) - 1; i >= 0 && len(matched) < limit; i-- {
			item := candidates[i]
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
// b[start-1:end+1] — start-1/end index the opening/closing quotes, which scanString guarantees exist.
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
