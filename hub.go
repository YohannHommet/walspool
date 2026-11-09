package walspool

import (
	"encoding/json"
	"fmt"
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
// Hot atomic counters, cursors, and synchronization primitives are explicitly
// padded to 64-byte boundaries to eliminate multi-core CPU false sharing.
type MemoryLogHub struct {
	// Ligne de cache 0 (64 octets) : Verrou RWMutex de synchronisation
	mu sync.RWMutex
	_  [40]byte // Padding: 24 + 40 = 64 octets

	// Ligne de cache 1 (64 octets) : Curseur de tete du tampon circulaire
	head int
	_    [56]byte // Padding: 8 + 56 = 64 octets

	// Ligne de cache 2 (64 octets) : Nombre d'elements actifs dans le tampon
	count int
	_     [56]byte // Padding: 8 + 56 = 64 octets

	// Ligne de cache 3 (64 octets) : Compteur de sequence d'evenements (atomique)
	seqID uint64
	_     [56]byte // Padding: 8 + 56 = 64 octets

	// Ligne de cache 4 (64 octets) : Compteur cumulatif d'evenements ingeres
	totalIngested uint64
	_             [56]byte // Padding: 8 + 56 = 64 octets

	// Ligne de cache 5 (64 octets) : Compteur cumulatif d'evenements SSE abandonnes (atomique)
	droppedEvents uint64
	_             [56]byte // Padding: 8 + 56 = 64 octets

	// Ligne de cache 6+ : Metadonnees de configuration et structures froides / dominante lecture
	capacity    int
	entries     []*LogEntry
	links       []slotLinks
	byTraceID   map[string]indexSpan
	byService   map[string]indexSpan
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
func (h *MemoryLogHub) normalizeEntry(entry LogEntry) LogEntry {
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
	if len(entry.Payload) > 0 {
		payloadCopy := make(json.RawMessage, len(entry.Payload))
		copy(payloadCopy, entry.Payload)
		entry.Payload = payloadCopy
	}
	return entry
}

// evictAndInsertLocked writes item into the ring and prunes the evicted
// secondary-index links. Caller holds h.mu.
func (h *MemoryLogHub) evictAndInsertLocked(item *LogEntry) int32 {
	var pos int32
	if h.count == h.capacity {
		pos = int32(h.head)
		oldItem := h.entries[pos]
		h.entries[pos] = nil

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
	return pos
}

// snapshotSubscribersLocked copies the subscriber slice so notification can
// happen outside the hub lock. Caller holds h.mu.
func (h *MemoryLogHub) snapshotSubscribersLocked() []*subscriber {
	if len(h.subscribers) == 0 {
		return nil
	}
	subs := make([]*subscriber, 0, len(h.subscribers))
	for _, sub := range h.subscribers {
		subs = append(subs, sub)
	}
	return subs
}

func (h *MemoryLogHub) notifySubscribers(subs []*subscriber, entry LogEntry) {
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
}

func (h *MemoryLogHub) Ingest(entry LogEntry) error {
	if entry.Topic == "" && entry.Service == "" {
		return fmt.Errorf("%w: entry must specify topic or service", ErrPreconditionViolated)
	}

	entry = h.normalizeEntry(entry)
	item := &entry

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrHubClosed
	}
	h.totalIngested++

	pos := h.evictAndInsertLocked(item)
	if item.TraceID != "" {
		h.linkTrace(pos, item.TraceID)
	}
	if item.Service != "" {
		h.linkService(pos, item.Service)
	}
	subs := h.snapshotSubscribersLocked()
	h.mu.Unlock()

	h.notifySubscribers(subs, entry)
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
