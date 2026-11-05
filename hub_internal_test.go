package walspool

import (
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestMemoryLogHub_CacheLineAlignment(t *testing.T) {
	var h MemoryLogHub

	assertOffset := func(name string, offset uintptr, expected uintptr) {
		t.Helper()
		if offset%64 != 0 {
			t.Errorf("field %s offset %d is not a multiple of 64 bytes", name, offset)
		}
		if offset != expected {
			t.Errorf("field %s offset got %d, want %d", name, offset, expected)
		}
	}

	assertOffset("mu", unsafe.Offsetof(h.mu), 0)
	assertOffset("head", unsafe.Offsetof(h.head), 64)
	assertOffset("count", unsafe.Offsetof(h.count), 128)
	assertOffset("seqID", unsafe.Offsetof(h.seqID), 192)
	assertOffset("totalIngested", unsafe.Offsetof(h.totalIngested), 256)
	assertOffset("droppedEvents", unsafe.Offsetof(h.droppedEvents), 320)

	// Verification of cold fields starting point (capacity)
	if capOffset := unsafe.Offsetof(h.capacity); capOffset != 384 {
		t.Errorf("cold fields start (capacity) got offset %d, want 384", capOffset)
	}

	// Pairwise check: ensure at least 64 bytes between each pair of hot fields
	offsets := []struct {
		name string
		off  uintptr
	}{
		{"mu", unsafe.Offsetof(h.mu)},
		{"head", unsafe.Offsetof(h.head)},
		{"count", unsafe.Offsetof(h.count)},
		{"seqID", unsafe.Offsetof(h.seqID)},
		{"totalIngested", unsafe.Offsetof(h.totalIngested)},
		{"droppedEvents", unsafe.Offsetof(h.droppedEvents)},
	}

	for i := 0; i < len(offsets); i++ {
		for j := i + 1; j < len(offsets); j++ {
			diff := offsets[j].off - offsets[i].off
			if diff < 64 {
				t.Errorf("insufficient distance between %s and %s: got %d bytes, want >= 64",
					offsets[i].name, offsets[j].name, diff)
			}
		}
	}
}

// BenchmarkHub_FalseSharingContention measures contention when 16 parallel goroutines
// mutate seqID and droppedEvents concurrently.
func BenchmarkHub_FalseSharingContention(b *testing.B) {
	hub := NewMemoryLogHub(50000)
	defer hub.Close()

	b.ResetTimer()
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddUint64(&hub.seqID, 1)
			atomic.AddUint64(&hub.droppedEvents, 1)
		}
	})
}

// BenchmarkHub_FalseSharingSeparateFields measures the pure speedup when distinct worker
// goroutines write to separate fields without invalidating each other's cache lines.
func BenchmarkHub_FalseSharingSeparateFields(b *testing.B) {
	hub := NewMemoryLogHub(50000)
	defer hub.Close()

	var workerID uint64
	b.ResetTimer()
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddUint64(&workerID, 1)
		for pb.Next() {
			if id%2 == 0 {
				atomic.AddUint64(&hub.seqID, 1)
			} else {
				atomic.AddUint64(&hub.droppedEvents, 1)
			}
		}
	})
}
