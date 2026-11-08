package walspool

import (
	"context"
	"errors"
	"testing"
)

// downedSink models a downstream that is unavailable, so records never commit.
type downedSink struct{}

func (downedSink) Deliver(context.Context, []Record) error {
	return ErrSinkUnavailable
}

// TestBugMaxcapacityIgnored verifies that Config.MaxCapacity is the engine-level
// backpressure quota: with one uncommitted record pending, a quota of 1 must
// reject the next Enqueue with ErrSpoolFull.
func TestBugMaxcapacityIgnored(t *testing.T) {
	storage := NewMemoryStorageEngine(100)

	cfg := DefaultConfig()
	cfg.BatchSize = 1
	cfg.MaxCapacity = 1

	spool, err := New(cfg, storage, downedSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	ctx := context.Background()
	if err := spool.Enqueue(ctx, "audit", []byte("first")); err != nil {
		t.Fatalf("first Enqueue should succeed: %v", err)
	}

	// The sink is down, so the first record stays uncommitted. Config.MaxCapacity=1
	// must therefore reject the second record at the backpressure quota.
	err = spool.Enqueue(ctx, "audit", []byte("second"))
	if !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("second Enqueue: got %v, want ErrSpoolFull (Config.MaxCapacity=%d is ignored)", err, cfg.MaxCapacity)
	}

	if n, err := storage.UncommittedCount(); err != nil {
		t.Fatalf("UncommittedCount: %v", err)
	} else if n != 1 {
		t.Fatalf("uncommitted records = %d, want 1 (a rejected record must not be appended)", n)
	}
}
