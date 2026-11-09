package walspool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// externalStorage deliberately implements only StorageEngine, with no
// package-private limiter method. It models a third-party adapter.
type externalStorage struct {
	mu         sync.Mutex
	records    []Record
	checkpoint Offset
	closed     bool
	appendErrs int
}

func (s *externalStorage) Append(rec Record) (Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStorageUnavailable
	}
	if s.appendErrs > 0 {
		s.appendErrs--
		return 0, ErrStorageUnavailable
	}
	rec.Offset = Offset(len(s.records))
	s.records = append(s.records, rec)
	return rec.Offset, nil
}

func (s *externalStorage) ReadBatch(maxCount int) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStorageUnavailable
	}
	start := int(s.checkpoint)
	if start >= len(s.records) {
		return nil, nil
	}
	end := start + maxCount
	if end > len(s.records) {
		end = len(s.records)
	}
	out := make([]Record, end-start)
	copy(out, s.records[start:end])
	return out, nil
}

func (s *externalStorage) Commit(offset Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStorageUnavailable
	}
	if offset >= Offset(len(s.records)) {
		return ErrPreconditionViolated
	}
	if offset+1 > s.checkpoint {
		s.checkpoint = offset + 1
	}
	return nil
}

func (s *externalStorage) UncommittedCount() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStorageUnavailable
	}
	if int(s.checkpoint) >= len(s.records) {
		return 0, nil
	}
	return len(s.records) - int(s.checkpoint), nil
}

func (s *externalStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

type blockedSink struct{}

func (blockedSink) Deliver(context.Context, []Record) error {
	return errors.New("downstream blocked")
}

type successSink struct{}

func (successSink) Deliver(context.Context, []Record) error { return nil }

func TestEngine_AtomicCapacityForExternalStorage(t *testing.T) {
	storage := &externalStorage{}
	cfg := DefaultConfig()
	cfg.BatchSize = 1
	cfg.MaxCapacity = 1

	spool, err := New(cfg, storage, blockedSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	var accepted atomic.Int64
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := spool.Enqueue(context.Background(), "audit", []byte("record"))
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrSpoolFull):
				rejected.Add(1)
			default:
				t.Errorf("unexpected Enqueue error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted = %d, want exactly 1 under MaxCapacity=1", got)
	}
	if got := rejected.Load(); got != 99 {
		t.Fatalf("rejected = %d, want 99", got)
	}
	if n, _ := storage.UncommittedCount(); n != 1 {
		t.Fatalf("storage uncommitted = %d, want 1", n)
	}
}

func TestEngine_AdmissionReleasedOnAppendFailure(t *testing.T) {
	storage := &externalStorage{appendErrs: 1}
	cfg := DefaultConfig()
	cfg.MaxCapacity = 1

	spool, err := New(cfg, storage, blockedSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	if err := spool.Enqueue(context.Background(), "audit", []byte("first")); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("first Enqueue error = %v, want ErrStorageUnavailable", err)
	}
	if err := spool.Enqueue(context.Background(), "audit", []byte("second")); err != nil {
		t.Fatalf("reservation leaked after append failure: %v", err)
	}
}

func TestEngine_AdmissionSeededFromRecoveredBacklog(t *testing.T) {
	storage := NewMemoryStorageEngine(100)
	if _, err := storage.Append(Record{ID: 1, Topic: "audit", Payload: []byte("recovered")}); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	cfg := DefaultConfig()
	cfg.MaxCapacity = 1

	spool, err := New(cfg, storage, blockedSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	if err := spool.Enqueue(context.Background(), "audit", []byte("new")); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("Enqueue error = %v, want ErrSpoolFull from recovered backlog", err)
	}
}

func TestEngine_AdmissionReleasedAfterCommit(t *testing.T) {
	storage := NewMemoryStorageEngine(100)
	cfg := DefaultConfig()
	cfg.BatchSize = 1
	cfg.MaxCapacity = 1

	spool, err := New(cfg, storage, successSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	if err := spool.Enqueue(context.Background(), "audit", []byte("first")); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, _ := storage.UncommittedCount()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("record was not committed before timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := spool.Enqueue(context.Background(), "audit", []byte("second")); err != nil {
		t.Fatalf("admission not released after commit: %v", err)
	}
}
