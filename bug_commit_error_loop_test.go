package walspool

import (
	"context"
	"errors"
	"testing"
)

// commitErrLoopStorage simulates a durable log whose reads succeed but whose
// checkpoint (Commit) keeps failing, for example after an I/O error or a full disk.
type commitErrLoopStorage struct {
	records     []Record
	commitCalls int
	commitErr   error
}

func (s *commitErrLoopStorage) Append(rec Record) (Offset, error) {
	s.records = append(s.records, rec)
	return rec.Offset, nil
}

func (s *commitErrLoopStorage) ReadBatch(maxCount int) ([]Record, error) {
	if maxCount > len(s.records) {
		maxCount = len(s.records)
	}
	return s.records[:maxCount], nil
}

func (s *commitErrLoopStorage) Commit(offset Offset) error {
	s.commitCalls++
	return s.commitErr
}

func (s *commitErrLoopStorage) UncommittedCount() (int, error) { return len(s.records), nil }

func (s *commitErrLoopStorage) Close() error { return nil }

// commitErrLoopSink permanently rejects every batch, as a sink does for
// unprocessable payloads (HTTP 400/422).
type commitErrLoopSink struct {
	deliverCalls int
}

func (s *commitErrLoopSink) Deliver(ctx context.Context, batch []Record) error {
	s.deliverCalls++
	return ErrPermanentRejection
}

func TestBugCommitErrorLoop(t *testing.T) {
	storage := &commitErrLoopStorage{
		records:   []Record{{Offset: 7, ID: 1, Topic: "t", Payload: []byte("poison")}},
		commitErr: ErrStorageUnavailable,
	}
	sink := &commitErrLoopSink{}
	e := &Engine{cfg: Config{BatchSize: 1}, storage: storage, sink: sink}

	drained, err := e.drainPendingBatches(context.Background())

	if sink.deliverCalls != 1 {
		t.Fatalf("expected 1 Deliver call before the checkpoint, got %d", sink.deliverCalls)
	}
	if storage.commitCalls != 1 {
		t.Fatalf("expected 1 Commit call to advance past the permanent rejection, got %d", storage.commitCalls)
	}
	if err == nil {
		t.Fatalf("BUG: drainPendingBatches returned nil error after storage.Commit failed with %v (drained=%v); the commit failure is silently swallowed, so the dispatcher will immediately re-read and re-deliver the same permanently-rejected batch in a hot loop", ErrStorageUnavailable, drained)
	}
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("expected the commit failure to surface ErrStorageUnavailable, got %v", err)
	}
}
