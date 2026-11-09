package walspool

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTopicBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		want  error
	}{
		{name: "empty", topic: "", want: ErrEmptyTopic},
		{name: "max", topic: strings.Repeat("t", MaxTopicBytes), want: nil},
		{name: "too_long", topic: strings.Repeat("t", MaxTopicBytes+1), want: ErrTopicTooLong},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTopic(tc.topic)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("ValidateTopic: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if !errors.Is(err, ErrPreconditionViolated) {
				t.Fatalf("error %v must wrap ErrPreconditionViolated", err)
			}
		})
	}
}

func TestValidatePayloadBoundaries(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		max     int
		want    error
	}{
		{name: "empty", payload: nil, max: 10, want: ErrEmptyPayload},
		{name: "exact", payload: []byte("0123456789"), max: 10, want: nil},
		{name: "too_large", payload: []byte("0123456789X"), max: 10, want: ErrPayloadTooLarge},
		{name: "unbounded", payload: []byte(strings.Repeat("x", 1025)), max: 0, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePayload(tc.payload, tc.max)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("ValidatePayload: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if !errors.Is(err, ErrPreconditionViolated) {
				t.Fatalf("error %v must wrap ErrPreconditionViolated", err)
			}
		})
	}
}

func TestStorageAdaptersRejectOversizedTopicConsistently(t *testing.T) {
	topic := strings.Repeat("x", MaxTopicBytes+1)
	rec := Record{Topic: topic, Payload: []byte("payload")}

	if _, err := NewMemoryStorageEngine(10).Append(rec); !errors.Is(err, ErrTopicTooLong) {
		t.Fatalf("memory Append error = %v, want ErrTopicTooLong", err)
	}

	file, err := NewFileStorageEngineWithOptions(t.TempDir(), 10, FileStorageOptions{
		BufferSize: 1024,
		SyncPolicy: SyncEveryRecord,
	})
	if err != nil {
		t.Fatalf("NewFileStorageEngineWithOptions: %v", err)
	}
	defer file.Close()
	if _, err := file.Append(rec); !errors.Is(err, ErrTopicTooLong) {
		t.Fatalf("file Append error = %v, want ErrTopicTooLong", err)
	}
}

func TestEngineRejectsOversizedTopicBeforeStorage(t *testing.T) {
	storage := NewMemoryStorageEngine(10)
	cfg := DefaultConfig()
	cfg.MaxCapacity = 1
	spool, err := New(cfg, storage, successSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer spool.Close()

	err = spool.Enqueue(t.Context(), strings.Repeat("x", MaxTopicBytes+1), []byte("payload"))
	if !errors.Is(err, ErrTopicTooLong) {
		t.Fatalf("Enqueue error = %v, want ErrTopicTooLong", err)
	}
	if n, _ := storage.UncommittedCount(); n != 0 {
		t.Fatalf("storage accepted a rejected record: uncommitted=%d", n)
	}
}
