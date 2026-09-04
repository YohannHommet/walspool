package walspool

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// faultyWriter simulates a disk that accepts `budget` bytes then fails every subsequent write
// with ENOSPC, so the buffered-write failure paths can be exercised deterministically.
type faultyWriter struct {
	target  io.Writer
	budget  int // bytes allowed before failing; the rest error with ENOSPC
	written int
}

func (w *faultyWriter) Write(p []byte) (int, error) {
	remaining := w.budget - w.written
	if remaining <= 0 {
		return 0, syscall.ENOSPC
	}
	if len(p) <= remaining {
		n, err := w.target.Write(p)
		w.written += n
		return n, err
	}
	n, _ := w.target.Write(p[:remaining])
	w.written += n
	return n, syscall.ENOSPC
}

// A write failure mid-flush must never grow the WAL with zero padding. The rollback must
// truncate to what is physically on disk, not to the (larger) logical write position.
func TestFileStorage_NoZeroPaddingOnWriteFailure(t *testing.T) {
	dir := t.TempDir()

	recA := Record{ID: 1, Timestamp: time.Unix(0, 1), Topic: "t", Payload: []byte("AAAAAAAAAAAAAAAA")}
	dataA, err := recA.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	sizeA := len(dataA)
	partial := sizeA / 2

	// SyncBatchCommit has no background flusher (deterministic); the buffer overflows on B, forcing a flush through the faulty writer.
	opts := FileStorageOptions{BufferSize: sizeA + 8, SyncPolicy: SyncBatchCommit}
	eng, err := NewFileStorageEngineWithOptions(dir, 100, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	faulty := &faultyWriter{target: eng.walFile, budget: partial}
	eng.writer = bufio.NewWriterSize(faulty, sizeA+8)

	if _, err := eng.Append(recA); err != nil {
		t.Fatalf("append A failed: %v", err)
	}

	recB := Record{ID: 2, Timestamp: time.Unix(0, 2), Topic: "t", Payload: []byte("BBBBBBBBBBBBBBBB")}
	if _, err := eng.Append(recB); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("expected ErrStorageUnavailable on B, got %v", err)
	}

	walPath := filepath.Join(dir, "active.wal")
	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}

	// The bug: Truncate(logicalPos) would extend the file to sizeA with zero bytes.
	if fi.Size() != int64(partial) {
		t.Fatalf("expected file truncated to physical size %d, got %d (zero-padding corruption)", partial, fi.Size())
	}
	if eng.writePos != int64(partial) {
		t.Fatalf("expected writePos %d after rollback, got %d", partial, eng.writePos)
	}

	// No record survives fully: A was only half-flushed, B never landed.
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal failed: %v", err)
	}
	if len(data) != partial {
		t.Fatalf("expected %d bytes on disk, got %d", partial, len(data))
	}
	_ = eng.Close()
}

// A background flush failure must be logged, not silently swallowed.
func TestFileStorage_PeriodicFlusherLogsError(t *testing.T) {
	dir := t.TempDir()

	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(prev)

	opts := FileStorageOptions{BufferSize: 4096, SyncPolicy: SyncInterval, SyncInterval: 10 * time.Millisecond}
	eng, err := NewFileStorageEngineWithOptions(dir, 100, opts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	// Swap in an always-failing writer with dirty buffered bytes so the next flush tick errors.
	eng.mu.Lock()
	nw := bufio.NewWriterSize(&faultyWriter{target: io.Discard, budget: 0}, 4096)
	_, _ = nw.WriteString("dirty")
	eng.writer = nw
	eng.mu.Unlock()

	time.Sleep(40 * time.Millisecond)

	// Close stops the flusher; flusherWg.Wait() orders the goroutine's log writes before this read.
	_ = eng.Close()

	if !bytes.Contains(logBuf.Bytes(), []byte("periodic WAL flush failed")) {
		t.Fatalf("expected periodic flush error to be logged, got: %q", logBuf.String())
	}
}

// The alloc-light scanner must return the same top-level metadata a full unmarshal would,
// including key precedence, nested-object shadowing, and non-JSON fallback to the topic.
func TestExtractMetadataFromPayload(t *testing.T) {
	cases := []struct {
		name                        string
		topic                       string
		payload                     string
		wantTrace, wantSvc, wantLvl string
	}{
		{"simple", "topic", `{"trace_id":"t1","service":"svc","level":"warn"}`, "t1", "svc", "warn"},
		{"camelTrace", "topic", `{"traceId":"t2"}`, "t2", "topic", "INFO"},
		{"snakeWinsOverCamel", "topic", `{"traceId":"c","trace_id":"s"}`, "s", "topic", "INFO"},
		{"severityFallback", "topic", `{"severity":"error"}`, "", "topic", "error"},
		{"levelWinsOverSeverity", "topic", `{"level":"info","severity":"err"}`, "", "topic", "info"},
		{"nestedShadow", "topic", `{"meta":{"service":"inner","trace_id":"z"},"service":"outer","trace_id":"x"}`, "x", "outer", "INFO"},
		{"scalarsAndArrays", "topic", `{"n":123,"tags":["a","b"],"ok":true,"service":"s","trace_id":"t"}`, "t", "s", "INFO"},
		{"whitespace", "topic", `{ "service" : "s" , "trace_id" : "t" }`, "t", "s", "INFO"},
		{"escapedQuote", "topic", `{"service":"a\"b","trace_id":"t"}`, "t", `a"b`, "INFO"},
		{"nonJSON", "topic", `plain text log line`, "", "topic", "INFO"},
		{"emptyObject", "topic", `{}`, "", "topic", "INFO"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trace, svc, lvl := extractMetadataFromPayload(tc.topic, []byte(tc.payload))
			if trace != tc.wantTrace || svc != tc.wantSvc || lvl != tc.wantLvl {
				t.Fatalf("got (%q,%q,%q), want (%q,%q,%q)", trace, svc, lvl, tc.wantTrace, tc.wantSvc, tc.wantLvl)
			}
		})
	}
}

// Extraction must not allocate a JSON tree; only the (few) extracted strings.
func TestExtractMetadata_BoundedAllocations(t *testing.T) {
	payload := []byte(`{"trace_id":"tr-123","service":"billing","level":"warn","amount":9223372036854775807,"nested":{"a":1}}`)
	allocs := testing.AllocsPerRun(200, func() {
		_, _, _ = extractMetadataFromPayload("topic", payload)
	})
	// Three extracted strings (trace_id, service, level); a full json.Unmarshal would allocate far more.
	if allocs > 3 {
		t.Fatalf("expected <= 3 allocations per extraction, got %.0f", allocs)
	}
}
