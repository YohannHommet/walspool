package main

import (
	"errors"
	"testing"
	"time"

	"github.com/YohannHommet/walspool"
)

// Step 10: the sidecar must reuse the library's buffered/sync defaults while adding a
// 64 MiB segment rotation default and mapping the spool byte quota.
func TestSidecarStorageOptions(t *testing.T) {
	opts := sidecarStorageOptions(1 << 30)

	if want := int64(64 * 1024 * 1024); opts.MaxSegmentSize != want {
		t.Errorf("MaxSegmentSize = %d, want %d (64 MiB)", opts.MaxSegmentSize, want)
	}
	if want := int64(1 << 30); opts.MaxUncommittedBytes != want {
		t.Errorf("MaxUncommittedBytes = %d, want %d", opts.MaxUncommittedBytes, want)
	}
	if want := 128 * 1024; opts.BufferSize != want {
		t.Errorf("BufferSize = %d, want %d", opts.BufferSize, want)
	}
	if opts.SyncPolicy != walspool.SyncInterval {
		t.Errorf("SyncPolicy = %v, want SyncInterval", opts.SyncPolicy)
	}
	if want := 50 * time.Millisecond; opts.SyncInterval != want {
		t.Errorf("SyncInterval = %v, want %v", opts.SyncInterval, want)
	}
}

// Step 12: a real engine built from the sidecar options must reject the uncommitted backlog
// at the byte quota (well before the count guard), recover after a commit, and never need a
// handler change because Append already maps ErrSpoolFull to HTTP 503.
func TestSidecarMaxSpoolBytesRejectsBacklog(t *testing.T) {
	dir := t.TempDir()
	opts := sidecarStorageOptions(1024)
	// Deterministic test engine: no background flusher, exact byte accounting.
	opts.SyncPolicy = walspool.SyncBatchCommit
	eng, err := walspool.NewFileStorageEngineWithOptions(dir, 50000, opts)
	if err != nil {
		t.Fatalf("init engine failed: %v", err)
	}
	defer eng.Close()

	payload := make([]byte, 100)
	accepted := 0
	var rejectErr error
	for i := 0; i < 100; i++ {
		rec := walspool.Record{ID: uint64(i + 1), Timestamp: time.Unix(0, int64(i)), Topic: "quota", Payload: payload}
		if _, err := eng.Append(rec); err != nil {
			rejectErr = err
			break
		}
		accepted++
	}

	if !errors.Is(rejectErr, walspool.ErrSpoolFull) {
		t.Fatalf("expected ErrSpoolFull within 100 appends (quota 1024, %d accepted), got %v", accepted, rejectErr)
	}
	if accepted >= 100 {
		t.Fatalf("byte quota did not trigger within 100 appends")
	}
	if got := eng.UncommittedBytes(); got > 1024 {
		t.Fatalf("UncommittedBytes = %d exceeds quota 1024", got)
	}
	if count, err := eng.UncommittedCount(); err != nil || count >= 50000 {
		t.Fatalf("UncommittedCount = %d (err %v), want < 50000 (count guard must not be what triggered)", count, err)
	}

	next := walspool.Record{ID: 999, Timestamp: time.Unix(0, 999), Topic: "quota", Payload: payload}
	if _, err := eng.Append(next); !errors.Is(err, walspool.ErrSpoolFull) {
		t.Fatalf("append while saturated: got %v, want ErrSpoolFull", err)
	}

	if err := eng.Commit(0); err != nil {
		t.Fatalf("Commit(0) failed: %v", err)
	}
	if _, err := eng.Append(next); err != nil {
		t.Fatalf("append after committing the prefix must succeed: %v", err)
	}
}

// Step 11: -max-spool-bytes / WALSPOOL_MAX_SPOOL_BYTES resolve with flag > env > default
// precedence, reject malformed int64 env values, and require a positive value.
func TestSidecarMaxSpoolBytesConfig(t *testing.T) {
	noEnv := func(k string) (string, bool) { return "", false }
	envWith := func(pairs map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			v, ok := pairs[k]
			return v, ok
		}
	}

	t.Run("Default", func(t *testing.T) {
		cfg, err := ParseConfig([]string{}, noEnv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := int64(1 << 30); cfg.MaxSpoolBytes != want {
			t.Fatalf("default MaxSpoolBytes = %d, want %d (1 GiB)", cfg.MaxSpoolBytes, want)
		}
	})

	t.Run("EnvOverride", func(t *testing.T) {
		cfg, err := ParseConfig([]string{}, envWith(map[string]string{
			"WALSPOOL_MAX_SPOOL_BYTES": "2097152",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxSpoolBytes != 2097152 {
			t.Fatalf("env MaxSpoolBytes = %d, want 2097152", cfg.MaxSpoolBytes)
		}
	})

	t.Run("FlagBeatsEnv", func(t *testing.T) {
		cfg, err := ParseConfig([]string{"-max-spool-bytes", "1048576"}, envWith(map[string]string{
			"WALSPOOL_MAX_SPOOL_BYTES": "2097152",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxSpoolBytes != 1048576 {
			t.Fatalf("flag MaxSpoolBytes = %d, want 1048576 (flag beats env)", cfg.MaxSpoolBytes)
		}
	})

	t.Run("RejectZero", func(t *testing.T) {
		if _, err := ParseConfig([]string{"-max-spool-bytes", "0"}, noEnv); !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Fatalf("zero max-spool-bytes: got %v, want ErrPreconditionViolated", err)
		}
	})

	t.Run("RejectNegative", func(t *testing.T) {
		if _, err := ParseConfig([]string{"-max-spool-bytes", "-1"}, noEnv); !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Fatalf("negative max-spool-bytes: got %v, want ErrPreconditionViolated", err)
		}
	})

	t.Run("MalformedEnv", func(t *testing.T) {
		if _, err := ParseConfig([]string{}, envWith(map[string]string{
			"WALSPOOL_MAX_SPOOL_BYTES": "1GiB",
		})); !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Fatalf("malformed env int64: got %v, want ErrPreconditionViolated", err)
		}
	})

	t.Run("ValidateRejectsZero", func(t *testing.T) {
		cfg := SidecarConfig{
			Addr:          ":9099",
			BatchSize:     50,
			FlushMs:       50,
			MaxRecords:    50000,
			MaxSpoolBytes: 0,
			HubCapacity:   50000,
			LogFormat:     "text",
			LogLevel:      "info",
		}
		if err := cfg.Validate(); !errors.Is(err, walspool.ErrPreconditionViolated) {
			t.Fatalf("Validate with zero MaxSpoolBytes: got %v, want ErrPreconditionViolated", err)
		}
	})
}
