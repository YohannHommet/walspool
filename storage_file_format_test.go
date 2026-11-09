package walspool

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeCheckpoint(t *testing.T, dir string, offset uint64) {
	t.Helper()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)
	writeBytes(t, filepath.Join(dir, "checkpoint.meta"), buf[:])
}

func dirSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	snapshot := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			snapshot[entry.Name()] = "<dir>"
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		snapshot[entry.Name()] = string(data)
	}
	return snapshot
}

func sameSnapshot(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func TestFileStorage_FormatMismatchIsRefusedWithoutMutation(t *testing.T) {
	cases := []struct {
		name       string
		maxSegment int64
		setup      func(t *testing.T, dir string)
	}{
		{
			name:       "default_options_with_segment",
			maxSegment: 0,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "segment-000000000001.wal"), []byte("segment"))
			},
		},
		{
			name:       "default_options_with_active_and_segment",
			maxSegment: 0,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "active.wal"), []byte("active"))
				writeBytes(t, filepath.Join(dir, "segment-000000000001.wal"), []byte("segment"))
			},
		},
		{
			name:       "default_options_with_checkpoint_only",
			maxSegment: 0,
			setup: func(t *testing.T, dir string) {
				writeCheckpoint(t, dir, 5)
			},
		},
		{
			name:       "segmented_options_with_checkpoint_only",
			maxSegment: 1024,
			setup: func(t *testing.T, dir string) {
				writeCheckpoint(t, dir, 5)
			},
		},
		{
			name:       "segmented_options_with_active_and_zero_segment",
			maxSegment: 1024,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "active.wal"), []byte("legacy"))
				writeBytes(t, filepath.Join(dir, "segment-000000000000.wal"), []byte("zero"))
			},
		},
		{
			name:       "malformed_segment_name",
			maxSegment: 0,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "segment-not-a-number.wal"), []byte("bad"))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(t, dir)
			before := dirSnapshot(t, dir)

			engine, err := NewFileStorageEngineWithOptions(dir, 10, FileStorageOptions{
				BufferSize:     1024,
				SyncPolicy:     SyncEveryRecord,
				MaxSegmentSize: tc.maxSegment,
			})
			if engine != nil {
				_ = engine.Close()
				t.Fatalf("expected constructor to refuse incompatible layout")
			}
			if !errors.Is(err, ErrStorageFormatMismatch) {
				t.Fatalf("error = %v, want ErrStorageFormatMismatch", err)
			}
			if !errors.Is(err, ErrPreconditionViolated) {
				t.Fatalf("error = %v, want it to wrap ErrPreconditionViolated", err)
			}
			after := dirSnapshot(t, dir)
			if !sameSnapshot(before, after) {
				t.Fatalf("directory mutated on refusal: before=%v after=%v", before, after)
			}
		})
	}
}

func TestFileStorage_CompatibleLayoutsStillOpen(t *testing.T) {
	cases := []struct {
		name       string
		maxSegment int64
		setup      func(t *testing.T, dir string)
	}{
		{
			name:       "default_empty",
			maxSegment: 0,
			setup:      func(t *testing.T, dir string) {},
		},
		{
			name:       "default_active_only",
			maxSegment: 0,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "active.wal"), nil)
			},
		},
		{
			name:       "default_zero_checkpoint_only",
			maxSegment: 0,
			setup: func(t *testing.T, dir string) {
				writeCheckpoint(t, dir, 0)
			},
		},
		{
			name:       "segmented_empty",
			maxSegment: 1024,
			setup:      func(t *testing.T, dir string) {},
		},
		{
			name:       "segmented_active_legacy_only",
			maxSegment: 1024,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "active.wal"), nil)
			},
		},
		{
			name:       "segmented_active_with_nonzero_segment",
			maxSegment: 1024,
			setup: func(t *testing.T, dir string) {
				writeBytes(t, filepath.Join(dir, "active.wal"), nil)
				writeBytes(t, filepath.Join(dir, "segment-000000000001.wal"), nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(t, dir)
			engine, err := NewFileStorageEngineWithOptions(dir, 10, FileStorageOptions{
				BufferSize:     1024,
				SyncPolicy:     SyncEveryRecord,
				MaxSegmentSize: tc.maxSegment,
			})
			if err != nil {
				t.Fatalf("expected compatible layout to open: %v", err)
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

func TestFileStorage_FormatMismatchSymlinkRefused(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target.wal")
	writeBytes(t, target, nil)
	if err := os.Symlink(target, filepath.Join(dir, "active.wal")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	engine, err := NewFileStorageEngineWithOptions(dir, 10, FileStorageOptions{
		BufferSize: 1024,
		SyncPolicy: SyncEveryRecord,
	})
	if engine != nil {
		_ = engine.Close()
		t.Fatalf("expected symlinked WAL to be refused")
	}
	if !errors.Is(err, ErrStorageFormatMismatch) {
		t.Fatalf("error = %v, want ErrStorageFormatMismatch", err)
	}
}
