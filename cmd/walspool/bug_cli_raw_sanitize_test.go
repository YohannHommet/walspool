package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestBugCliRawSanitize verifies that the "raw" stream output formatter
// sanitizes untrusted payload bytes before writing them to stdout, as required
// by the terminal injection guardrails (no raw ESC/ANSI control bytes).
func TestBugCliRawSanitize(t *testing.T) {
	ev := &streamEvent{
		Payload: json.RawMessage([]byte("\x1b[31mRED\x1b[0m")),
	}

	var buf bytes.Buffer
	if err := printEvent(&buf, ev, "raw", true, false); err != nil {
		t.Fatalf("printEvent(raw) returned error: %v", err)
	}

	out := buf.Bytes()
	if !bytes.Contains(out, []byte("RED")) {
		t.Fatalf("raw output should still contain the payload text RED, got %q", out)
	}
	if bytes.Contains(out, []byte{0x1b}) {
		t.Fatalf("raw output contains a raw ESC (0x1b) byte: %q; untrusted payload must be sanitized via SanitizeTerminal before writing to stdout", out)
	}
}
