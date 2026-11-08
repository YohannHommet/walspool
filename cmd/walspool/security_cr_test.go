package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestSanitizeTerminal_CarriageReturnAlwaysEscaped(t *testing.T) {
	for _, singleLine := range []bool{false, true} {
		input := "safe\rEVIL"
		got := SanitizeTerminal(input, singleLine)
		if strings.ContainsRune(got, '\r') {
			t.Fatalf("singleLine=%v: sanitized output still contains raw CR: %q", singleLine, got)
		}
		if !strings.Contains(got, `\r`) {
			t.Fatalf("singleLine=%v: expected literal \\r escape, got %q", singleLine, got)
		}
		if !strings.Contains(got, "EVIL") {
			t.Fatalf("singleLine=%v: payload text was lost: %q", singleLine, got)
		}
	}
}

func TestSanitizeTerminal_MultilineKeepsLF(t *testing.T) {
	got := SanitizeTerminal("line1\nline2", false)
	if !strings.ContainsRune(got, '\n') {
		t.Fatalf("multi-line mode must preserve LF, got %q", got)
	}
	if strings.ContainsRune(got, '\r') {
		t.Fatalf("unexpected CR in LF test: %q", got)
	}
}

func TestPrintEventRawEscapesCR(t *testing.T) {
	ev := &streamEvent{
		Payload: json.RawMessage("safe\rEVIL"),
	}
	var buf bytes.Buffer
	if err := printEvent(&buf, ev, "raw", true, false); err != nil {
		t.Fatalf("printEvent raw failed: %v", err)
	}
	if bytes.ContainsRune(buf.Bytes(), '\r') {
		t.Fatalf("raw output contains raw CR: %q", buf.String())
	}
	if !strings.Contains(buf.String(), `safe\rEVIL`) {
		t.Fatalf("raw output must contain escaped CR and payload text: %q", buf.String())
	}
}
