package main

import (
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestSanitizeTerminal_CleanASCII(t *testing.T) {
	input := "user_login successful for alice@example.com (id=42)"
	got := SanitizeTerminal(input, false)
	if got != input {
		t.Fatalf("expected identical clean string, got %q", got)
	}
}

func TestSanitizeTerminal_ANSIEscapeCodes(t *testing.T) {
	input := "\x1b[31;1mALERT: HACKED\x1b[0m"
	got := SanitizeTerminal(input, false)
	if strings.Contains(got, "\x1b") {
		t.Fatalf("sanitized string must not contain raw escape byte 0x1b, got %q", got)
	}
	if !strings.Contains(got, `\x1b[31;1mALERT: HACKED\x1b[0m`) {
		t.Fatalf("expected escaped representation, got %q", got)
	}
}

func TestSanitizeTerminal_UnicodeC1Controls(t *testing.T) {
	// \xC2\x9B is UTF-8 for U+009B (C1 CSI: Control Sequence Introducer)
	input := "header:\xC2\x9B2Jcleared"
	got := SanitizeTerminal(input, false)
	if strings.Contains(got, "\xC2\x9B") {
		t.Fatalf("sanitized string must not contain raw C1 control bytes, got %q", got)
	}
	if !strings.Contains(got, `\u009b2Jcleared`) {
		t.Fatalf("expected escaped representation for C1 control, got %q", got)
	}
}

func TestSanitizeTerminal_BiDiOverride(t *testing.T) {
	// U+202E is Right-to-Left Override (Trojan Source)
	input := "admin\u202Egnirts"
	got := SanitizeTerminal(input, false)
	if strings.Contains(got, "\u202E") {
		t.Fatalf("must neutralize BiDi override rune, got %q", got)
	}
	if !strings.Contains(got, "admin[BIDI]gnirts") {
		t.Fatalf("expected [BIDI] marker, got %q", got)
	}
}

func TestSanitizeTerminal_LogForgerySingleLine(t *testing.T) {
	input := "service-auth\n2026-10-13 [INFO] forged log line\r\n"
	got := SanitizeTerminal(input, true)
	if strings.Contains(got, "\n") || strings.Contains(got, "\r") {
		t.Fatalf("singleLine mode must not contain raw newline characters, got %q", got)
	}
	if !strings.Contains(got, `service-auth\n2026-10-13 [INFO] forged log line\r\n`) {
		t.Fatalf("expected escaped newlines in singleLine mode, got %q", got)
	}
}

func TestBoundedLineReader_NormalAndTruncated(t *testing.T) {
	stream := "short line 1\n" +
		strings.Repeat("A", 100) + "\n" +
		"short line 2\n"

	// Limit line length to 30 bytes
	reader := NewBoundedLineReader(strings.NewReader(stream), 30)

	// Line 1
	line1, trunc1, err := reader.ReadLine()
	if err != nil || trunc1 || string(line1) != "short line 1" {
		t.Fatalf("line 1 mismatch: line=%q, trunc=%v, err=%v", string(line1), trunc1, err)
	}

	// Line 2: 100 'A's -> truncated to 30 bytes
	line2, trunc2, err := reader.ReadLine()
	if err != nil || !trunc2 || len(line2) != 30 || string(line2) != strings.Repeat("A", 30) {
		t.Fatalf("line 2 expected truncation to 30: len=%d, trunc=%v, err=%v", len(line2), trunc2, err)
	}

	// Line 3: resumes normally after truncated line
	line3, trunc3, err := reader.ReadLine()
	if err != nil || trunc3 || string(line3) != "short line 2" {
		t.Fatalf("line 3 mismatch: line=%q, trunc=%v, err=%v", string(line3), trunc3, err)
	}

	// EOF
	_, _, err = reader.ReadLine()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestCheckBrokenPipe(t *testing.T) {
	if CheckBrokenPipe(nil) {
		t.Fatalf("nil error should not be broken pipe")
	}
	if !CheckBrokenPipe(syscall.EPIPE) {
		t.Fatalf("syscall.EPIPE must be recognized")
	}
	if !CheckBrokenPipe(&os.PathError{Op: "write", Path: "/dev/stdout", Err: syscall.EPIPE}) {
		t.Fatalf("PathError with EPIPE must be recognized")
	}
}
