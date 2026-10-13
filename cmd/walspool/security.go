package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"unicode/utf8"
)

var (
	// ErrLineTooLong indicates an incoming SSE line exceeded the safety bound.
	ErrLineTooLong = errors.New("sse: event line exceeds maximum allowed memory limit")

	hexDigits = "0123456789abcdef"
)

// SanitizeTerminal neutralizes potential terminal escape sequences, C0 and C1 control characters,
// and bidirectional overrides before outputting untrusted text to a console.
// If singleLine is true, newline characters are converted to literal \n to prevent log forgery (CWE-117).
func SanitizeTerminal(s string, singleLine bool) string {
	// Fast path: pure printable ASCII with allowed whitespace requires no allocation.
	clean := true
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x20 && b != '\t' && (singleLine || (b != '\n' && b != '\r')) {
			clean = false
			break
		}
		if b >= 0x7F {
			clean = false
			break
		}
	}
	if clean {
		return s
	}

	var buf strings.Builder
	buf.Grow(len(s))

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size

		switch {
		case r == utf8.RuneError:
			buf.WriteString(`\uFFFD`)
		case r == '\t':
			buf.WriteRune('\t')
		case r == '\n':
			if singleLine {
				buf.WriteString(`\n`)
			} else {
				buf.WriteRune('\n')
			}
		case r == '\r':
			if singleLine {
				buf.WriteString(`\r`)
			} else {
				buf.WriteRune('\r')
			}
		case r < 0x20, r == 0x7F:
			// C0 control codes & DEL (e.g. \x1b ESC, \b backspace, bell)
			buf.WriteString(`\x`)
			buf.WriteByte(hexDigits[r>>4])
			buf.WriteByte(hexDigits[r&0x0F])
		case r >= 0x0080 && r <= 0x009F:
			// C1 control codes (including UTF-8 encoded CSI 0x9B and OSC 0x9D)
			buf.WriteString(`\u00`)
			buf.WriteByte(hexDigits[r>>4])
			buf.WriteByte(hexDigits[r&0x0F])
		case (r >= 0x202A && r <= 0x202E) || (r >= 0x2066 && r <= 0x2069):
			// Trojan Source / BiDi direction override codes
			buf.WriteString(`[BIDI]`)
		case (r >= 0x200B && r <= 0x200F) || r == 0xFEFF:
			// Zero-width characters and BOM stripped silently
			continue
		default:
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

// IsTerminal returns true if the specified file is an interactive character terminal.
func IsTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// CheckBrokenPipe detects if an error corresponds to SIGPIPE / EPIPE or broken stdout.
func CheckBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && errors.Is(pathErr.Err, syscall.EPIPE) {
		return true
	}
	return false
}

// BoundedLineReader wraps a buffered reader to enforce an absolute byte ceiling on lines,
// preventing unbounded allocation attacks from runaway or un-terminated SSE streams.
type BoundedLineReader struct {
	r        *bufio.Reader
	maxBytes int
	buf      bytes.Buffer
}

// NewBoundedLineReader constructs a line reader capped at maxBytes per line.
func NewBoundedLineReader(r io.Reader, maxBytes int) *BoundedLineReader {
	if maxBytes <= 0 {
		maxBytes = 1024 * 1024 // 1 MB default safety bound
	}
	return &BoundedLineReader{
		r:        bufio.NewReaderSize(r, 64*1024),
		maxBytes: maxBytes,
	}
}

// ReadLine reads until the next '\n' or EOF. If the line length exceeds maxBytes,
// it truncates the returned slice and returns truncated = true.
func (b *BoundedLineReader) ReadLine() (line []byte, truncated bool, err error) {
	b.buf.Reset()
	truncated = false

	for {
		chunk, isPrefix, readErr := b.r.ReadLine()
		if readErr != nil {
			if len(chunk) > 0 {
				b.buf.Write(chunk)
			}
			return b.buf.Bytes(), truncated, readErr
		}

		if !truncated {
			if b.buf.Len()+len(chunk) > b.maxBytes {
				allowed := b.maxBytes - b.buf.Len()
				if allowed > 0 {
					b.buf.Write(chunk[:allowed])
				}
				truncated = true
			} else {
				b.buf.Write(chunk)
			}
		}

		if !isPrefix {
			break
		}
	}

	return b.buf.Bytes(), truncated, nil
}
