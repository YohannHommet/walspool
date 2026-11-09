package walspool

import (
	"bytes"
	"strconv"
)

func extractMetadataFromPayload(topic string, payload []byte) (traceID, service, level string) {
	b := bytes.TrimSpace(payload)
	if len(b) >= 2 && b[0] == '{' {
		traceID, service, level = scanTopLevelMeta(b)
	}

	if service == "" {
		service = topic
	}
	if level == "" {
		level = "INFO"
	}
	return traceID, service, level
}

// scanTopLevelMeta extracts trace_id/traceId, service, and level/severity from the top level of a
// JSON object without a full unmarshal, keeping the hot path alloc-light. Nested objects/arrays are
// skipped so an inner key never shadows a top-level one; malformed input bails with whatever was parsed so far.
func scanTopLevelMeta(b []byte) (traceID, service, level string) {
	var snake, camel, lvl, sev string
	i, n := 1, len(b) // skip '{'
	for i < n {
		i = skipWS(b, i)
		if i >= n || b[i] == '}' || b[i] != '"' {
			break
		}
		ks, ke, ki, _, ok := scanString(b, i)
		if !ok {
			break
		}
		i = skipWS(b, ki)
		if i >= n || b[i] != ':' {
			break
		}
		i = skipWS(b, i+1)
		if i >= n {
			break
		}

		// string(b[ks:ke]) in a switch is compiler-optimized to byte comparisons, so classifying the key never allocates.
		field := 0 // 1 trace_id, 2 traceId, 3 service, 4 level, 5 severity
		switch string(b[ks:ke]) {
		case "trace_id":
			field = 1
		case "traceId":
			field = 2
		case "service":
			field = 3
		case "level":
			field = 4
		case "severity":
			field = 5
		}

		switch b[i] {
		case '"':
			vs, ve, vi, esc, vok := scanString(b, i)
			if !vok {
				return firstNonEmpty(snake, camel), service, firstNonEmpty(lvl, sev)
			}
			if field != 0 {
				val := materializeString(b, vs, ve, esc)
				switch field {
				case 1:
					snake = val
				case 2:
					camel = val
				case 3:
					service = val
				case 4:
					lvl = val
				case 5:
					sev = val
				}
			}
			i = vi
		case '{', '[':
			if i = skipContainer(b, i); i < 0 {
				return firstNonEmpty(snake, camel), service, firstNonEmpty(lvl, sev)
			}
		default:
			i = skipScalar(b, i)
		}

		i = skipWS(b, i)
		if i < n && b[i] == ',' {
			i++
		}
	}
	return firstNonEmpty(snake, camel), service, firstNonEmpty(lvl, sev)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func skipWS(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanString locates a JSON string starting at b[i]=='"' without allocating: it returns the content
// byte range [start,end), the index just past the closing quote, and whether the value held escapes.
func scanString(b []byte, i int) (start, end, next int, hasEscape, ok bool) {
	i++ // skip opening quote
	start = i
	for i < len(b) {
		switch b[i] {
		case '\\':
			hasEscape = true
			i += 2
		case '"':
			return start, i, i + 1, hasEscape, true
		default:
			i++
		}
	}
	return 0, 0, i, false, false
}

// materializeString turns a scanned string range [start,end) into a Go string. The common escape-free
// case is a single copy; an escaped value is decoded via strconv.Unquote on the full quoted token
// b[start-1:end+1]: start-1/end index the opening/closing quotes, which scanString guarantees exist.
func materializeString(b []byte, start, end int, hasEscape bool) string {
	if !hasEscape {
		return string(b[start:end])
	}
	if unq, err := strconv.Unquote(string(b[start-1 : end+1])); err == nil {
		return unq
	}
	return string(b[start:end])
}

// skipContainer advances past a balanced object/array (b[i] is '{' or '['), respecting strings.
// Returns -1 on malformed input.
func skipContainer(b []byte, i int) int {
	depth := 0
	for i < len(b) {
		switch b[i] {
		case '"':
			_, _, ni, _, ok := scanString(b, i)
			if !ok {
				return -1
			}
			i = ni
		case '{', '[':
			depth++
			i++
		case '}', ']':
			depth--
			i++
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return -1
}

// skipScalar advances past a bare number/true/false/null until the next structural delimiter.
func skipScalar(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ',', '}', ']', ' ', '\t', '\n', '\r':
			return i
		default:
			i++
		}
	}
	return i
}
