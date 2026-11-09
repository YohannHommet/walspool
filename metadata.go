package walspool

import (
	"bytes"
	"encoding/json"
)

// EnrichPayloadMetadata injects top-level trace/service/level fields into a JSON
// object payload so IngestionObserver can index them without re-parsing protocol
// headers. It is a core wire-metadata contract, not an ingress-adapter detail.
// Existing fields win; non-object or malformed payloads are returned unchanged.
func EnrichPayloadMetadata(payload []byte, traceID, service, level string) []byte {
	if traceID == "" && service == "" && level == "" {
		return payload
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return payload
	}
	// UseNumber avoids float64 rounding of 64-bit trace IDs / nanosecond timestamps on re-marshal.
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var obj map[string]any
	if err := decoder.Decode(&obj); err != nil || obj == nil {
		return payload
	}
	modified := false
	if traceID != "" && obj["trace_id"] == nil && obj["traceId"] == nil {
		obj["trace_id"] = traceID
		modified = true
	}
	if service != "" && obj["service"] == nil {
		obj["service"] = service
		modified = true
	}
	if level != "" && obj["level"] == nil && obj["severity"] == nil {
		obj["level"] = level
		modified = true
	}
	if modified {
		if merged, err := json.Marshal(obj); err == nil {
			return merged
		}
	}
	return payload
}
