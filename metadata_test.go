package walspool

import (
	"encoding/json"
	"testing"
)

func TestEnrichPayloadMetadata(t *testing.T) {
	payload := []byte(`{"body":"hello","trace_id":"existing"}`)
	got := EnrichPayloadMetadata(payload, "new-trace", "svc", "INFO")
	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Fatalf("unmarshal enriched payload: %v", err)
	}
	if obj["trace_id"] != "existing" {
		t.Fatalf("existing trace_id was overwritten: %v", obj["trace_id"])
	}
	if obj["service"] != "svc" || obj["level"] != "INFO" {
		t.Fatalf("missing injected metadata: %v", obj)
	}
}

func TestEnrichPayloadMetadataPreservesNonObject(t *testing.T) {
	payload := []byte(`hello`)
	if got := EnrichPayloadMetadata(payload, "trace", "svc", "INFO"); string(got) != string(payload) {
		t.Fatalf("non-object payload changed: %q", got)
	}
}
