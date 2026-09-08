package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/YohannHommet/walspool"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MaxOTLPRequestBodyLimit limits incoming OTLP batches to 10 MiB to prevent memory exhaustion DOS.
const MaxOTLPRequestBodyLimit = 10 * 1024 * 1024

// handleOTLPLogs processes OpenTelemetry HTTP Logs ingestion requests (POST /v1/logs).
// It supports standard OTLP Protobuf (application/x-protobuf) and OTLP JSON (application/json) payloads.
func (s *SidecarServer) handleOTLPLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxOTLPRequestBodyLimit))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "read_failed",
			"message": "failed to read request body",
		})
		return
	}

	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	var req collogspb.ExportLogsServiceRequest
	isProtobuf := false

	if strings.Contains(contentType, "application/x-protobuf") || strings.Contains(contentType, "application/protobuf") {
		isProtobuf = true
		if err := proto.Unmarshal(body, &req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "invalid_protobuf",
				"message": fmt.Sprintf("failed to parse OTLP protobuf: %v", err),
			})
			return
		}
	} else if strings.Contains(contentType, "application/json") || contentType == "" {
		unmarshaler := protojson.UnmarshalOptions{DiscardUnknown: true}
		if err := unmarshaler.Unmarshal(body, &req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "invalid_otlp_json",
				"message": fmt.Sprintf("failed to parse OTLP JSON: %v", err),
			})
			return
		}
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnsupportedMediaType)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "unsupported_media_type",
			"message": "expected application/x-protobuf or application/json",
		})
		return
	}

	// Ingest all log records into Walspool's persistent WAL and live memory hub
	for _, resLogs := range req.ResourceLogs {
		serviceName := extractResourceServiceName(resLogs.Resource, s.otlpDefaultTopic)
		resAttrs := attributesToMap(resLogs.Resource.GetAttributes())

		for _, scopeLogs := range resLogs.ScopeLogs {
			scopeName := ""
			if scopeLogs.Scope != nil {
				scopeName = scopeLogs.Scope.Name
			}

			for _, logRec := range scopeLogs.LogRecords {
				topic := serviceName
				logAttrs := attributesToMap(logRec.Attributes)
				if svc, ok := logAttrs["service.name"].(string); ok && svc != "" {
					topic = svc
				}

				// Timestamp resolution
				var ts time.Time
				if logRec.TimeUnixNano > 0 {
					ts = time.Unix(0, int64(logRec.TimeUnixNano))
				} else if logRec.ObservedTimeUnixNano > 0 {
					ts = time.Unix(0, int64(logRec.ObservedTimeUnixNano))
				} else {
					ts = time.Now()
				}

				// Severity level resolution
				level := strings.ToUpper(strings.TrimSpace(logRec.SeverityText))
				if level == "" {
					level = severityNumberToText(logRec.SeverityNumber)
				}

				// Trace & Span IDs
				traceID := resolveTraceID(logRec.TraceId, isProtobuf)
				spanID := resolveSpanID(logRec.SpanId, isProtobuf)

				bodyVal := anyValueToInterface(logRec.Body)

				// Normalized JSON representation:
				// Placing service, level, and trace_id at the top level allows
				// MemoryLogHub's OnIngested observer to index them directly without overhead.
				payloadMap := map[string]any{
					"timestamp": ts.Format(time.RFC3339Nano),
					"service":   topic,
					"level":     level,
					"body":      bodyVal,
				}
				if traceID != "" {
					payloadMap["trace_id"] = traceID
				}
				if spanID != "" {
					payloadMap["span_id"] = spanID
				}
				if scopeName != "" {
					payloadMap["scope"] = scopeName
				}
				if len(logAttrs) > 0 {
					payloadMap["attributes"] = logAttrs
				}
				if len(resAttrs) > 0 {
					payloadMap["resource"] = resAttrs
				}

				payloadBytes, err := json.Marshal(payloadMap)
				if err != nil {
					payloadBytes = []byte(fmt.Sprintf(`{"service":%q,"level":%q,"body":%q}`, topic, level, fmt.Sprint(bodyVal)))
				}

				if err := s.spooler.Enqueue(r.Context(), topic, payloadBytes); err != nil {
					if errors.Is(err, walspool.ErrSpoolFull) {
						w.Header().Set("Retry-After", "1")
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusServiceUnavailable)
						_ = json.NewEncoder(w).Encode(map[string]string{
							"error":   "spool_full",
							"message": "storage capacity exceeded, backpressure active",
						})
						return
					}
					if errors.Is(err, walspool.ErrSpoolerClosed) {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusServiceUnavailable)
						_ = json.NewEncoder(w).Encode(map[string]string{
							"error":   "spooler_closed",
							"message": "server shutting down",
						})
						return
					}

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error":   "internal_error",
						"message": err.Error(),
					})
					return
				}

				if s.metrics != nil {
					s.metrics.RecordIngested(topic)
				}
			}
		}
	}

	// Success response conforming to OTLP HTTP specification
	if isProtobuf {
		respProto, err := proto.Marshal(&collogspb.ExportLogsServiceResponse{})
		if err == nil {
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respProto)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}` + "\n"))
}

func severityNumberToText(num logspb.SeverityNumber) string {
	switch {
	case num >= 1 && num <= 8:
		return "DEBUG"
	case num >= 9 && num <= 12:
		return "INFO"
	case num >= 13 && num <= 16:
		return "WARN"
	case num >= 17 && num <= 20:
		return "ERROR"
	case num >= 21 && num <= 24:
		return "FATAL"
	default:
		return "INFO"
	}
}

func anyValueToInterface(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		if val.ArrayValue == nil {
			return nil
		}
		res := make([]any, len(val.ArrayValue.Values))
		for i, item := range val.ArrayValue.Values {
			res[i] = anyValueToInterface(item)
		}
		return res
	case *commonpb.AnyValue_KvlistValue:
		if val.KvlistValue == nil {
			return nil
		}
		res := make(map[string]any, len(val.KvlistValue.Values))
		for _, kv := range val.KvlistValue.Values {
			if kv != nil {
				res[kv.Key] = anyValueToInterface(kv.Value)
			}
		}
		return res
	default:
		return nil
	}
}

func attributesToMap(attrs []*commonpb.KeyValue) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		if attr != nil && attr.Key != "" {
			m[attr.Key] = anyValueToInterface(attr.Value)
		}
	}
	return m
}

func extractResourceServiceName(res *resourcepb.Resource, defaultTopic string) string {
	if res != nil {
		for _, attr := range res.Attributes {
			if attr != nil && attr.Key == "service.name" {
				if val := attr.Value.GetStringValue(); val != "" {
					return val
				}
			}
		}
	}
	if defaultTopic != "" {
		return defaultTopic
	}
	return "otel_logs"
}

// resolveTraceID extracts the 32-character hexadecimal trace ID string.
// For Protobuf payloads, trace_id is 16 raw binary bytes.
// For JSON payloads decoded via protojson, the 32-char hex string was treated as base64
// and decoded into 24 bytes (32 * 6 / 8 = 24). Re-encoding returns the original 32 hex chars.
func resolveTraceID(raw []byte, isProtobuf bool) string {
	if len(raw) == 0 {
		return ""
	}
	if isProtobuf {
		return hex.EncodeToString(raw)
	}
	if len(raw) == 24 {
		re := base64.StdEncoding.EncodeToString(raw)
		if len(re) == 32 {
			return re
		}
	}
	if len(raw) == 16 {
		return hex.EncodeToString(raw)
	}
	return hex.EncodeToString(raw)
}

// resolveSpanID extracts the 16-character hexadecimal span ID string.
// For Protobuf payloads, span_id is 8 raw binary bytes.
// For JSON payloads decoded via protojson, the 16-char hex string was treated as base64
// and decoded into 12 bytes. Re-encoding returns the original 16 hex chars.
func resolveSpanID(raw []byte, isProtobuf bool) string {
	if len(raw) == 0 {
		return ""
	}
	if isProtobuf {
		return hex.EncodeToString(raw)
	}
	if len(raw) == 12 {
		re := base64.StdEncoding.EncodeToString(raw)
		if len(re) == 16 {
			return re
		}
	}
	if len(raw) == 8 {
		return hex.EncodeToString(raw)
	}
	return hex.EncodeToString(raw)
}
