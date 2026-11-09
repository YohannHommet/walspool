package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/YohannHommet/walspool"
)

func (s *SidecarServer) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if isNDJSON(contentType) {
		s.handleEnqueueNDJSON(w, r)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSingleBodyBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":   "payload_too_large",
				"message": fmt.Sprintf("request body exceeds %d bytes", maxSingleBodyBytes),
			})
			return
		}
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}

	var req EnqueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":"invalid json payload"}`, http.StatusBadRequest)
		return
	}

	// Header trace_id/service/level are hub-indexed only for JSON-object payloads; non-JSON payloads stay byte-identical and unindexed.
	if req.TraceID == "" {
		req.TraceID = strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	}
	if req.Service == "" {
		req.Service = strings.TrimSpace(r.Header.Get("X-Service"))
	}
	if req.Level == "" {
		req.Level = strings.TrimSpace(r.Header.Get("X-Log-Level"))
	}

	if req.Topic == "" && req.Service != "" {
		req.Topic = req.Service
	}

	if req.Topic == "" {
		http.Error(w, `{"error":"topic cannot be empty"}`, http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		http.Error(w, `{"error":"payload cannot be empty"}`, http.StatusBadRequest)
		return
	}

	// If top-level metadata was supplied in EnqueueRequest and payload is a JSON object,
	// ensure payload retains it so WAL log and IngestionObserver receive complete metadata.
	payloadBytes := walspool.EnrichPayloadMetadata([]byte(req.Payload), req.TraceID, req.Service, req.Level)
	if err := walspool.ValidateRecord(req.Topic, payloadBytes, MaxPayloadSize); err != nil {
		writeIngestFault(w, err, 0, 0, 0)
		return
	}

	// 1. Sub-microsecond append to local disk WAL with CRC32 integrity
	// Persists on WAL and automatically notifies registered IngestionObserver (e.g. MemoryLogHub)
	err = s.spooler.Enqueue(r.Context(), req.Topic, payloadBytes)
	if err != nil {
		if errors.Is(err, walspool.ErrSpoolFull) {
			writeSpoolFull(w, 0, 1, 0)
			return
		}
		if isClientFault(err) {
			writeIngestFault(w, err, 0, 0, 0)
			return
		}
		if errors.Is(err, walspool.ErrStorageUnavailable) {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "storage_unavailable", "message": err.Error(), "enqueued": 0, "total": 1})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"internal_error","message":%q}`+"\n", err.Error())))
		return
	}

	if s.metrics != nil {
		s.metrics.RecordIngested(req.Topic)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(EnqueueResponse{
		Status: "accepted",
		Topic:  req.Topic,
		Size:   len(payloadBytes),
	})
}

func (s *SidecarServer) handleEnqueueNDJSON(w http.ResponseWriter, r *http.Request) {
	strictMode := strings.EqualFold(r.URL.Query().Get("strict"), "true") || strings.EqualFold(r.Header.Get("X-Strict"), "true")

	headerTopic := strings.TrimSpace(r.Header.Get("X-Topic"))
	headerService := strings.TrimSpace(r.Header.Get("X-Service"))
	headerTraceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	headerLevel := strings.TrimSpace(r.Header.Get("X-Log-Level"))

	reader := ndjsonReaderPool.Get().(*bufio.Reader)
	reader.Reset(r.Body)
	defer func() {
		reader.Reset(nil)
		ndjsonReaderPool.Put(reader)
	}()

	var bufPtr *[]byte
	defer func() {
		if bufPtr != nil {
			*bufPtr = (*bufPtr)[:0]
			ndjsonBufferPool.Put(bufPtr)
			bufPtr = nil
		}
	}()

	ingested := 0
	errorsCount := 0
	totalBytes := 0
	lineNum := 0
	var firstError *ndjsonLineError
	recordLineError := func(code string, lineErr error) {
		errorsCount++
		if firstError == nil {
			firstError = &ndjsonLineError{Line: lineNum, Code: code, Message: lineErr.Error()}
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}

		segment, isPrefix, err := reader.ReadLine()
		if err != nil && err != io.EOF {
			break
		}

		if len(segment) > 0 || isPrefix {
			if bufPtr == nil {
				if isPrefix {
					bufPtr = ndjsonBufferPool.Get().(*[]byte)
					*bufPtr = (*bufPtr)[:0]
					*bufPtr = append(*bufPtr, segment...)
					if len(*bufPtr) > maxNDJSONLineBytes {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusRequestEntityTooLarge)
						_, _ = w.Write([]byte(`{"error":"line_too_long","message":"line exceeds maximum allowed length of 1048576 bytes"}` + "\n"))
						return
					}
					continue
				}
			} else {
				*bufPtr = append(*bufPtr, segment...)
				if len(*bufPtr) > maxNDJSONLineBytes {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusRequestEntityTooLarge)
					_, _ = w.Write([]byte(`{"error":"line_too_long","message":"line exceeds maximum allowed length of 1048576 bytes"}` + "\n"))
					return
				}
				if isPrefix {
					continue
				}
			}
		}

		var line []byte
		if bufPtr != nil {
			line = *bufPtr
		} else {
			line = segment
		}

		if err == io.EOF && len(line) == 0 {
			break
		}

		lineNum++
		trimmed := bytes.TrimSpace(line)

		if bufPtr != nil {
			*bufPtr = (*bufPtr)[:0]
			ndjsonBufferPool.Put(bufPtr)
			bufPtr = nil
		}

		if len(trimmed) == 0 {
			if err == io.EOF {
				break
			}
			continue
		}

		if !json.Valid(trimmed) {
			if strictMode {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":    "invalid_json_line",
					"line":     lineNum,
					"message":  "invalid json payload",
					"ingested": ingested,
					"errors":   errorsCount,
				})
				return
			}
			recordLineError("invalid_json_line", fmt.Errorf("invalid json payload"))
			if err == io.EOF {
				break
			}
			continue
		}

		var env ndjsonLineEnvelope
		_ = json.Unmarshal(trimmed, &env)

		traceID := env.TraceID
		if traceID == "" {
			traceID = env.TraceId
		}
		if traceID == "" {
			traceID = headerTraceID
		}

		service := env.Service
		if service == "" {
			service = headerService
		}

		level := env.Level
		if level == "" {
			level = env.Severity
		}
		if level == "" {
			level = headerLevel
		}
		if level == "" {
			level = "INFO"
		}

		topic := env.Topic
		if topic == "" {
			topic = headerTopic
		}
		if topic == "" {
			topic = service
		}
		if service == "" && topic != "" {
			service = topic
		}

		if topic == "" {
			if strictMode {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":    "topic_missing",
					"line":     lineNum,
					"message":  "topic cannot be empty",
					"ingested": ingested,
					"errors":   errorsCount,
				})
				return
			}
			recordLineError("topic_missing", fmt.Errorf("topic cannot be empty"))
			if err == io.EOF {
				break
			}
			continue
		}

		var payloadBytes []byte
		hasPayload := len(env.Payload) > 0 && !bytes.Equal(bytes.TrimSpace(env.Payload), []byte("null"))
		if hasPayload {
			payloadBytes = []byte(env.Payload)
		} else {
			payloadBytes = trimmed
		}
		payloadBytes = walspool.EnrichPayloadMetadata(payloadBytes, traceID, service, level)

		if validationErr := walspool.ValidateRecord(topic, payloadBytes, MaxPayloadSize); validationErr != nil {
			if strictMode {
				writeIngestFault(w, validationErr, lineNum, ingested, errorsCount)
				return
			}
			recordLineError(clientFaultCode(validationErr), validationErr)
			if err == io.EOF {
				break
			}
			continue
		}

		enqueueErr := s.spooler.Enqueue(r.Context(), topic, payloadBytes)
		if enqueueErr != nil {
			if errors.Is(enqueueErr, walspool.ErrSpoolFull) {
				writeSpoolFull(w, ingested, 0, lineNum)
				return
			}
			if isClientFault(enqueueErr) {
				if strictMode {
					writeIngestFault(w, enqueueErr, lineNum, ingested, errorsCount)
					return
				}
				recordLineError(clientFaultCode(enqueueErr), enqueueErr)
				if err == io.EOF {
					break
				}
				continue
			}
			if errors.Is(enqueueErr, walspool.ErrStorageUnavailable) {
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":    "storage_unavailable",
					"message":  enqueueErr.Error(),
					"enqueued": ingested,
					"errors":   errorsCount,
					"line":     lineNum,
				})
				return
			}
			if r.Context().Err() != nil {
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":    "internal_error",
				"message":  enqueueErr.Error(),
				"enqueued": ingested,
				"errors":   errorsCount,
				"line":     lineNum,
			})
			return
		}

		if s.metrics != nil {
			s.metrics.RecordIngested(topic)
		}
		ingested++
		totalBytes += len(payloadBytes)

		if err == io.EOF {
			break
		}
	}

	if ingested == 0 && errorsCount == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":   "empty_body",
			"message": "no records found in request",
		})
		return
	}

	if ingested == 0 && errorsCount > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":       "all_lines_invalid",
			"message":     "no valid records ingested",
			"errors":      errorsCount,
			"lines":       lineNum,
			"first_error": firstError,
		})
		return
	}

	status := "accepted"
	if errorsCount > 0 {
		status = "partial_success"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(EnqueueNDJSONResponse{
		Status:     status,
		Ingested:   ingested,
		Enqueued:   ingested,
		Errors:     errorsCount,
		Bytes:      totalBytes,
		Lines:      lineNum,
		FirstError: firstError,
	})
}

var ndjsonReaderPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, ndjsonReaderSize)
	},
}

// ndjsonBufferPool mutualise les accumulateurs pour les lignes longues (> 64 Ko et <= 1 Mo).
var ndjsonBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, ndjsonReaderSize)
		return &b
	},
}

// ndjsonLineEnvelope represente les attributs optionnels extraits a la racine d'une ligne JSON.
type ndjsonLineEnvelope struct {
	Topic    string          `json:"topic"`
	Payload  json.RawMessage `json:"payload"`
	TraceID  string          `json:"trace_id"`
	TraceId  string          `json:"traceId"`
	Service  string          `json:"service"`
	Level    string          `json:"level"`
	Severity string          `json:"severity"`
}

// EnqueueNDJSONResponse definit le format de reponse JSON final du endpoint streaming.
type ndjsonLineError struct {
	Line    int    `json:"line"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type EnqueueNDJSONResponse struct {
	Status     string           `json:"status"`
	Ingested   int              `json:"ingested"`
	Enqueued   int              `json:"enqueued"`
	Errors     int              `json:"errors"`
	Bytes      int              `json:"bytes"`
	Lines      int              `json:"lines"`
	FirstError *ndjsonLineError `json:"first_error,omitempty"`
}

// clientFaultCode classifies core validation sentinels for HTTP responses.
func clientFaultCode(err error) string {
	switch {
	case errors.Is(err, walspool.ErrTopicTooLong):
		return "topic_too_long"
	case errors.Is(err, walspool.ErrEmptyTopic):
		return "topic_missing"
	case errors.Is(err, walspool.ErrPayloadTooLarge):
		return "payload_too_large"
	case errors.Is(err, walspool.ErrEmptyPayload):
		return "payload_empty"
	default:
		return "precondition_failed"
	}
}

func isClientFault(err error) bool {
	return errors.Is(err, walspool.ErrTopicTooLong) ||
		errors.Is(err, walspool.ErrEmptyTopic) ||
		errors.Is(err, walspool.ErrPayloadTooLarge) ||
		errors.Is(err, walspool.ErrEmptyPayload)
}

// writeSpoolFull returns a uniform backpressure response. total is used when
// the full request size is known; line identifies the failing stream line.
func writeSpoolFull(w http.ResponseWriter, enqueued, total, line int) {
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	body := map[string]any{
		"error":    "spool_full",
		"message":  "storage quota exceeded, backpressure active",
		"enqueued": enqueued,
	}
	if total > 0 {
		body["total"] = total
	} else {
		body["line"] = line
	}
	_ = json.NewEncoder(w).Encode(body)
}

// writeIngestFault maps a core validation fault to 400/413 and includes progress.
func writeIngestFault(w http.ResponseWriter, err error, line, ingested, errorsCount int) {
	status := http.StatusBadRequest
	if errors.Is(err, walspool.ErrPayloadTooLarge) {
		status = http.StatusRequestEntityTooLarge
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]any{
		"error":    clientFaultCode(err),
		"message":  err.Error(),
		"ingested": ingested,
		"errors":   errorsCount,
	}
	if line > 0 {
		body["line"] = line
	}
	_ = json.NewEncoder(w).Encode(body)
}

// isNDJSON detecte si le Content-Type indique un flux Newline Delimited JSON ou JSON Lines.
func isNDJSON(contentType string) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	return strings.Contains(ct, "ndjson") || strings.Contains(ct, "jsonl")
}

type EnqueueRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
	TraceID string          `json:"trace_id,omitempty"`
	Service string          `json:"service,omitempty"`
	Level   string          `json:"level,omitempty"`
}

type EnqueueResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
	Size   int    `json:"size_bytes"`
}
