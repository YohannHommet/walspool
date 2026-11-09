package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/YohannHommet/walspool"
)

func (s *SidecarServer) handleEnqueueBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.IsShuttingDown() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"shutting_down","message":"server is shutting down"}` + "\n"))
		return
	}

	// 1. Memory bounding protection: limit request stream to 10MB
	r.Body = http.MaxBytesReader(w, r.Body, MaxBatchBytes)

	// 2. Decode JSON array input
	var batch []EnqueueRequest
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&batch); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) || strings.Contains(err.Error(), "request body too large") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_, _ = w.Write([]byte(`{"error":"payload_too_large","message":"request body exceeds maximum limit of 10MB"}` + "\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_json","message":"expected JSON array of enqueue requests"}` + "\n"))
		return
	}

	// 3. Batch cardinality validation
	if len(batch) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"empty_batch","message":"batch must contain at least 1 item"}` + "\n"))
		return
	}

	if len(batch) > MaxBatchSize {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = w.Write([]byte(`{"error":"batch_too_large","message":"batch size exceeds maximum limit of 1000 items"}` + "\n"))
		return
	}

	// Default request headers for inheritance
	defaultTraceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	defaultService := strings.TrimSpace(r.Header.Get("X-Service"))
	defaultLevel := strings.TrimSpace(r.Header.Get("X-Log-Level"))

	// -------------------------------------------------------------------------
	// Phase 1: Pre-validation in memory (Strict All-or-Nothing)
	// -------------------------------------------------------------------------
	prepared := make([]preparedBatchItem, len(batch))
	totalBytes := 0

	for i := range batch {
		item := &batch[i]

		traceID := strings.TrimSpace(item.TraceID)
		if traceID == "" {
			traceID = defaultTraceID
		}
		service := strings.TrimSpace(item.Service)
		if service == "" {
			service = defaultService
		}
		level := strings.TrimSpace(item.Level)
		if level == "" {
			level = defaultLevel
		}

		topic := strings.TrimSpace(item.Topic)
		if topic == "" && service != "" {
			topic = service
		}

		if topic == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(batchErrorItemResponse{
				Error:   "invalid_batch_item",
				Index:   i,
				Message: "topic cannot be empty",
			})
			return
		}
		if len(topic) > walspool.MaxTopicBytes {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(batchErrorItemResponse{
				Error:   "invalid_batch_item",
				Index:   i,
				Message: fmt.Sprintf("topic length %d exceeds limit of %d bytes", len(topic), walspool.MaxTopicBytes),
			})
			return
		}

		trimmedPayload := bytes.TrimSpace(item.Payload)
		if len(trimmedPayload) == 0 || bytes.Equal(trimmedPayload, []byte("null")) || bytes.Equal(trimmedPayload, []byte(`""`)) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(batchErrorItemResponse{
				Error:   "invalid_batch_item",
				Index:   i,
				Message: "payload cannot be empty",
			})
			return
		}

		if len(item.Payload) > MaxPayloadSize {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(batchErrorItemResponse{
				Error:   "invalid_batch_item",
				Index:   i,
				Message: fmt.Sprintf("payload size exceeds limit of %d bytes", MaxPayloadSize),
			})
			return
		}

		payloadBytes := enrichPayloadMetadata([]byte(item.Payload), traceID, service, level)
		// Enrichment can add trace_id/service/level keys and grow the payload.
		// Re-validate the final bytes so a client error is not deferred to the
		// engine and reported as a 500 after earlier items were already spooled.
		if len(payloadBytes) > MaxPayloadSize {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(batchErrorItemResponse{
				Error:   "invalid_batch_item",
				Index:   i,
				Message: fmt.Sprintf("payload size exceeds limit of %d bytes after metadata enrichment", MaxPayloadSize),
			})
			return
		}
		prepared[i] = preparedBatchItem{
			topic:   topic,
			payload: payloadBytes,
		}
		totalBytes += len(payloadBytes)
	}

	// -------------------------------------------------------------------------
	// Phase 2: Sequential Ingestion into Spooler (Execution Phase)
	// -------------------------------------------------------------------------
	ctx := r.Context()
	enqueuedCount := 0

	for i := range prepared {
		err := s.spooler.Enqueue(ctx, prepared[i].topic, prepared[i].payload)
		if err != nil {
			if errors.Is(err, walspool.ErrSpoolFull) {
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(batchFailureResponse{
					Error:    "spool_full",
					Message:  "storage capacity exceeded, backpressure active",
					Enqueued: enqueuedCount,
					Total:    len(prepared),
				})
				return
			}

			if errors.Is(err, walspool.ErrPreconditionViolated) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(batchErrorItemResponse{
					Error:    "invalid_batch_item",
					Index:    i,
					Message:  err.Error(),
					Enqueued: enqueuedCount,
					Total:    len(prepared),
				})
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(batchFailureResponse{
				Error:    "internal_error",
				Message:  err.Error(),
				Enqueued: enqueuedCount,
				Total:    len(prepared),
			})
			return
		}

		if s.metrics != nil {
			s.metrics.RecordIngested(prepared[i].topic)
		}
		enqueuedCount++
	}

	// Full batch ingestion successful
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(BatchEnqueueResponse{
		Status:   "ok",
		Enqueued: enqueuedCount,
		Size:     totalBytes,
	})
}

type BatchEnqueueResponse struct {
	Status   string `json:"status"`
	Enqueued int    `json:"enqueued"`
	Size     int    `json:"size_bytes"`
}

type preparedBatchItem struct {
	topic   string
	payload []byte
}

type batchErrorItemResponse struct {
	Error   string `json:"error"`
	Index   int    `json:"index"`
	Message string `json:"message"`
	// Enqueued/Total are populated only for Phase 2 partial failures; they are
	// omitted for Phase 1 validation failures, where no record was spooled.
	Enqueued int `json:"enqueued,omitempty"`
	Total    int `json:"total,omitempty"`
}

type batchFailureResponse struct {
	Error    string `json:"error"`
	Message  string `json:"message"`
	Enqueued int    `json:"enqueued"`
	Total    int    `json:"total"`
}
