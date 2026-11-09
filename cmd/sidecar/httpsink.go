package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/YohannHommet/walspool"
)

type HTTPSink struct {
	TargetURL  string
	HTTPClient *http.Client
	Metrics    *SidecarMetrics
}

type BatchItem struct {
	Offset    uint64          `json:"offset"`
	ID        uint64          `json:"id"`
	Timestamp int64           `json:"timestamp_nano"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
}

func (s *HTTPSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	if s.TargetURL == "" {
		// If no target sink configured, log to stdout for testing/demo
		for _, rec := range batch {
			slog.Debug("[SINK CONSOLE] Shipped", "offset", rec.Offset, "topic", rec.Topic, "bytes", len(rec.Payload))
			if s.Metrics != nil {
				s.Metrics.RecordDelivered(rec.Topic)
			}
		}
		return nil
	}

	items := make([]BatchItem, len(batch))
	if s.Metrics != nil {
		s.Metrics.RecordSinkFlush()
	}
	for i, r := range batch {
		// Try to preserve raw JSON if payload is valid JSON, else quote as string
		var raw json.RawMessage
		if json.Valid(r.Payload) {
			raw = json.RawMessage(r.Payload)
		} else {
			quoted, _ := json.Marshal(string(r.Payload))
			raw = json.RawMessage(quoted)
		}

		items[i] = BatchItem{
			Offset:    uint64(r.Offset),
			ID:        r.ID,
			Timestamp: r.Timestamp.UnixNano(),
			Topic:     r.Topic,
			Payload:   raw,
		}
	}

	body, err := json.Marshal(items)
	if err != nil {
		if s.Metrics != nil {
			s.Metrics.RecordSinkError()
		}
		return fmt.Errorf("%w: failed to encode batch: %v", walspool.ErrPermanentRejection, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.TargetURL, bytes.NewReader(body))
	if err != nil {
		if s.Metrics != nil {
			s.Metrics.RecordSinkError()
		}
		return fmt.Errorf("%w: invalid request: %v", walspool.ErrPermanentRejection, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "walspool-sidecar/1.0")

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		if s.Metrics != nil {
			s.Metrics.RecordSinkError()
			s.Metrics.RecordSinkRetry()
		}
		return fmt.Errorf("%w: network transport fault: %v", walspool.ErrSinkUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if s.Metrics != nil {
			for _, rec := range batch {
				s.Metrics.RecordDelivered(rec.Topic)
			}
		}
		return nil
	}

	// HTTP 429 (Too Many Requests) and HTTP 408 (Request Timeout) are transient conditions -> retry with backoff
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		slog.Warn("[SINK TRANSIENT] Downstream transient error", "status", resp.StatusCode, "body", string(respBody))
		if s.Metrics != nil {
			s.Metrics.RecordSinkError()
			s.Metrics.RecordSinkRetry()
		}
		return fmt.Errorf("%w: destination returned transient HTTP %d", walspool.ErrSinkUnavailable, resp.StatusCode)
	}

	// 4xx client errors mean downstream rejected this payload format permanently (400, 422, etc.)
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		slog.Error("[SINK 4XX] Downstream permanently rejected batch", "status", resp.StatusCode, "body", string(respBody))
		if s.Metrics != nil {
			s.Metrics.RecordSinkError()
		}
		return walspool.ErrPermanentRejection
	}

	// 5xx server errors indicate transient downstream failure -> trigger exponential backoff
	if s.Metrics != nil {
		s.Metrics.RecordSinkError()
		s.Metrics.RecordSinkRetry()
	}
	return fmt.Errorf("%w: destination returned HTTP %d", walspool.ErrSinkUnavailable, resp.StatusCode)
}

// SidecarMetrics collects operational counters and gauges for Prometheus exposition.
