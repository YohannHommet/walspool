package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/YohannHommet/walspool"
)

// Client handles communication with a running walspool-sidecar instance.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	SSEClient  *http.Client
}

// NewClient parses, normalizes and validates the target sidecar endpoint URL.
func NewClient(rawURL string) (*Client, error) {
	addr := strings.TrimSpace(rawURL)
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid sidecar address %q: %w", rawURL, err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported address scheme %q: only http and https are allowed", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("missing host in sidecar address %q", rawURL)
	}

	cleanBase := strings.TrimRight(u.String(), "/")

	// Short timeout client for unary queries (stats, health, metrics)
	unaryClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 3 * time.Second,
		},
	}

	// Infinite timeout client for SSE streaming (Timeout must be 0)
	sseClient := &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   3 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}

	return &Client{
		BaseURL:    cleanBase,
		HTTPClient: unaryClient,
		SSEClient:  sseClient,
	}, nil
}

// OpenStream initiates a long-lived Server-Sent Events HTTP connection to /v1/logs/stream.
func (c *Client) OpenStream(ctx context.Context, service, level string) (io.ReadCloser, error) {
	reqURL := c.BaseURL + "/v1/logs/stream"
	params := url.Values{}
	if service != "" {
		params.Set("service", service)
	}
	if level != "" {
		params.Set("level", level)
	}
	if q := params.Encode(); q != "" {
		reqURL += "?" + q
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.SSEClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to sidecar stream at %s: %w", reqURL, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("sidecar stream returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return resp.Body, nil
}

// FetchStats queries GET /v1/logs/stats and parses the HubStats payload.
func (c *Client) FetchStats(ctx context.Context) (*walspool.HubStats, error) {
	reqURL := c.BaseURL + "/v1/logs/stats"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sidecar unreachable at %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats endpoint returned status %d", resp.StatusCode)
	}

	var stats walspool.HubStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("failed to decode stats payload: %w", err)
	}

	return &stats, nil
}

// FetchMetrics queries GET /metrics and returns raw Prometheus-formatted text metrics.
func (c *Client) FetchMetrics(ctx context.Context) (string, error) {
	reqURL := c.BaseURL + "/metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sidecar unreachable at %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", fmt.Errorf("failed to read metrics body: %w", err)
	}

	return string(body), nil
}
