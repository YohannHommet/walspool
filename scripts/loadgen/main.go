package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	url      string
	workers  int
	duration time.Duration
	count    uint64
	rate     int
	quiet    bool
}

type stats struct {
	totalSent     uint64
	successCount  uint64
	rejectedCount uint64
	errorCount    uint64
	totalBytes    uint64
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.url, "url", "http://127.0.0.1:8080/v1/enqueue", "Target HTTP enqueue endpoint")
	flag.IntVar(&cfg.workers, "workers", 16, "Number of concurrent worker goroutines")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "Test duration (e.g. 5s, 10s, 30s)")
	flag.Uint64Var(&cfg.count, "count", 0, "Max requests to send (0 = unlimited until duration)")
	flag.IntVar(&cfg.rate, "rate", 0, "Target requests/sec across all workers (0 = unlimited)")
	flag.BoolVar(&cfg.quiet, "quiet", false, "Suppress per-second progress logs")
	flag.Parse()

	if cfg.workers <= 0 {
		cfg.workers = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}

	var st stats
	// Pre-allocated sampled latency collection (sample 1 in 5 if massive volume)
	const maxLatencies = 200000
	latencies := make([]int32, 0, maxLatencies) // in microseconds
	var latMu sync.Mutex

	recordLatency := func(us int32) {
		latMu.Lock()
		if len(latencies) < maxLatencies {
			latencies = append(latencies, us)
		}
		latMu.Unlock()
	}

	payloadTemplate := []byte(`{"topic":"benchmarks","service":"stress-svc","level":"INFO","trace_id":"tr-load-test","payload":{"message":"walspool high-throughput load event","timestamp":"2026-10-13T17:00:00Z","metric":42}}`)
	payloadLen := uint64(len(payloadTemplate))

	if !cfg.quiet {
		fmt.Printf("🚀 Starting Walspool Load Generator\n")
		fmt.Printf("   Endpoint : %s\n", cfg.url)
		fmt.Printf("   Workers  : %d goroutines\n", cfg.workers)
		if cfg.rate > 0 {
			fmt.Printf("   Target   : %d requests/sec\n", cfg.rate)
		} else {
			fmt.Printf("   Target   : MAX THROTTLE (Unlimited)\n")
		}
		fmt.Printf("   Duration : %s\n\n", cfg.duration)
	}

	startTime := time.Now()
	var wg sync.WaitGroup

	// Rate limiter channel if rate > 0
	var rateTicker *time.Ticker
	var limiter chan struct{}
	if cfg.rate > 0 {
		limiter = make(chan struct{}, cfg.rate)
		interval := time.Second / time.Duration(cfg.rate)
		if interval < time.Microsecond {
			interval = time.Microsecond
		}
		rateTicker = time.NewTicker(interval)
		defer rateTicker.Stop()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-rateTicker.C:
					select {
					case limiter <- struct{}{}:
					default:
					}
				}
			}
		}()
	}

	// Worker loop
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			reqBodyReader := bytes.NewReader(payloadTemplate)

			for {
				if ctx.Err() != nil {
					return
				}

				if cfg.count > 0 && atomic.LoadUint64(&st.totalSent) >= cfg.count {
					return
				}

				if limiter != nil {
					select {
					case <-ctx.Done():
						return
					case <-limiter:
					}
				}

				atomic.AddUint64(&st.totalSent, 1)

				reqBodyReader.Reset(payloadTemplate)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.url, reqBodyReader)
				if err != nil {
					atomic.AddUint64(&st.errorCount, 1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				t0 := time.Now()
				resp, err := client.Do(req)
				latencyUs := int32(time.Since(t0).Microseconds())

				if err != nil {
					if ctx.Err() == nil {
						atomic.AddUint64(&st.errorCount, 1)
					}
					continue
				}

				// Consume and discard body
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				atomic.AddUint64(&st.totalBytes, payloadLen)
				recordLatency(latencyUs)

				switch {
				case resp.StatusCode >= 200 && resp.StatusCode < 300:
					atomic.AddUint64(&st.successCount, 1)
				case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable:
					atomic.AddUint64(&st.rejectedCount, 1)
				default:
					atomic.AddUint64(&st.errorCount, 1)
				}
			}
		}(i)
	}

	// Progress reporter loop
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastSent uint64
		lastTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				elapsed := t.Sub(lastTime).Seconds()
				currentSent := atomic.LoadUint64(&st.totalSent)
				currentSuccess := atomic.LoadUint64(&st.successCount)
				currentRejected := atomic.LoadUint64(&st.rejectedCount)
				currentErrors := atomic.LoadUint64(&st.errorCount)

				eps := float64(currentSent-lastSent) / elapsed
				lastSent = currentSent
				lastTime = t

				if !cfg.quiet {
					totalElapsed := int(time.Since(startTime).Seconds())
					fmt.Printf("[%02ds] Ingestion: %8.0f ev/s | Success: %-7d | Rejected: %-5d | Errors: %d\n",
						totalElapsed, eps, currentSuccess, currentRejected, currentErrors)
				}
			}
		}
	}()

	// Wait for duration or count completion
	if cfg.duration > 0 {
		go func() {
			time.Sleep(cfg.duration)
			cancel()
		}()
	}

	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Calculate latency percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var p50, p90, p95, p99, pMax float64
	n := len(latencies)
	if n > 0 {
		p50 = float64(latencies[n*50/100]) / 1000.0
		p90 = float64(latencies[n*90/100]) / 1000.0
		p95 = float64(latencies[n*95/100]) / 1000.0
		p99 = float64(latencies[n*99/100]) / 1000.0
		pMax = float64(latencies[n-1]) / 1000.0
	}

	avgEPS := float64(st.totalSent) / totalElapsed.Seconds()
	mbTransferred := float64(st.totalBytes) / (1024 * 1024)
	mbPerSec := mbTransferred / totalElapsed.Seconds()

	fmt.Println()
	fmt.Println("=================================================================")
	fmt.Println("📊 WALSPOOL BENCHMARK & LOAD TEST RESULTS")
	fmt.Println("=================================================================")
	fmt.Printf("Duration         : %.2fs\n", totalElapsed.Seconds())
	fmt.Printf("Total Requests   : %d\n", st.totalSent)
	fmt.Printf("Successful (2xx) : %d (%.1f%%)\n", st.successCount, float64(st.successCount)/float64(max(st.totalSent, 1))*100.0)
	fmt.Printf("Rejected (429)   : %d (%.1f%%)\n", st.rejectedCount, float64(st.rejectedCount)/float64(max(st.totalSent, 1))*100.0)
	fmt.Printf("Errors / Timeout : %d\n", st.errorCount)
	fmt.Println("-----------------------------------------------------------------")
	fmt.Printf("Throughput (Avg) : %.1f events/sec\n", avgEPS)
	fmt.Printf("Bandwidth        : %.2f MB/s (Total: %.2f MB)\n", mbPerSec, mbTransferred)
	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("Latency (RTT Roundtrip):")
	fmt.Printf("  p50 (Median)   : %6.2f ms\n", p50)
	fmt.Printf("  p90            : %6.2f ms\n", p90)
	fmt.Printf("  p95            : %6.2f ms\n", p95)
	fmt.Printf("  p99            : %6.2f ms\n", p99)
	fmt.Printf("  Max            : %6.2f ms\n", pMax)
	fmt.Println("=================================================================")
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
