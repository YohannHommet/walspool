package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/YohannHommet/walspool"
)

type monitorConfig struct {
	addr     string
	interval time.Duration
	noColor  bool
}

type metricsSnapshot struct {
	uncommittedRecords int
	storageDiskBytes   int64
	deliveredTotal     uint64
	sinkFlushesTotal   uint64
	sinkErrorsTotal    uint64
	sinkRetriesTotal   uint64
}

func parsePrometheusMetrics(text string) metricsSnapshot {
	snap := metricsSnapshot{}
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		valStr := parts[len(parts)-1]
		metricName := parts[0]

		if strings.HasPrefix(metricName, "walspool_uncommitted_records") {
			if v, err := strconv.Atoi(valStr); err == nil {
				snap.uncommittedRecords = v
			}
		} else if strings.HasPrefix(metricName, "walspool_storage_disk_bytes") {
			if v, err := strconv.ParseInt(valStr, 10, 64); err == nil {
				snap.storageDiskBytes = v
			}
		} else if strings.HasPrefix(metricName, "walspool_delivered_records_total") {
			if v, err := strconv.ParseUint(valStr, 10, 64); err == nil {
				snap.deliveredTotal += v
			}
		} else if strings.HasPrefix(metricName, "walspool_sink_flushes_total") {
			if v, err := strconv.ParseUint(valStr, 10, 64); err == nil {
				snap.sinkFlushesTotal = v
			}
		} else if strings.HasPrefix(metricName, "walspool_sink_errors_total") {
			if v, err := strconv.ParseUint(valStr, 10, 64); err == nil {
				snap.sinkErrorsTotal += v
			}
		} else if strings.HasPrefix(metricName, "walspool_sink_retries_total") {
			if v, err := strconv.ParseUint(valStr, 10, 64); err == nil {
				snap.sinkRetriesTotal = v
			}
		}
	}
	return snap
}

func runMonitor(args []string) error {
	fs := flag.NewFlagSet("monitor", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := monitorConfig{}
	fs.StringVar(&cfg.addr, "addr", defaultAddr(), "Address of walspool-sidecar")
	fs.StringVar(&cfg.addr, "a", defaultAddr(), "Address of walspool-sidecar (shorthand)")
	fs.DurationVar(&cfg.interval, "interval", 1*time.Second, "Dashboard refresh interval")
	fs.DurationVar(&cfg.interval, "i", 1*time.Second, "Dashboard refresh interval (shorthand)")
	fs.BoolVar(&cfg.noColor, "no-color", false, "Disable ANSI color formatting")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cfg.interval < 100*time.Millisecond {
		cfg.interval = 100 * time.Millisecond
	}

	isTTY := IsTerminal(os.Stdout)
	if os.Getenv("NO_COLOR") != "" || !isTTY {
		cfg.noColor = true
	}

	client, err := NewClient(cfg.addr)
	if err != nil {
		return err
	}

	// 1. Enter Alternate Screen Buffer and hide cursor on TTY
	if isTTY {
		fmt.Print("\033[?1049h\033[?25l")
	}

	// 2. Set up Raw terminal mode on POSIX to intercept single keystrokes without blocking
	rawState, _ := makeRaw(os.Stdin)

	// Clean cleanup hook guaranteed on exit
	cleanup := func() {
		if rawState != nil {
			restoreTerminal(rawState)
		}
		if isTTY {
			fmt.Print("\033[?25h\033[?1049l")
		}
	}
	defer cleanup()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	// Non-blocking keyboard listener
	keyCh := make(chan byte, 16)
	if isTTY {
		go func() {
			buf := make([]byte, 1)
			for {
				n, err := os.Stdin.Read(buf)
				if err != nil || n == 0 {
					return
				}
				keyCh <- buf[0]
			}
		}()
	}

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	// Rolling history of 60 points for sparklines
	samples := make([]float64, 0, 60)
	var prevIngested uint64
	var prevDelivered uint64
	var prevTime time.Time
	var paused bool
	var currentEPS float64
	var currentDeliveryRate float64

	frameBuf := bytes.NewBuffer(make([]byte, 0, 4096))
	startTime := time.Now()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		stats, statsErr := client.FetchStats(ctx)
		metricsText, metricsErr := client.FetchMetrics(ctx)
		cancel()

		now := time.Now()
		var snap metricsSnapshot
		if metricsErr == nil {
			snap = parsePrometheusMetrics(metricsText)
		}

		if statsErr == nil && !prevTime.IsZero() && !paused {
			elapsed := now.Sub(prevTime).Seconds()
			if elapsed > 0 {
				if stats.TotalIngested >= prevIngested {
					currentEPS = float64(stats.TotalIngested-prevIngested) / elapsed
				}
				if snap.deliveredTotal >= prevDelivered {
					currentDeliveryRate = float64(snap.deliveredTotal-prevDelivered) / elapsed
				}

				if len(samples) >= 60 {
					samples = samples[1:]
				}
				samples = append(samples, currentEPS)
			}
		}

		if statsErr == nil {
			prevIngested = stats.TotalIngested
			prevDelivered = snap.deliveredTotal
			prevTime = now
		}

		// Render frame
		termWidth, _ := getTerminalSize(os.Stdout)
		if termWidth <= 0 {
			termWidth = 80
		}

		frameBuf.Reset()
		renderFrame(frameBuf, &cfg, client.BaseURL, startTime, stats, statsErr, &snap, currentEPS, currentDeliveryRate, samples, paused, termWidth)

		// Send frame in 1 single write syscall
		if _, err := os.Stdout.Write(frameBuf.Bytes()); err != nil {
			if CheckBrokenPipe(err) {
				return nil
			}
			return err
		}

		// Wait for next tick, keyboard input, or OS signal
		select {
		case <-sigCh:
			return nil
		case k := <-keyCh:
			switch k {
			case 'q', 'Q', 3: // 'q' or Ctrl+C
				return nil
			case ' ': // space: pause / resume
				paused = !paused
			case 'r', 'R': // force refresh
				// immediate loop cycle
			}
		case <-ticker.C:
			// regular tick
		}
	}
}

func renderFrame(
	buf *bytes.Buffer,
	cfg *monitorConfig,
	baseURL string,
	startTime time.Time,
	stats *walspool.HubStats,
	statsErr error,
	snap *metricsSnapshot,
	eps float64,
	deliveryEPS float64,
	samples []float64,
	paused bool,
	termWidth int,
) {
	// Reposition cursor to Home (0,0) without clearing screen (no flicker!)
	buf.WriteString("\033[H")

	uptime := time.Since(startTime)
	statusBadge := "HEALTHY"
	if statsErr != nil {
		statusBadge = "DISCONNECTED (Retrying...)"
	} else if paused {
		statusBadge = "PAUSED"
	}

	header := fmt.Sprintf("walspool monitor — %s | Uptime: %s | Status: %s", baseURL, FormatUptime(uptime), statusBadge)
	if !cfg.noColor {
		if statsErr != nil {
			header = fmt.Sprintf("\033[1;37;41m %s \033[0m", header) // Red background
		} else if paused {
			header = fmt.Sprintf("\033[1;30;43m %s \033[0m", header) // Yellow background
		} else {
			header = fmt.Sprintf("\033[1;32m%s\033[0m", header)
		}
	}
	writeLine(buf, header)
	writeLine(buf, strings.Repeat("─", min(termWidth, 90)))

	if statsErr != nil {
		writeLine(buf, "")
		writeLine(buf, fmt.Sprintf("⚠️  Cannot connect to sidecar at %s (%v)", baseURL, statsErr))
		writeLine(buf, "    Waiting for sidecar daemon to start...")
		writeLine(buf, "")
		writeLine(buf, strings.Repeat("─", min(termWidth, 90)))
		writeLine(buf, "Controls: [q] Quit  [r] Retry Now")
		buf.WriteString("\033[J") // Clear remaining screen
		return
	}

	// [1. INGESTION SECTION]
	writeLine(buf, "[INBOUND INGESTION]")
	sparkMaxLen := min(30, max(10, termWidth-50))
	sparkline := RenderSparkline(samples, sparkMaxLen)

	writeLine(buf, fmt.Sprintf("  Rate:     %8.1f ev/s      History (%ds): %s", eps, len(samples), sparkline))
	writeLine(buf, fmt.Sprintf("  Total:    %10s events     Dropped: %d (0.00%%)", FormatNumber(stats.TotalIngested), stats.DroppedEvents))
	writeLine(buf, "")

	// [2. BUFFER & STORAGE SECTION]
	writeLine(buf, "[BUFFER & STORAGE]")
	barWidth := min(25, max(10, termWidth-45))
	progBar := RenderProgressBar(stats.CurrentSize, stats.Capacity, barWidth)
	writeLine(buf, fmt.Sprintf("  Memory:   %s", progBar))
	writeLine(buf, fmt.Sprintf("  Disk WAL: %d uncommitted records | Size on disk: %s", snap.uncommittedRecords, FormatBytes(snap.storageDiskBytes)))
	writeLine(buf, fmt.Sprintf("  Streams:  %d active SSE subscribers | Indexed Traces: %d", stats.ActiveStreams, stats.IndexedTraces))
	writeLine(buf, "")

	// [3. SINK DISPATCHER SECTION]
	writeLine(buf, "[OUTBOUND DISPATCHER & SINK]")
	writeLine(buf, fmt.Sprintf("  Delivered Rate: %8.1f ev/s     Total Delivered: %s", deliveryEPS, FormatNumber(snap.deliveredTotal)))
	writeLine(buf, fmt.Sprintf("  Sink Flushes:   %8d           Errors: %d | Retries: %d", snap.sinkFlushesTotal, snap.sinkErrorsTotal, snap.sinkRetriesTotal))
	writeLine(buf, "")

	writeLine(buf, strings.Repeat("─", min(termWidth, 90)))
	writeLine(buf, "Controls: [q] Quit  [space] Pause/Resume  [r] Refresh")

	// Clear any leftover lines from previous taller frame
	buf.WriteString("\033[J")
}

func writeLine(buf *bytes.Buffer, line string) {
	buf.WriteString(line)
	buf.WriteString("\033[K\r\n") // Clear to end of line + CRLF
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
