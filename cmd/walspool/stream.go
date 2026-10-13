package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
)

type streamConfig struct {
	addr      string
	service   string
	level     string
	traceID   string
	topic     string
	grep      string
	format    string
	limit     int
	noColor   bool
	grepRegex *regexp.Regexp
}

type streamEvent struct {
	ID        uint64          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Topic     string          `json:"topic"`
	Service   string          `json:"service"`
	TraceID   string          `json:"trace_id,omitempty"`
	Level     string          `json:"level,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

var levelRanks = map[string]int{
	"DEBUG": 1,
	"INFO":  2,
	"WARN":  3,
	"ERROR": 4,
	"FATAL": 5,
}

func parseLevelRank(lvl string) int {
	return levelRanks[strings.ToUpper(strings.TrimSpace(lvl))]
}

func runStream(args []string) error {
	fs := flag.NewFlagSet("stream", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := streamConfig{}
	fs.StringVar(&cfg.addr, "addr", defaultAddr(), "Address of walspool-sidecar")
	fs.StringVar(&cfg.addr, "a", defaultAddr(), "Address of walspool-sidecar (shorthand)")
	fs.StringVar(&cfg.service, "service", "", "Filter by service name")
	fs.StringVar(&cfg.service, "s", "", "Filter by service name (shorthand)")
	fs.StringVar(&cfg.level, "level", "", "Filter by minimum log level (DEBUG, INFO, WARN, ERROR)")
	fs.StringVar(&cfg.level, "l", "", "Filter by minimum log level (shorthand)")
	fs.StringVar(&cfg.traceID, "trace-id", "", "Filter by distributed trace ID")
	fs.StringVar(&cfg.traceID, "t", "", "Filter by distributed trace ID (shorthand)")
	fs.StringVar(&cfg.topic, "topic", "", "Filter by topic name")
	fs.StringVar(&cfg.grep, "grep", "", "Filter by regular expression matching payload")
	fs.StringVar(&cfg.grep, "g", "", "Filter by regular expression (shorthand)")
	fs.StringVar(&cfg.format, "format", "", "Output format: pretty, json, or raw")
	fs.StringVar(&cfg.format, "f", "", "Output format (shorthand)")
	fs.IntVar(&cfg.limit, "limit", 0, "Stop after receiving N matching events (0 = unlimited)")
	fs.IntVar(&cfg.limit, "n", 0, "Stop after receiving N matching events (shorthand)")
	fs.BoolVar(&cfg.noColor, "no-color", false, "Disable ANSI color output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cfg.grep != "" {
		re, err := regexp.Compile(cfg.grep)
		if err != nil {
			return fmt.Errorf("invalid --grep regular expression %q: %w", cfg.grep, err)
		}
		cfg.grepRegex = re
	}

	isTTY := IsTerminal(os.Stdout)
	if cfg.format == "" {
		if isTTY {
			cfg.format = "pretty"
		} else {
			cfg.format = "json"
		}
	}
	cfg.format = strings.ToLower(cfg.format)
	if cfg.format != "pretty" && cfg.format != "json" && cfg.format != "raw" {
		return fmt.Errorf("unsupported format %q: choose 'pretty', 'json', or 'raw'", cfg.format)
	}

	if os.Getenv("NO_COLOR") != "" || !isTTY {
		cfg.noColor = true
	}

	client, err := NewClient(cfg.addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	startTime := time.Now()
	var capturedCount int

	go func() {
		<-sigCh
		cancel()
	}()

	fmt.Fprintf(os.Stderr, "● Connected to walspool-sidecar at %s\n", client.BaseURL)
	var activeFilters []string
	if cfg.service != "" {
		activeFilters = append(activeFilters, fmt.Sprintf("service=%q", cfg.service))
	}
	if cfg.level != "" {
		activeFilters = append(activeFilters, fmt.Sprintf("min-level=%s", strings.ToUpper(cfg.level)))
	}
	if cfg.traceID != "" {
		activeFilters = append(activeFilters, fmt.Sprintf("trace-id=%q", cfg.traceID))
	}
	if cfg.grep != "" {
		activeFilters = append(activeFilters, fmt.Sprintf("grep=%q", cfg.grep))
	}
	if len(activeFilters) > 0 {
		fmt.Fprintf(os.Stderr, "● Active client filters: %s\n", strings.Join(activeFilters, ", "))
	}
	fmt.Fprintf(os.Stderr, "● Streaming live events... Press Ctrl+C to stop.\n\n")

	// Pass service to server for network efficiency (Predicate Pushdown)
	body, err := client.OpenStream(ctx, cfg.service, "")
	if err != nil {
		return err
	}
	defer body.Close()

	reader := NewBoundedLineReader(body, 1024*1024) // 1 MB line guard
	minRank := parseLevelRank(cfg.level)

	for {
		if ctx.Err() != nil {
			break
		}

		line, truncated, err := reader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				break
			}
			if CheckBrokenPipe(err) {
				return nil
			}
			return fmt.Errorf("stream read error: %w", err)
		}

		if len(line) == 0 || line[0] == ':' {
			// Skip empty lines or SSE comment heartbeats
			continue
		}

		if !strings.HasPrefix(string(line), "data:") {
			continue
		}

		payloadBytes := strings.TrimSpace(strings.TrimPrefix(string(line), "data:"))
		if len(payloadBytes) == 0 {
			continue
		}

		var event streamEvent
		if err := json.Unmarshal([]byte(payloadBytes), &event); err != nil {
			continue
		}

		if !matchesFilter(&event, &cfg, minRank) {
			continue
		}

		capturedCount++
		if err := printEvent(os.Stdout, &event, cfg.format, cfg.noColor, truncated); err != nil {
			if CheckBrokenPipe(err) {
				return nil
			}
			return err
		}

		if cfg.limit > 0 && capturedCount >= cfg.limit {
			break
		}
	}

	duration := time.Since(startTime).Round(100 * time.Millisecond)
	fmt.Fprintf(os.Stderr, "\nCaptured %d events in %s.\n", capturedCount, duration)
	return nil
}

func matchesFilter(ev *streamEvent, cfg *streamConfig, minRank int) bool {
	if cfg.service != "" && !strings.EqualFold(ev.Service, cfg.service) {
		return false
	}
	if cfg.topic != "" && ev.Topic != cfg.topic {
		return false
	}
	if cfg.traceID != "" && ev.TraceID != cfg.traceID {
		return false
	}
	if minRank > 0 {
		rank := parseLevelRank(ev.Level)
		if rank < minRank {
			return false
		}
	}
	if cfg.grepRegex != nil {
		target := string(ev.Payload)
		if !cfg.grepRegex.MatchString(target) {
			return false
		}
	}
	return true
}

func printEvent(w io.Writer, ev *streamEvent, format string, noColor bool, truncated bool) error {
	switch format {
	case "raw":
		_, err := fmt.Fprintf(w, "%s\n", string(ev.Payload))
		return err

	case "json":
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(w, "%s\n", string(data))
		return err

	case "pretty":
		ts := ev.Timestamp.Format("15:04:05.000")
		level := strings.ToUpper(ev.Level)
		if level == "" {
			level = "INFO"
		}

		levelBadge := fmt.Sprintf("[%s]", level)
		if !noColor {
			switch level {
			case "ERROR", "FATAL":
				levelBadge = fmt.Sprintf("\033[31;1m[%s]\033[0m", level) // Red
			case "WARN":
				levelBadge = fmt.Sprintf("\033[33;1m[%s]\033[0m", level) // Yellow
			case "INFO":
				levelBadge = fmt.Sprintf("\033[32;1m[%s]\033[0m", level) // Green
			case "DEBUG":
				levelBadge = fmt.Sprintf("\033[36m[%s]\033[0m", level) // Cyan
			}
		}

		service := SanitizeTerminal(ev.Service, true)
		if service == "" {
			service = SanitizeTerminal(ev.Topic, true)
		}

		payloadStr := SanitizeTerminal(string(ev.Payload), true)
		if truncated {
			payloadStr += " [TRUNCATED]"
		}

		traceExtra := ""
		if ev.TraceID != "" {
			cleanTrace := SanitizeTerminal(ev.TraceID, true)
			if !noColor {
				traceExtra = fmt.Sprintf(" \033[90mtrace_id=%s\033[0m", cleanTrace)
			} else {
				traceExtra = fmt.Sprintf(" trace_id=%s", cleanTrace)
			}
		}

		_, err := fmt.Fprintf(w, "[%s] %s [%s] %s%s\n", ts, levelBadge, service, payloadStr, traceExtra)
		return err
	}

	return nil
}
