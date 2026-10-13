package main

import (
	"fmt"
	"math"
	"strings"
	"time"
)

var sparkBlocks = []rune{' ', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

// RenderSparkline converts a slice of metric samples into an 8-level Unicode sparkline string.
func RenderSparkline(samples []float64, maxLen int) string {
	if len(samples) == 0 || maxLen <= 0 {
		return ""
	}
	if len(samples) > maxLen {
		samples = samples[len(samples)-maxLen:]
	}

	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	for _, v := range samples {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	if maxVal <= 0 {
		return strings.Repeat(" ", len(samples))
	}

	spread := maxVal - minVal
	var sb strings.Builder
	for _, v := range samples {
		if spread == 0 {
			sb.WriteRune(sparkBlocks[len(sparkBlocks)/2])
			continue
		}
		ratio := (v - minVal) / spread
		idx := int(ratio * float64(len(sparkBlocks)-1))
		if idx < 0 {
			idx = 0
		} else if idx >= len(sparkBlocks) {
			idx = len(sparkBlocks) - 1
		}
		sb.WriteRune(sparkBlocks[idx])
	}
	return sb.String()
}

// RenderProgressBar renders an ANSI progress bar [████░░░░] percent%
func RenderProgressBar(current, capacity int, barWidth int) string {
	if capacity <= 0 {
		return "[--------------------] 0.0%"
	}
	if barWidth <= 0 {
		barWidth = 20
	}

	percent := float64(current) / float64(capacity)
	if percent < 0 {
		percent = 0
	} else if percent > 1.0 {
		percent = 1.0
	}

	filled := int(percent * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	empty := barWidth - filled

	bar := strings.Repeat("█", filled) + strings.Repeat("░", empty)
	return fmt.Sprintf("[%s] %5.1f%% (%d / %d)", bar, percent*100.0, current, capacity)
}

// FormatBytes formats a byte count into a human-readable B, KB, MB, GB representation.
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatNumber formats an integer with comma thousands separators.
func FormatNumber(n uint64) string {
	in := fmt.Sprintf("%d", n)
	out := make([]byte, len(in)+(len(in)-1)/3)
	for i, j, k := len(in)-1, len(out)-1, 0; i >= 0; i, j, k = i-1, j-1, k+1 {
		if k > 0 && k%3 == 0 {
			out[j] = ','
			j--
		}
		out[j] = in[i]
	}
	return string(out)
}

// FormatUptime formats duration into Xd Yh Zm Ws.
func FormatUptime(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %02dm %02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
