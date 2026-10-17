package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

// Build metadata injected via -ldflags during release builds.
var (
	Version   = "1.1.0"
	GitCommit = "HEAD"
	BuildDate = "unknown"
)

const usageText = `walspool — high-performance WAL telemetry streaming and monitoring CLI

Usage:
  walspool <command> [arguments]

Commands:
  stream     Listen to live telemetry logs with client-side filtering
  monitor    Interactive full-screen terminal dashboard for sidecar health & throughput
  version    Show walspool CLI version information
  help       Show help for any command

Flags:
  -a, --addr string   Target walspool-sidecar address (default "http://127.0.0.1:8080" or $WALSPOOL_ADDR)
  -h, --help          Show this help message

Run 'walspool <command> --help' for details on a specific command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "stream":
		if err := runStream(args); err != nil {
			if !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		}
	case "monitor":
		if err := runMonitor(args); err != nil {
			if !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		}
	case "version", "--version", "-v":
		fmt.Printf("walspool CLI version %s (commit %s, built %s) [powered by meow labs]\n", Version, GitCommit, BuildDate)
	case "help", "--help", "-h":
		fmt.Fprint(os.Stdout, usageText)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q. Run 'walspool help' for available commands.\n", cmd)
		os.Exit(1)
	}
}

func defaultAddr() string {
	if env := os.Getenv("WALSPOOL_ADDR"); env != "" {
		return env
	}
	return "http://127.0.0.1:8080"
}
