//go:build windows

package main

import "os"

type rawTerminalState struct{}

func makeRaw(f *os.File) (*rawTerminalState, error) {
	return nil, nil
}

func restoreTerminal(state *rawTerminalState) {}

func getTerminalSize(f *os.File) (int, int) {
	return 80, 24
}
