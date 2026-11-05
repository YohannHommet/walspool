//go:build darwin

package main

import "syscall"

const (
	ioTCGETS = syscall.TIOCGETA
	ioTCSETS = syscall.TIOCSETA
)
