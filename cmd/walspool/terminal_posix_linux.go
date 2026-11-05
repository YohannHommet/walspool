//go:build linux

package main

import "syscall"

const (
	ioTCGETS = syscall.TCGETS
	ioTCSETS = syscall.TCSETS
)
