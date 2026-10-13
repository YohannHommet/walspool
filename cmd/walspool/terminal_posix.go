//go:build !windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

type winsize struct {
	ws_row    uint16
	ws_col    uint16
	ws_xpixel uint16
	ws_ypixel uint16
}

type rawTerminalState struct {
	fd      int
	termios syscall.Termios
}

func makeRaw(f *os.File) (*rawTerminalState, error) {
	if !IsTerminal(f) {
		return nil, nil
	}
	fd := int(f.Fd())
	var oldState syscall.Termios
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TCGETS), uintptr(unsafe.Pointer(&oldState))); err != 0 {
		return nil, err
	}

	newState := oldState
	newState.Lflag &^= (syscall.ICANON | syscall.ECHO)
	newState.Cc[syscall.VMIN] = 0  // non-blocking read
	newState.Cc[syscall.VTIME] = 0 // immediate return

	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TCSETS), uintptr(unsafe.Pointer(&newState))); err != 0 {
		return nil, err
	}

	return &rawTerminalState{fd: fd, termios: oldState}, nil
}

func restoreTerminal(state *rawTerminalState) {
	if state == nil {
		return
	}
	_, _, _ = syscall.Syscall(syscall.SYS_IOCTL, uintptr(state.fd), uintptr(syscall.TCSETS), uintptr(unsafe.Pointer(&state.termios)))
}

func getTerminalSize(f *os.File) (int, int) {
	if !IsTerminal(f) {
		return 80, 24
	}
	var ws winsize
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws))); err == 0 {
		if ws.ws_col > 0 && ws.ws_row > 0 {
			return int(ws.ws_col), int(ws.ws_row)
		}
	}
	return 80, 24
}
