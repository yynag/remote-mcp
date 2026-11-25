package main

import "time"

// Fixed terminal size and session lifecycle constants.
const (
	terminalCols = 120
	terminalRows = 40

	// sessionIdleTimeout is the idle reaping time after a session makes no read/write progress.
	sessionIdleTimeout = 30 * time.Minute
	// sessionRetention is the retention period for output buffers after a session ends.
	sessionRetention = 10 * time.Minute
	// maxSessions is the concurrent session limit per instance.
	maxSessions = 8
	// defaultReadLimit is the default and maximum number of bytes per read.
	defaultReadLimit = 64 * 1024
	// maxWaitMS is the maximum wait in milliseconds for session.read.
	maxWaitMS = int64(30000)
)

// terminal abstracts a readable and writable pseudo-terminal.
// Unix uses creack/pty; Windows uses ConPTY.
type terminal interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
	// Wait blocks until the process exits and returns its exit code; it may be called only once.
	Wait() (int, error)
	Pid() int
}
