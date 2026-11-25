//go:build !windows

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

const unixTerminalWriteTimeout = 5 * time.Second

var errCleanupIncomplete = incompleteTerminalCleanup{}

type unixTerminal struct {
	file *os.File
	pid  int

	exited   chan struct{}
	exitCode int
	waitErr  error

	cleanupOnce sync.Once
	cleanupErr  error
	closeOnce   sync.Once
	closeErr    error
}

func newTerminal(command string, args []string, cwd string) (terminal, error) {
	if command == "" {
		return nil, errors.New("command must not be empty")
	}
	cmd := exec.Command(command, args...)
	cmd.Dir = cwd
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: terminalCols, Rows: terminalRows})
	if err != nil {
		return nil, err
	}
	file, err := pollableTerminalFile(master)
	_ = master.Close()
	if err != nil {
		_ = terminateTerminalProcesses(cmd.Process.Pid, true)
		_ = cmd.Wait()
		return nil, err
	}
	t := &unixTerminal{file: file, pid: cmd.Process.Pid, exited: make(chan struct{})}
	go t.watchProcess(cmd)
	return t, nil
}

func pollableTerminalFile(master *os.File) (*os.File, error) {
	// Re-register Go's poller with a non-blocking descriptor so deadlines and Close can interrupt I/O.
	syscall.ForkLock.RLock()
	fd, err := syscall.Dup(int(master.Fd()))
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, err
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	file := os.NewFile(uintptr(fd), master.Name())
	if err := file.SetWriteDeadline(time.Time{}); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (t *unixTerminal) Pid() int { return t.pid }

func (t *unixTerminal) Read(p []byte) (int, error) { return t.file.Read(p) }

func (t *unixTerminal) Write(p []byte) (int, error) {
	if err := t.file.SetWriteDeadline(time.Now().Add(unixTerminalWriteTimeout)); err != nil {
		return 0, err
	}
	return t.file.Write(p)
}

func (t *unixTerminal) watchProcess(cmd *exec.Cmd) {
	t.waitErr = cmd.Wait()
	t.exitCode = -1
	if cmd.ProcessState != nil {
		t.exitCode, t.waitErr = cmd.ProcessState.ExitCode(), nil
	}
	close(t.exited)

	// Background processes may still hold the terminal, so cleanup must not wait for EOF.
	_ = t.cleanup()
	_ = t.file.SetReadDeadline(time.Now().Add(time.Second))
}

func (t *unixTerminal) Wait() (int, error) {
	<-t.exited
	return t.exitCode, t.waitErr
}

func (t *unixTerminal) Close() error {
	t.closeOnce.Do(func() {
		t.closeErr = errors.Join(t.cleanup(), t.file.Close())
	})
	return t.closeErr
}

func (t *unixTerminal) cleanup() error {
	t.cleanupOnce.Do(func() {
		select {
		case <-t.exited:
			t.cleanupErr = terminateTerminalProcesses(t.pid, false)
		default:
			t.cleanupErr = terminateTerminalProcesses(t.pid, true)
		}
	})
	return t.cleanupErr
}

func terminateTerminalProcesses(sid int, killGroup bool) error {
	var groupErr error
	if killGroup {
		groupErr = killTerminalPID(-sid)
	}
	err := visitTerminalProcesses(sid, func(pid int) error {
		if pid != sid {
			return killTerminalPID(pid)
		}
		return nil
	})
	if groupErr != nil || err != nil {
		return errors.Join(errCleanupIncomplete, groupErr, err)
	}
	return nil
}

func killTerminalPID(pid int) error {
	err := syscall.Kill(pid, syscall.SIGKILL)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

func visitTerminalProcesses(sid int, visit func(int) error) error {
	var pids []string
	var sessionID func(int) (int, error)
	switch runtime.GOOS {
	case "linux":
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				pids = append(pids, entry.Name())
			}
		}
		sessionID = linuxProcessSessionID
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		data, err := exec.CommandContext(ctx, "ps", "-axo", "pid=").Output()
		if err != nil {
			return err
		}
		pids = strings.Fields(string(data))
		sessionID = darwinProcessSessionID
	default:
		return errors.New("terminal session process enumeration is not supported on this platform")
	}

	var firstErr error
	for _, entry := range pids {
		pid, err := strconv.Atoi(entry)
		if err != nil || pid <= 0 {
			continue
		}
		actualSID, err := sessionID(pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err == nil && actualSID == sid {
			err = visit(pid)
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("pid %d: %w", pid, err)
		}
	}
	return firstErr
}

func linuxProcessSessionID(pid int) (int, error) {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	return procStatSessionID(stat)
}

func procStatSessionID(stat []byte) (int, error) {
	// comm can contain spaces and right parentheses; after it come state, ppid, pgid, and sid.
	end := bytes.LastIndexByte(stat, ')')
	if end >= 0 {
		fields := bytes.Fields(stat[end+1:])
		if len(fields) >= 4 {
			return strconv.Atoi(string(fields[3]))
		}
	}
	return 0, errors.New("failed to parse /proc process session ID")
}

func darwinProcessSessionID(pid int) (int, error) {
	// syscall.Getsid is not in the Linux standard library, so the same system call is used only on Darwin.
	sid, _, errno := syscall.Syscall(syscall.SYS_GETSID, uintptr(pid), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(sid), nil
}
