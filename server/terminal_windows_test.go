//go:build windows

package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

const (
	windowsTerminalTestTimeout = 10 * time.Second
	windowsTerminalOutputLine  = "0123456789"
)

func TestWindowsTerminalRoundTrip(t *testing.T) {
	cwd := t.TempDir()
	args := []string{"", "two words", `a\"b`, `tail space\`, "\u03bb"}
	term := startWindowsTestTerminal(t, cwd, "roundtrip", append([]string{cwd}, args...)...)
	if term.Pid() <= 0 {
		t.Fatalf("unexpected process ID: %d", term.Pid())
	}
	type result struct {
		output []byte
		code   int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		out, err := io.ReadAll(term)
		if err != nil {
			done <- result{output: out, err: err}
			return
		}
		code, err := term.Wait()
		done <- result{output: out, code: code, err: err}
	}()
	input := []byte("hello-conpty\r")
	if n, err := term.Write(input); n != len(input) || err != nil {
		t.Fatalf("write input = %d, %v", n, err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.code != 23 {
			t.Fatalf("exit = %d, %v; output %q", got.code, got.err, got.output)
		}
		argsJSON, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		wantArgs := "args=" + base64.RawStdEncoding.EncodeToString(argsJSON)
		if !bytes.Contains(got.output, []byte(wantArgs)) || !bytes.Contains(got.output, []byte("got=hello-conpty")) {
			t.Fatalf("output %q, want %q and input round trip", got.output, wantArgs)
		}
	case <-time.After(windowsTerminalTestTimeout):
		t.Fatal("timed out waiting for ConPTY output EOF and exit")
	}
}

func TestWindowsTerminalCloseWithUnreadOutput(t *testing.T) {
	term := startWindowsTestTerminal(t, "", "write")
	ready := make(chan error, 1)
	go func() {
		r := bufio.NewReader(term)
		for {
			line, err := r.ReadString('\n')
			if strings.Contains(line, windowsTerminalOutputLine) {
				ready <- nil
				return
			}
			if err != nil {
				ready <- err
				return
			}
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(windowsTerminalTestTimeout):
		t.Fatal("timed out waiting for ConPTY output")
	}
	closeWindowsTestTerminal(t, term)
	if code, err := term.Wait(); code != -1 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Wait after Close = %d, %v", code, err)
	}
}

func TestWindowsTerminalCloseTerminatesDetachedChild(t *testing.T) {
	for _, mode := range []string{"active", "natural"} {
		t.Run(mode, func(t *testing.T) {
			term := startWindowsTestTerminal(t, "", "spawn-detached", mode)
			r := bufio.NewReader(term)
			pid := readWindowsTestChildPID(t, r)
			child, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_TERMINATE, false, pid)
			if err != nil {
				t.Fatalf("open detached child: %v", err)
			}
			t.Cleanup(func() {
				if t.Failed() {
					_ = windows.TerminateProcess(child, 1)
				}
				_ = windows.CloseHandle(child)
			})
			if mode == "natural" {
				done := make(chan error, 1)
				go func() {
					if _, err := io.Copy(io.Discard, r); err != nil {
						done <- err
						return
					}
					code, err := term.Wait()
					if code != 29 || err != nil {
						done <- fmt.Errorf("root exit = %d, %v; want 29", code, err)
						return
					}
					done <- nil
				}()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(windowsTerminalTestTimeout):
					t.Fatal("timed out waiting for the root process to exit naturally")
				}
			}
			if state, err := windows.WaitForSingleObject(child, 0); state != uint32(windows.WAIT_TIMEOUT) || err != nil {
				t.Fatalf("detached child not running before Close: state %#x, %v", state, err)
			}
			closeWindowsTestTerminal(t, term)
			if state, err := windows.WaitForSingleObject(child, uint32(windowsTerminalTestTimeout/time.Millisecond)); state != windows.WAIT_OBJECT_0 || err != nil {
				t.Fatalf("detached child did not exit after Close: state %#x, %v", state, err)
			}
		})
	}
}

func readWindowsTestChildPID(t *testing.T, r *bufio.Reader) uint32 {
	t.Helper()
	type result struct {
		pid uint32
		err error
	}
	done := make(chan result, 1)
	go func() {
		for {
			line, err := r.ReadString('\n')
			if _, value, ok := strings.Cut(line, "job-child:"); ok {
				var pid uint32
				_, err := fmt.Sscanf(value, "%d", &pid)
				done <- result{pid, err}
				return
			}
			if err != nil {
				done <- result{err: err}
				return
			}
		}
	}()
	select {
	case got := <-done:
		if got.err != nil || got.pid == 0 {
			t.Fatalf("read detached child PID: %d, %v", got.pid, got.err)
		}
		return got.pid
	case <-time.After(windowsTerminalTestTimeout):
		t.Fatal("timed out reading the detached child PID")
		return 0
	}
}

func startWindowsTestTerminal(t *testing.T, cwd, mode string, args ...string) terminal {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	argv := append([]string{"-test.run=^TestWindowsTerminalHelper$", "--", mode}, args...)
	term, err := newTerminal(exe, argv, cwd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeWindowsTestTerminal(t, term) })
	return term
}

func closeWindowsTestTerminal(t *testing.T, term terminal) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- term.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(windowsTerminalTestTimeout):
		t.Fatal("timed out closing ConPTY")
	}
}

func TestWindowsTerminalHelper(t *testing.T) {
	sep := slices.Index(os.Args, "--")
	if sep == -1 || sep+1 == len(os.Args) {
		return
	}
	args := os.Args[sep+1:]
	switch args[0] {
	case "roundtrip":
		cwd, err := os.Getwd()
		if err != nil || len(args) < 2 || !strings.EqualFold(filepath.Clean(cwd), filepath.Clean(args[1])) {
			t.Fatalf("unexpected working directory: %q, %v", cwd, err)
		}
		var input string
		if _, err := fmt.Fscan(os.Stdin, &input); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(args[2:])
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "args="+base64.RawStdEncoding.EncodeToString(encoded))
		fmt.Fprintln(os.Stdout, "got="+input)
		os.Exit(23)
	case "write":
		block := strings.Repeat(windowsTerminalOutputLine+"\r\n", 1024)
		for {
			if _, err := io.WriteString(os.Stdout, block); err != nil {
				os.Exit(24)
			}
		}
	case "spawn-detached":
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(exe, "-test.run=^TestWindowsTerminalHelper$", "--", "detached-child")
		cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.DETACHED_PROCESS}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pid := cmd.Process.Pid
		if err := cmd.Process.Release(); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(os.Stdout, "job-child:%d\r\n", pid)
		if args[1] == "natural" {
			os.Exit(29)
		}
		time.Sleep(time.Minute)
	case "detached-child":
		time.Sleep(time.Minute)
	}
}
