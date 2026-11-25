//go:build windows

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/windows"

	"remote-mcp/internal/protocol"
)

const windowsJobTestTimeout = 15 * time.Second

func TestWindowsExecRunCapturesOutput(t *testing.T) {
	e := NewEngine("test", "")
	t.Cleanup(e.Close)
	result := windowsJobRequest(t, e, "exec.run", commandParams{
		Command: `[Console]::Out.Write('alpha'); [Console]::Error.Write('beta')`,
	})
	if result["state"] != "completed" || result["exit_code"] != 0 || result["stdout"] != "alpha" || result["stderr"] != "beta" {
		t.Fatalf("exec.run result mismatch: %#v", result)
	}
}

func TestWindowsJobTerminatesChildHoldingOutput(t *testing.T) {
	const ping = `$ErrorActionPreference = 'Stop'; ` +
		`$child = Start-Process -FilePath "$env:SystemRoot\System32\ping.exe" -ArgumentList '-t','127.0.0.1' -NoNewWindow -PassThru; ` +
		`[Console]::Error.WriteLine("job-child:$($child.Id)"); `
	for _, tc := range []struct {
		name, afterStart, state string
		exitCode                int
	}{
		{"kill", `$child.WaitForExit()`, "killed", 1},
		{"natural", `while (-not (Test-Path -LiteralPath '.\exit-root')) { Start-Sleep -Milliseconds 25 }; exit 0`, "completed", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cwd := t.TempDir()
			e := NewEngine("test", "")
			t.Cleanup(e.Close)
			started := windowsJobRequest(t, e, "job.start", commandParams{Command: ping + tc.afterStart, Cwd: cwd})
			id := started["job_id"].(string)
			j, err := e.getJob(id)
			if err != nil {
				t.Fatal(err)
			}
			child := openWindowsJobTestChild(t, j)
			if state, err := windows.WaitForSingleObject(child, 0); state != uint32(windows.WAIT_TIMEOUT) || err != nil {
				t.Fatalf("ping is not running: state=%#x err=%v", state, err)
			}

			params := map[string]string{"job_id": id}
			if tc.name == "kill" {
				windowsJobRequest(t, e, "job.kill", params)
			} else if err := os.WriteFile(filepath.Join(cwd, "exit-root"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case <-j.done:
			case <-time.After(windowsJobTestTimeout):
				t.Fatal("job did not exit; the output pipe may still be held by a child process")
			}
			status := windowsJobRequest(t, e, "job.status", params)
			if status["state"] != tc.state || status["exit_code"] != tc.exitCode {
				t.Fatalf("job status = %#v, want %s, exit code %d", status, tc.state, tc.exitCode)
			}
			if state, err := windows.WaitForSingleObject(child, 1000); state != windows.WAIT_OBJECT_0 || err != nil {
				t.Fatalf("ping did not exit: state=%#x err=%v", state, err)
			}
		})
	}
}

func windowsJobRequest(t *testing.T, e *Engine, method string, params any) map[string]any {
	t.Helper()
	body, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	resp := e.Handle(protocol.Message{Version: 1, Type: "request", ClientID: "c", RequestID: method, InstanceID: "test", Method: method, Params: body})
	if resp.Error != nil {
		t.Fatal(resp.Error)
	}
	return resp.Result.(map[string]any)
}

func openWindowsJobTestChild(t *testing.T, j *Job) windows.Handle {
	t.Helper()
	marker := regexp.MustCompile(`job-child:([0-9]+)\r?\n`)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(windowsJobTestTimeout)
	defer timeout.Stop()
	var child windows.Handle
	for {
		stderr, _, _ := j.stderr.Read(0)
		if m := marker.FindSubmatch(stderr); child == 0 && m != nil {
			pid, err := strconv.ParseUint(string(m[1]), 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			child, err = windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_TERMINATE, false, uint32(pid))
			if err != nil {
				t.Fatalf("open ping process handle: %v", err)
			}
			t.Cleanup(func() {
				if t.Failed() {
					_ = windows.TerminateProcess(child, 1)
				}
				_ = windows.CloseHandle(child)
			})
		}
		stdout, _, _ := j.stdout.Read(0)
		if child != 0 && bytes.Contains(stdout, []byte("127.0.0.1")) {
			return child
		}
		select {
		case <-ticker.C:
		case <-j.done:
			t.Fatalf("job exited before starting ping: %q", stderr)
		case <-timeout.C:
			t.Fatalf("timed out waiting for ping PID or output: stdout=%q stderr=%q", stdout, stderr)
		}
	}
}
