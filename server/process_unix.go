//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

type processGroup struct{ pid int }

func shellCommand(command string) *exec.Cmd {
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

func newProcessGroup(_ *exec.Cmd) (*processGroup, error) { return &processGroup{}, nil }

func (p *processGroup) attach(cmd *exec.Cmd) error {
	p.pid = cmd.Process.Pid
	return nil
}

func (p *processGroup) waitRoot() bool { return false }

func (p *processGroup) close() error { return nil }

// Terminate the process group as well; otherwise child processes keep holding the standard output pipes.
func (p *processGroup) terminate(force bool) error {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	return syscall.Kill(-p.pid, sig)
}
