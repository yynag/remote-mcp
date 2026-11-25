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

// 同时终止进程组，否则子进程会继续占用标准输出管道。
func (p *processGroup) terminate(force bool) error {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	return syscall.Kill(-p.pid, sig)
}
