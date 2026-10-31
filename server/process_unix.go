//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

func shellCommand(command string) *exec.Cmd {
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

// 同时终止进程组，否则子进程会继续占用标准输出管道。
func signalProcess(cmd *exec.Cmd, force bool) error {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	return syscall.Kill(-cmd.Process.Pid, sig)
}
