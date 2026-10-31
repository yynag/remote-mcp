//go:build windows

package main

import (
	"os/exec"
	"strconv"
)

func shellCommand(command string) *exec.Cmd {
	return exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command)
}

func signalProcess(cmd *exec.Cmd, force bool) error {
	// Windows 没有可移植的 SIGTERM。taskkill /T 终止进程树；
	// job_kill 和超时升级时额外使用 /F。
	args := []string{"/PID", strconv.Itoa(cmd.Process.Pid), "/T"}
	if force {
		args = append(args, "/F")
	}
	return exec.Command("taskkill", args...).Run()
}
