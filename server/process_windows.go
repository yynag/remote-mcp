//go:build windows

package main

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type processGroup struct {
	job        windows.Handle
	root       windows.Handle
	terminated bool
}

var getProcessIDOfThread = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetProcessIdOfThread")

func shellCommand(command string) *exec.Cmd {
	return exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command)
}

func newProcessGroup(cmd *exec.Cmd) (*processGroup, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("CreateJobObject: %w", err)
	}
	var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	var pin runtime.Pinner
	pin.Pin(&info)
	_, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	pin.Unpin()
	if err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("SetInformationJobObject: %w", err)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	// 对 Job 之外仍持有输出管道的进程，给 Wait 一个回收上限。
	cmd.WaitDelay = 3 * time.Second
	return &processGroup{job: job}, nil
}

func (p *processGroup) attach(cmd *exec.Cmd) (err error) {
	pid := uint32(cmd.Process.Pid)
	root, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.SYNCHRONIZE, false, pid)
	if err != nil {
		return fmt.Errorf("OpenProcess: %w", err)
	}
	defer func() {
		if err != nil {
			_ = windows.CloseHandle(root)
		}
	}()
	if err := windows.AssignProcessToJobObject(p.job, root); err != nil {
		return fmt.Errorf("AssignProcessToJobObject: %w", err)
	}
	if err := resumeJobProcess(pid); err != nil {
		return err
	}
	p.root = root
	return nil
}

func resumeJobProcess(pid uint32) error {
	// os/exec 已关闭创建时的线程句柄。挂起进程还未运行，按 PID 找回主线程。
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return fmt.Errorf("CreateToolhelp32Snapshot: %w", err)
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME|windows.THREAD_QUERY_LIMITED_INFORMATION, false, entry.ThreadID)
		if err != nil {
			return fmt.Errorf("OpenThread: %w", err)
		}
		defer windows.CloseHandle(thread)
		owner, _, callErr := getProcessIDOfThread.Call(uintptr(thread))
		if uint32(owner) == 0 {
			return fmt.Errorf("GetProcessIdOfThread: %w", callErr)
		}
		if uint32(owner) != pid {
			return errors.New("进程主线程已退出")
		}
		previous, err := windows.ResumeThread(thread)
		if err != nil {
			return fmt.Errorf("ResumeThread: %w", err)
		}
		if previous != 1 {
			return fmt.Errorf("ResumeThread: 主线程挂起计数为 %d，预期为 1", previous)
		}
		return nil
	}
	if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return errors.New("找不到挂起进程的主线程")
	}
	return fmt.Errorf("枚举进程线程: %w", err)
}

func (p *processGroup) terminate(_ bool) error {
	if p.terminated {
		return nil
	}
	if err := windows.TerminateJobObject(p.job, 1); err != nil {
		return fmt.Errorf("TerminateJobObject: %w", err)
	}
	p.terminated = true
	return nil
}

func (p *processGroup) waitRoot() bool {
	state, err := windows.WaitForSingleObject(p.root, windows.INFINITE)
	_ = windows.CloseHandle(p.root)
	p.root = 0
	return err == nil && state == windows.WAIT_OBJECT_0
}

func (p *processGroup) close() error {
	if p.job == 0 {
		return nil
	}
	err := windows.CloseHandle(p.job)
	p.job = 0
	if err != nil {
		return fmt.Errorf("CloseHandle (Job Object): %w", err)
	}
	return nil
}
