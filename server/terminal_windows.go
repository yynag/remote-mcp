//go:build windows

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var releasePseudoConsole = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReleasePseudoConsole")

type windowsTerminal struct {
	hpc    windows.Handle
	job    windows.Handle
	in     *os.File
	out    *os.File
	pid    int
	legacy bool

	mu        sync.Mutex
	process   windows.Handle
	watchStop windows.Handle

	consoleOnce sync.Once
	closeOnce   sync.Once
	closeErr    error
}

func newTerminal(command string, args []string, cwd string) (_ terminal, retErr error) {
	if command == "" {
		return nil, errors.New("terminal command is empty")
	}
	argv := make([]string, 1+len(args))
	argv[0] = syscall.EscapeArg(command)
	for i, arg := range args {
		argv[i+1] = syscall.EscapeArg(arg)
	}
	cmdLine, err := windows.UTF16PtrFromString(strings.Join(argv, " "))
	if err != nil {
		return nil, fmt.Errorf("encode command line: %w", err)
	}
	var dir *uint16
	if cwd != "" {
		dir, err = windows.UTF16PtrFromString(cwd)
		if err != nil {
			return nil, fmt.Errorf("encode working directory: %w", err)
		}
	}

	var ptyIn, hostIn, hostOut, ptyOut, hpc, job windows.Handle
	var pi windows.ProcessInformation
	assigned := false
	defer func() {
		if job != 0 {
			if err := closeConPTYJob(job); err != nil {
				retErr = errors.Join(retErr, err)
			}
		}
		// 分配失败时 Job 可能未包含根进程，必须单独终止挂起进程。
		if pi.Process != 0 && !assigned {
			if err := windows.TerminateProcess(pi.Process, 1); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("TerminateProcess (ConPTY startup): %w", err))
			}
		}
		if pi.Thread != 0 {
			_ = windows.CloseHandle(pi.Thread)
		}
		// 启动失败时没有读循环，先断开管道，避免旧版 ConPTY 阻塞。
		for _, handle := range [...]windows.Handle{ptyIn, ptyOut, hostIn, hostOut} {
			if handle != 0 && handle != windows.InvalidHandle {
				_ = windows.CloseHandle(handle)
			}
		}
		if hpc != 0 {
			windows.ClosePseudoConsole(hpc)
		}
		if pi.Process != 0 {
			_ = windows.CloseHandle(pi.Process)
		}
	}()
	job, err = newConPTYJob()
	if err != nil {
		return nil, err
	}
	if err := windows.CreatePipe(&ptyIn, &hostIn, nil, 0); err != nil {
		return nil, fmt.Errorf("CreatePipe (ConPTY input): %w", err)
	}
	if err := windows.CreatePipe(&hostOut, &ptyOut, nil, 0); err != nil {
		return nil, fmt.Errorf("CreatePipe (ConPTY output): %w", err)
	}
	if err := windows.CreatePseudoConsole(windows.Coord{X: terminalCols, Y: terminalRows}, ptyIn, ptyOut, 0, &hpc); err != nil {
		return nil, fmt.Errorf("CreatePseudoConsole: %w", err)
	}
	if err := startConPTYProcess(hpc, cmdLine, dir, &pi); err != nil {
		return nil, err
	}
	// 先加入 Job 再运行用户代码，避免子进程在分配前启动。
	if err := windows.AssignProcessToJobObject(job, pi.Process); err != nil {
		return nil, fmt.Errorf("AssignProcessToJobObject (ConPTY): %w", err)
	}
	assigned = true
	if count, err := windows.ResumeThread(pi.Thread); err != nil {
		return nil, fmt.Errorf("ResumeThread (ConPTY): %w", err)
	} else if count > 1 {
		return nil, fmt.Errorf("ResumeThread (ConPTY): primary thread remains suspended (count %d)", count-1)
	}
	_ = windows.CloseHandle(pi.Thread)
	_ = windows.CloseHandle(ptyIn)
	_ = windows.CloseHandle(ptyOut)
	pi.Thread, ptyIn, ptyOut = 0, 0, 0

	t := &windowsTerminal{hpc: hpc, job: job, process: pi.Process, pid: int(pi.ProcessId)}
	// 新版释放控制台引用后，最后一个附着进程退出就会关闭输出。
	t.legacy = true
	if releasePseudoConsole.Find() == nil {
		result, _, _ := releasePseudoConsole.Call(uintptr(hpc))
		t.legacy = uint32(result) != 0
	}
	if t.legacy {
		if err := t.startExitWatcher(); err != nil {
			return nil, err
		}
	}
	t.in = os.NewFile(uintptr(hostIn), "conpty-in")
	t.out = os.NewFile(uintptr(hostOut), "conpty-out")
	hpc, job, pi.Process, hostIn, hostOut = 0, 0, 0, 0, 0
	return t, nil
}

func newConPTYJob() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("CreateJobObject (ConPTY): %w", err)
	}
	var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	// x/sys 通过 uintptr 传递结构体地址，固定它直到系统调用返回。
	var pin runtime.Pinner
	pin.Pin(&info)
	_, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	pin.Unpin()
	if err != nil {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("SetInformationJobObject (ConPTY): %w", err)
	}
	return job, nil
}

func closeConPTYJob(job windows.Handle) error {
	err := windows.TerminateJobObject(job, 1)
	if err != nil {
		// 只在确认 Job 已无活动进程时忽略终止错误。
		var info struct {
			totalUserTime, totalKernelTime, periodUserTime, periodKernelTime int64
			pageFaults, totalProcesses, activeProcesses, terminatedProcesses uint32
		}
		var pin runtime.Pinner
		pin.Pin(&info)
		queryErr := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil)
		pin.Unpin()
		if queryErr == nil && info.activeProcesses == 0 {
			err = nil
		} else {
			err = fmt.Errorf("TerminateJobObject (ConPTY): %w", err)
		}
	}
	if closeErr := windows.CloseHandle(job); closeErr != nil {
		return errors.Join(err, fmt.Errorf("CloseHandle (ConPTY job): %w", closeErr))
	}
	return err
}

func startConPTYProcess(hpc windows.Handle, cmdLine, dir *uint16, pi *windows.ProcessInformation) error {
	attrs, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return fmt.Errorf("NewProcThreadAttributeList: %w", err)
	}
	defer attrs.Delete()
	// 此属性接收指向系统分配的控制台对象的 HPCON 值，不是 &hpc。
	hpcValue := *(*unsafe.Pointer)(unsafe.Pointer(&hpc))
	if err := attrs.Update(windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, hpcValue, unsafe.Sizeof(hpc)); err != nil {
		return fmt.Errorf("UpdateProcThreadAttribute (ConPTY): %w", err)
	}
	siEx := windows.StartupInfoEx{ProcThreadAttributeList: attrs.List()}
	siEx.StartupInfo.Cb = uint32(unsafe.Sizeof(siEx))
	flags := uint32(windows.EXTENDED_STARTUPINFO_PRESENT | windows.CREATE_UNICODE_ENVIRONMENT | windows.CREATE_SUSPENDED)
	if err := windows.CreateProcess(nil, cmdLine, nil, nil, false, flags, nil, dir, &siEx.StartupInfo, pi); err != nil {
		return fmt.Errorf("CreateProcess (ConPTY): %w", err)
	}
	return nil
}

// 旧版 ConPTY 不会自行结束；根进程退出后关闭控制台，让读循环收到 EOF。
func (t *windowsTerminal) startExitWatcher() error {
	stop, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return fmt.Errorf("CreateEvent (ConPTY exit watcher): %w", err)
	}
	var observer windows.Handle
	self := windows.CurrentProcess()
	if err := windows.DuplicateHandle(self, t.process, self, &observer, windows.SYNCHRONIZE, false, 0); err != nil {
		_ = windows.CloseHandle(stop)
		return fmt.Errorf("DuplicateHandle (ConPTY exit watcher): %w", err)
	}
	t.watchStop = stop
	go func() {
		state, _ := windows.WaitForMultipleObjects([]windows.Handle{stop, observer}, false, windows.INFINITE)
		_ = windows.CloseHandle(observer)
		t.mu.Lock()
		t.watchStop = 0
		_ = windows.CloseHandle(stop)
		t.mu.Unlock()
		if state != windows.WAIT_OBJECT_0 {
			t.closeConsole()
		}
	}()
	return nil
}

func (t *windowsTerminal) Read(p []byte) (int, error)  { return t.out.Read(p) }
func (t *windowsTerminal) Write(p []byte) (int, error) { return t.in.Write(p) }
func (t *windowsTerminal) Pid() int                    { return t.pid }

func (t *windowsTerminal) closeConsole() {
	t.consoleOnce.Do(func() { windows.ClosePseudoConsole(t.hpc) })
}

// Wait 和 Close 中仅先取得句柄的一方可以使用并释放它。
func (t *windowsTerminal) takeProcess() windows.Handle {
	t.mu.Lock()
	defer t.mu.Unlock()
	process := t.process
	t.process = 0
	return process
}

func (t *windowsTerminal) Close() error {
	t.closeOnce.Do(func() {
		process := t.takeProcess()
		var stopErr error
		if t.legacy {
			t.mu.Lock()
			if t.watchStop != 0 {
				stopErr = windows.SetEvent(t.watchStop)
			}
			t.mu.Unlock()
			// 主动关闭时，即使没有会话读循环，也要排空旧版控制台的输出。
			go func() { _, _ = io.Copy(io.Discard, t.out) }()
		}
		// 先终止 Job；关闭其最后一个句柄也可兜底，不受控制台关闭阻塞影响。
		jobErr := closeConPTYJob(t.job)
		t.closeConsole()
		var processErr error
		if process != 0 {
			processErr = windows.CloseHandle(process)
		}
		t.closeErr = errors.Join(jobErr, stopErr, processErr, t.out.Close(), t.in.Close())
	})
	return t.closeErr
}

func (t *windowsTerminal) Wait() (int, error) {
	process := t.takeProcess()
	if process == 0 {
		return -1, os.ErrClosed
	}
	defer windows.CloseHandle(process)
	state, err := windows.WaitForSingleObject(process, windows.INFINITE)
	if err != nil {
		return -1, fmt.Errorf("WaitForSingleObject (ConPTY process): %w", err)
	}
	if state != windows.WAIT_OBJECT_0 {
		return -1, fmt.Errorf("WaitForSingleObject (ConPTY process): unexpected result %#x", state)
	}
	var code uint32
	if err := windows.GetExitCodeProcess(process, &code); err != nil {
		return -1, fmt.Errorf("GetExitCodeProcess (ConPTY): %w", err)
	}
	return int(code), nil
}
