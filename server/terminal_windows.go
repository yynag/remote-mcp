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
		// If assignment fails, the job may not contain the root process; the suspended process must be terminated separately.
		if pi.Process != 0 && !assigned {
			if err := windows.TerminateProcess(pi.Process, 1); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("TerminateProcess (ConPTY startup): %w", err))
			}
		}
		if pi.Thread != 0 {
			_ = windows.CloseHandle(pi.Thread)
		}
		// There is no read loop when startup fails; close the pipes first so legacy ConPTY does not block.
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
	// Assign to the job before running user code so child processes do not start before assignment.
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
	// With newer versions, once the console reference is released, output closes when the last attached process exits.
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
	// x/sys passes the struct address through uintptr; pin it until the system call returns.
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
		// Ignore the termination error only after confirming the job has no active processes.
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
	// This attribute receives the HPCON value pointing to the system-allocated console object, not &hpc.
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

// Legacy ConPTY does not end on its own; close the console after the root process exits so the read loop receives EOF.
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

// Only the first of Wait and Close to acquire the handle may use and release it.
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
			// On explicit close, drain legacy console output even without a session read loop.
			go func() { _, _ = io.Copy(io.Discard, t.out) }()
		}
		// Terminate the job first; closing its last handle also works as a fallback and is not affected by a blocked console close.
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
