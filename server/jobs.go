package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"time"

	"remote-mcp/internal/protocol"
)

type commandParams struct {
	Command   string `json:"command"`
	Cwd       string `json:"cwd,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// Job 保存远端命令进程及其有界输出缓存。
type Job struct {
	mu         sync.Mutex
	ID         string
	Request    protocol.Message
	cmd        *exec.Cmd
	stdout     output
	stderr     output
	state      string
	exitCode   int
	started    time.Time
	ended      time.Time
	done       chan struct{}
	stopReason string
}

func newID() string { var b [16]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func (e *Engine) start(req protocol.Message, p commandParams) (*Job, error) {
	if p.Command == "" {
		return nil, errors.New("command must not be empty")
	}
	if p.TimeoutMS < 0 || p.TimeoutMS > int64((365*24*time.Hour)/time.Millisecond) {
		return nil, errors.New("timeout_ms must be between 0 and one year")
	}
	cmd := shellCommand(p.Command)
	cmd.Dir = p.Cwd
	j := &Job{ID: newID(), Request: req, cmd: cmd, state: "running", exitCode: -1, started: time.Now(), done: make(chan struct{})}
	cmd.Stdout = &j.stdout
	cmd.Stderr = &j.stderr
	if err := cmd.Start(); err != nil {
		log.Printf("[命令启动失败] 实例=%s，client=%s，请求=%s，原因=%v", e.id, req.ClientID, req.RequestID, err)
		return nil, err
	}
	e.mu.Lock()
	e.jobs[j.ID] = j
	e.mu.Unlock()
	log.Printf("[任务已启动] 实例=%s，任务=%s，client=%s，请求=%s，cwd=%q，命令字节数=%d，超时毫秒=%d", e.id, j.ID, req.ClientID, req.RequestID, p.Cwd, len(p.Command), p.TimeoutMS)
	go func() {
		err := cmd.Wait()
		j.mu.Lock()
		j.ended = time.Now()
		j.exitCode = 0
		if err != nil {
			j.exitCode = -1
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				j.exitCode = exit.ExitCode()
			}
		}
		j.state = "completed"
		if j.stopReason != "" {
			j.state = j.stopReason
		} else if err != nil {
			j.state = "failed"
		}
		close(j.done)
		state, exitCode, duration := j.state, j.exitCode, j.ended.Sub(j.started).Round(time.Millisecond)
		j.mu.Unlock()
		log.Printf("[任务结束] 实例=%s，任务=%s，client=%s，请求=%s，状态=%s，退出码=%d，耗时=%s", e.id, j.ID, req.ClientID, req.RequestID, state, exitCode, duration)
	}()
	go e.publishJob(j)
	if p.TimeoutMS > 0 {
		go func() {
			timer := time.NewTimer(time.Duration(p.TimeoutMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-j.done:
			case <-timer.C:
				_ = j.stop("timed_out", true)
			}
		}()
	}
	return j, nil
}

func (j *Job) stop(reason string, force bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.state != "running" {
		return nil
	}
	if err := signalProcess(j.cmd, force); err != nil {
		log.Printf("[任务终止失败] 任务=%s，原因=%s，强制=%t，错误=%v", j.ID, reason, force, err)
		return err
	}
	j.stopReason = reason
	log.Printf("[任务终止请求] 任务=%s，原因=%s，强制=%t", j.ID, reason, force)
	if !force {
		go func() {
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			select {
			case <-j.done:
			case <-timer.C:
				_ = j.stop(reason, true)
			}
		}()
	}
	return nil
}

func (j *Job) status() map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	end := j.ended
	if end.IsZero() {
		end = time.Now()
	}
	return map[string]any{"job_id": j.ID, "state": j.state, "exit_code": j.exitCode, "started_at": j.started.UTC().Format(time.RFC3339Nano), "duration_ms": end.Sub(j.started).Milliseconds()}
}

func (e *Engine) getJob(id string) (*Job, error) {
	e.mu.RLock()
	j := e.jobs[id]
	e.mu.RUnlock()
	if j == nil {
		return nil, fmt.Errorf("job %q not found (jobs expire 10 minutes after completion)", id)
	}
	return j, nil
}

func (e *Engine) publishJob(j *Job) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var stdoutOffset, stderrOffset int64
	flush := func() {
		for _, s := range []struct {
			name   string
			out    *output
			cursor *int64
		}{{"stdout", &j.stdout, &stdoutOffset}, {"stderr", &j.stderr, &stderrOffset}} {
			data, offset, end := s.out.Read(*s.cursor)
			// 如果有界输出尾部丢弃了旧字节，首个推送帧从保留区起点开始，而不是从零开始。
			if *s.cursor < offset {
				*s.cursor = offset
			}
			*s.cursor = end
			// 限制帧大小，避免大量输出阻塞控制通道。
			for len(data) > 0 {
				n := len(data)
				if n > 64*1024 {
					n = 64 * 1024
				}
				e.broadcast(protocol.Message{Version: 1, Type: "event", ClientID: j.Request.ClientID, RequestID: j.Request.RequestID, InstanceID: e.id, Event: "job.output", Data: map[string]any{"job_id": j.ID, "stream": s.name, "offset": offset, "data_base64": base64.StdEncoding.EncodeToString(data[:n])}})
				offset += int64(n)
				data = data[n:]
			}
		}
	}
	for {
		select {
		case <-tick.C:
			flush()
		case <-j.done:
			flush()
			e.broadcast(protocol.Message{Version: 1, Type: "event", ClientID: j.Request.ClientID, RequestID: j.Request.RequestID, InstanceID: e.id, Event: "job.completed", Data: j.status()})
			return
		}
	}
}

func (e *Engine) reap() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-t.C:
			e.mu.Lock()
			for id, j := range e.jobs {
				j.mu.Lock()
				expired := !j.ended.IsZero() && time.Since(j.ended) > 10*time.Minute
				j.mu.Unlock()
				if expired {
					delete(e.jobs, id)
				}
			}
			e.mu.Unlock()
		}
	}
}
