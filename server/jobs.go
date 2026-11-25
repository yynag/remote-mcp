package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
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

// Job holds a remote command process and its bounded output buffers.
type Job struct {
	mu         sync.Mutex
	ID         string
	Request    protocol.Message
	pg         *processGroup
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
	pg, err := newProcessGroup(cmd)
	if err != nil {
		return nil, err
	}
	j := &Job{ID: newID(), Request: req, pg: pg, state: "running", exitCode: -1, started: time.Now(), done: make(chan struct{})}
	cmd.Stdout = &j.stdout
	cmd.Stderr = &j.stderr
	if err := cmd.Start(); err != nil {
		slog.Error("[command start failed]", "instance", e.id, "client", req.ClientID, "request", req.RequestID, "error", err)
		return nil, errors.Join(err, pg.close())
	}
	if err := pg.attach(cmd); err != nil {
		killErr := cmd.Process.Kill()
		closeErr := pg.close()
		if killErr != nil {
			go cmd.Wait()
			return nil, errors.Join(err, closeErr, fmt.Errorf("terminate process after start failure: %w", killErr))
		}
		_ = cmd.Wait()
		return nil, errors.Join(err, closeErr)
	}
	e.mu.Lock()
	e.jobs[j.ID] = j
	e.mu.Unlock()
	select {
	case <-e.done:
		_ = j.stop("killed", true)
	default:
	}
	slog.Info("[job started]", "instance", e.id, "job", j.ID, "client", req.ClientID, "request", req.RequestID, "cwd", p.Cwd, "command_bytes", len(p.Command), "timeout_ms", p.TimeoutMS)
	go func() {
		if j.pg.waitRoot() {
			// The root process has exited; terminate child processes that still hold the output pipes, then drain the output.
			j.mu.Lock()
			cleanupErr := j.pg.terminate(true)
			j.mu.Unlock()
			if cleanupErr != nil {
				slog.Error("[failed to reap job children]", "job", j.ID, "error", cleanupErr)
			}
		}
		err := cmd.Wait()
		j.mu.Lock()
		closeErr := j.pg.close()
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
		if closeErr != nil {
			slog.Error("[failed to release job process group]", "job", j.ID, "error", closeErr)
		}
		slog.Info("[job ended]", "instance", e.id, "job", j.ID, "client", req.ClientID, "request", req.RequestID, "state", state, "exit_code", exitCode, "duration", duration)
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
	if err := j.pg.terminate(force); err != nil {
		slog.Error("[job termination failed]", "job", j.ID, "reason", reason, "force", force, "error", err)
		return err
	}
	j.stopReason = reason
	slog.Info("[job termination requested]", "job", j.ID, "reason", reason, "force", force)
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
			// If the bounded output tail dropped old bytes, the first pushed frame starts at the beginning of the retained region rather than at zero.
			if *s.cursor < offset {
				*s.cursor = offset
			}
			*s.cursor = end
			// Limit frame size so large output does not block the control channel.
			for len(data) > 0 {
				n := min(len(data), 64*1024)
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
