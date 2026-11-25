package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"remote-mcp/internal/protocol"
)

// Engine manages jobs, connections, and file transfers for a single remote instance.
type Engine struct {
	id        string
	guide     string
	mu        sync.RWMutex
	jobs      map[string]*Job
	peers     map[*protocol.Peer]struct{}
	done      chan struct{}
	transfers *transferStore
	sessions  *SessionManager
}

// NewEngine creates an instance execution engine.
func NewEngine(id, guide string) *Engine {
	e := &Engine{id: id, guide: guide, jobs: make(map[string]*Job), peers: make(map[*protocol.Peer]struct{}), done: make(chan struct{}), transfers: newTransferStore(), sessions: newSessionManager(id)}
	go e.reap()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-e.done:
				return
			case <-ticker.C:
				e.transfers.reap(false)
				e.sessions.reap()
			}
		}
	}()
	return e
}

// Close stops engine jobs, sessions, the transfer reaper, and all client connections.
func (e *Engine) Close() {
	close(e.done)
	e.transfers.reap(true)
	e.sessions.closeAll()
	e.mu.RLock()
	jobs := make([]*Job, 0, len(e.jobs))
	for _, j := range e.jobs {
		jobs = append(jobs, j)
	}
	peers := make([]*protocol.Peer, 0, len(e.peers))
	for p := range e.peers {
		peers = append(peers, p)
	}
	e.mu.RUnlock()
	for _, j := range jobs {
		_ = j.stop("killed", true)
	}
	for _, p := range peers {
		p.Close()
	}
}

func (e *Engine) broadcast(m protocol.Message) {
	e.mu.RLock()
	peers := make([]*protocol.Peer, 0, len(e.peers))
	for p := range e.peers {
		peers = append(peers, p)
	}
	e.mu.RUnlock()
	for _, p := range peers {
		_ = p.Write(m)
	}
}

// ServePeer handles a direct client control channel.
func (e *Engine) ServePeer(p *protocol.Peer) {
	e.ServeReversePeer(p, "")
}

// ServeReversePeer handles a control channel established through the transport.
func (e *Engine) ServeReversePeer(p *protocol.Peer, dataURL string) {
	connectionID := newID()
	remote := p.Conn.RemoteAddr().String()
	mode := "direct"
	if dataURL != "" {
		mode = "transport reverse connection"
	}
	e.mu.Lock()
	e.peers[p] = struct{}{}
	e.mu.Unlock()
	defer func() {
		p.Close()
		e.mu.Lock()
		delete(e.peers, p)
		e.mu.Unlock()
		slog.Info("[client disconnected]", "instance", e.id, "connection", connectionID, "mode", mode, "remote", remote)
	}()
	if err := p.Write(protocol.Hello{Type: "hello", InstanceID: e.id}); err != nil {
		slog.Error("[failed to send instance introduction]", "instance", e.id, "connection", connectionID, "remote", remote, "error", err)
		return
	}
	slog.Info("[instance introduction sent]", "instance", e.id, "connection", connectionID, "mode", mode, "remote", remote, "guide_bytes", len(e.guide))
	go p.Heartbeat()
	for {
		var req protocol.Message
		if err := p.Read(&req); err != nil {
			slog.Warn("[client connection ended]", "instance", e.id, "connection", connectionID, "mode", mode, "remote", remote, "error", err)
			return
		}
		switch req.Type {
		case "data.open":
			if dataURL != "" {
				var h protocol.DataHandshake
				if json.Unmarshal(req.Params, &h) == nil && h.InstanceID == e.id && h.ClientID == req.ClientID && h.PairID != "" {
					go e.reverseData(dataURL, h)
				}
			}
			continue
		case "welcome":
			slog.Info("[relay registration succeeded]", "instance", e.id, "connection", connectionID, "remote", remote)
			continue
		case "heartbeat":
			if err := p.Write(protocol.Message{Type: "heartbeat_ack"}); err != nil {
				slog.Error("[heartbeat response failed]", "instance", e.id, "connection", connectionID, "remote", remote, "error", err)
				return
			}
			continue
		case "heartbeat_ack":
			continue
		}
		if err := req.ValidateRequest(); err != nil {
			slog.Warn("[request format error]", "instance", e.id, "connection", connectionID, "remote", remote, "type", req.Type, "error", err)
			_ = p.Write(protocol.Failure(req, "INVALID_REQUEST", err))
			continue
		}
		if req.InstanceID != e.id {
			_ = p.Write(protocol.Failure(req, "INSTANCE_OFFLINE", errors.New("unknown instance")))
			continue
		}
		slog.Info("[request received]", "instance", e.id, "connection", connectionID, "client", req.ClientID, "request", req.RequestID, "method", req.Method)
		go func(req protocol.Message) {
			if err := p.Write(e.Handle(req)); err != nil {
				slog.Error("[failed to send response]", "instance", e.id, "connection", connectionID, "client", req.ClientID, "request", req.RequestID, "method", req.Method, "error", err)
			}
		}(req)
	}
}

// Handle executes one control protocol request and produces the corresponding response.
func (e *Engine) Handle(req protocol.Message) protocol.Message {
	started := time.Now()
	result, err := e.dispatch(req)
	if err != nil {
		var pe *protocol.Error
		if errors.As(err, &pe) {
			slog.Warn("[request failed]", "instance", e.id, "client", req.ClientID, "request", req.RequestID, "method", req.Method, "duration", time.Since(started).Round(time.Millisecond), "code", pe.Code, "reason", pe.Message)
			response := protocol.Failure(req, pe.Code, errors.New(pe.Message))
			response.Error.Data = pe.Data
			return response
		}
		slog.Error("[request failed]", "instance", e.id, "client", req.ClientID, "request", req.RequestID, "method", req.Method, "duration", time.Since(started).Round(time.Millisecond), "error", err)
		return protocol.Failure(req, "REQUEST_FAILED", err)
	}
	slog.Info("[request completed]", "instance", e.id, "client", req.ClientID, "request", req.RequestID, "method", req.Method, "duration", time.Since(started).Round(time.Millisecond))
	return protocol.Success(req, result)
}

func (e *Engine) dispatch(req protocol.Message) (any, error) {
	switch req.Method {
	case "instance.list":
		return map[string]any{"instances": []protocol.Hello{{Type: "hello", InstanceID: e.id}}}, nil
	case "instance.get":
		return protocol.Hello{Type: "hello", InstanceID: e.id, Guide: e.guide}, nil
	case "session.create", "session.write", "session.read", "session.close", "session.list":
		return e.sessions.request(req)
	case "exec.run", "job.start":
		var p commandParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, err
		}
		if req.Method == "exec.run" && p.TimeoutMS == 0 {
			p.TimeoutMS = 30000
		}
		j, err := e.start(req, p)
		if err != nil {
			return nil, err
		}
		if req.Method == "job.start" {
			return j.status(), nil
		}
		<-j.done
		result := j.status()
		stdout, so, se := j.stdout.Read(0)
		stderr, eo, ee := j.stderr.Read(0)
		result["stdout"] = string(stdout)
		result["stderr"] = string(stderr)
		result["stdout_offset"] = so
		result["stderr_offset"] = eo
		result["stdout_next_offset"] = se
		result["stderr_next_offset"] = ee
		result["output_truncated"] = so > 0 || eo > 0
		if result["state"] == "timed_out" {
			result["error"] = map[string]any{"code": "COMMAND_TIMEOUT", "message": "command exceeded the timeout_ms limit and was terminated"}
		}
		return result, nil
	case "job.status", "job.output", "job.cancel", "job.kill":
		var p struct {
			JobID        string `json:"job_id"`
			Offset       int64  `json:"offset"`
			StdoutOffset *int64 `json:"stdout_offset"`
			StderrOffset *int64 `json:"stderr_offset"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, err
		}
		if p.Offset < 0 || (p.StdoutOffset != nil && *p.StdoutOffset < 0) || (p.StderrOffset != nil && *p.StderrOffset < 0) {
			return nil, errors.New("offset must not be negative")
		}
		j, err := e.getJob(p.JobID)
		if err != nil {
			return nil, err
		}
		switch req.Method {
		case "job.cancel":
			if err := j.stop("cancelled", false); err != nil {
				return nil, err
			}
		case "job.kill":
			if err := j.stop("killed", true); err != nil {
				return nil, err
			}
		case "job.output":
			so, eo := p.Offset, p.Offset
			if p.StdoutOffset != nil {
				so = *p.StdoutOffset
			}
			if p.StderrOffset != nil {
				eo = *p.StderrOffset
			}
			s, sa, se := j.stdout.Read(so)
			er, ea, ee := j.stderr.Read(eo)
			return map[string]any{"job_id": j.ID, "stdout": string(s), "stderr": string(er), "stdout_offset": sa, "stderr_offset": ea, "stdout_next_offset": se, "stderr_next_offset": ee, "output_truncated": sa > so || ea > eo}, nil
		}
		return j.status(), nil
	case "file.stat", "file.list":
		return fileRequest(req)
	case "file.upload", "file.download", "transfer.prepare", "transfer.status", "transfer.commit", "transfer.cancel":
		return e.transferRequest(req)
	default:
		return nil, &protocol.Error{Code: "METHOD_NOT_FOUND", Message: fmt.Sprintf("unknown method %q", req.Method)}
	}
}
