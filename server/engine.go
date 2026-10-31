package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"remote-mcp/internal/protocol"
)

// Engine 管理单个远端实例的任务、连接和文件传输。
type Engine struct {
	id        string
	guide     string
	mu        sync.RWMutex
	jobs      map[string]*Job
	peers     map[*protocol.Peer]struct{}
	done      chan struct{}
	transfers *transferStore
}

// NewEngine 创建一个实例执行引擎。
func NewEngine(id, guide string) *Engine {
	e := &Engine{id: id, guide: guide, jobs: make(map[string]*Job), peers: make(map[*protocol.Peer]struct{}), done: make(chan struct{}), transfers: newTransferStore()}
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
			}
		}
	}()
	return e
}

// Close 停止引擎任务、传输清理器和所有客户端连接。
func (e *Engine) Close() {
	close(e.done)
	e.transfers.reap(true)
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

// ServePeer 处理客户端直连控制通道。
func (e *Engine) ServePeer(p *protocol.Peer) {
	e.ServeReversePeer(p, "")
}

// ServeReversePeer 处理经由 transport 建立的控制通道。
func (e *Engine) ServeReversePeer(p *protocol.Peer, dataURL string) {
	connectionID := newID()
	remote := p.Conn.RemoteAddr().String()
	mode := "直连"
	if dataURL != "" {
		mode = "transport反向连接"
	}
	e.mu.Lock()
	e.peers[p] = struct{}{}
	e.mu.Unlock()
	defer func() {
		p.Close()
		e.mu.Lock()
		delete(e.peers, p)
		e.mu.Unlock()
		log.Printf("[客户端断开] 实例=%s，连接=%s，模式=%s，远端=%s", e.id, connectionID, mode, remote)
	}()
	if err := p.Write(protocol.Hello{Type: "hello", InstanceID: e.id, Guide: e.guide}); err != nil {
		log.Printf("[实例介绍发送失败] 实例=%s，连接=%s，远端=%s，原因=%v", e.id, connectionID, remote, err)
		return
	}
	log.Printf("[实例介绍已发送] 实例=%s，连接=%s，模式=%s，远端=%s，指南字节数=%d", e.id, connectionID, mode, remote, len(e.guide))
	go p.Heartbeat()
	heartbeatCount := 0
	for {
		var req protocol.Message
		if err := p.Read(&req); err != nil {
			log.Printf("[客户端连接结束] 实例=%s，连接=%s，模式=%s，远端=%s，原因=%v", e.id, connectionID, mode, remote, err)
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
			log.Printf("[中继注册成功] 实例=%s，连接=%s，远端=%s", e.id, connectionID, remote)
			continue
		case "heartbeat":
			if err := p.Write(protocol.Message{Type: "heartbeat_ack"}); err != nil {
				log.Printf("[心跳应答失败] 实例=%s，连接=%s，远端=%s，原因=%v", e.id, connectionID, remote, err)
				return
			}
			continue
		case "heartbeat_ack":
			heartbeatCount++
			if heartbeatCount%12 == 0 {
				log.Printf("[连接心跳正常] 实例=%s，连接=%s，远端=%s，累计应答=%d", e.id, connectionID, remote, heartbeatCount)
			}
			continue
		}
		if err := req.ValidateRequest(); err != nil {
			log.Printf("[请求格式错误] 实例=%s，连接=%s，远端=%s，类型=%s，原因=%v", e.id, connectionID, remote, req.Type, err)
			_ = p.Write(protocol.Failure(req, "INVALID_REQUEST", err))
			continue
		}
		if req.InstanceID != e.id {
			_ = p.Write(protocol.Failure(req, "INSTANCE_OFFLINE", errors.New("unknown instance")))
			continue
		}
		log.Printf("[请求已接收] 实例=%s，连接=%s，client=%s，请求=%s，方法=%s", e.id, connectionID, req.ClientID, req.RequestID, req.Method)
		go func(req protocol.Message) {
			if err := p.Write(e.Handle(req)); err != nil {
				log.Printf("[响应发送失败] 实例=%s，连接=%s，client=%s，请求=%s，方法=%s，原因=%v", e.id, connectionID, req.ClientID, req.RequestID, req.Method, err)
			}
		}(req)
	}
}

// Handle 执行一条控制协议请求并生成对应响应。
func (e *Engine) Handle(req protocol.Message) protocol.Message {
	started := time.Now()
	result, err := e.dispatch(req)
	if err != nil {
		var pe *protocol.Error
		if errors.As(err, &pe) {
			log.Printf("[请求失败] 实例=%s，client=%s，请求=%s，方法=%s，耗时=%s，代码=%s，原因=%s", e.id, req.ClientID, req.RequestID, req.Method, time.Since(started).Round(time.Millisecond), pe.Code, pe.Message)
			return protocol.Failure(req, pe.Code, errors.New(pe.Message))
		}
		log.Printf("[请求失败] 实例=%s，client=%s，请求=%s，方法=%s，耗时=%s，原因=%v", e.id, req.ClientID, req.RequestID, req.Method, time.Since(started).Round(time.Millisecond), err)
		return protocol.Failure(req, "REQUEST_FAILED", err)
	}
	log.Printf("[请求完成] 实例=%s，client=%s，请求=%s，方法=%s，耗时=%s", e.id, req.ClientID, req.RequestID, req.Method, time.Since(started).Round(time.Millisecond))
	return protocol.Success(req, result)
}

func (e *Engine) dispatch(req protocol.Message) (any, error) {
	switch req.Method {
	case "instance.list":
		return map[string]any{"instances": []protocol.Hello{{Type: "hello", InstanceID: e.id, Guide: e.guide}}}, nil
	case "instance.get":
		return protocol.Hello{Type: "hello", InstanceID: e.id, Guide: e.guide}, nil
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
			result["error"] = map[string]any{"code": "COMMAND_TIMEOUT", "message": "command exceeded timeout_ms and was terminated"}
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
