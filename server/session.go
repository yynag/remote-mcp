package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"remote-mcp/internal/protocol"
)

type sessionCreateParams struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
	Cwd     string   `json:"cwd"`
}

type sessionWriteParams struct {
	SessionID string `json:"session_id"`
	Input     string `json:"input"`
}

type sessionReadParams struct {
	SessionID string `json:"session_id"`
	Offset    int64  `json:"offset"`
	MaxBytes  int    `json:"max_bytes"`
	WaitMS    int64  `json:"wait_ms"`
	StripANSI bool   `json:"strip_ansi"`
}

type incompleteTerminalCleanup struct{}

func (incompleteTerminalCleanup) Error() string { return "incomplete session process cleanup" }

// Session holds the terminal, output tail, and lifecycle state independent of the write lock.
type Session struct {
	id, instance, command, cwd string
	term                       terminal
	started                    time.Time
	writeMu                    sync.Mutex
	closeOnce                  sync.Once
	closeErr                   error

	mu          sync.Mutex
	out         output
	notify      chan struct{}
	lastActive  time.Time
	ended       time.Time
	exitCode    int
	closeReason string
}

// SessionManager manages session quota and reaping for a single instance.
type SessionManager struct {
	instance    string
	limit       int
	idleTimeout time.Duration
	retention   time.Duration

	mu       sync.Mutex
	sessions map[string]*Session
	starting int
	starts   sync.WaitGroup
	closed   bool
}

func newSessionManager(instance string) *SessionManager {
	return &SessionManager{
		instance: instance, limit: maxSessions, idleTimeout: sessionIdleTimeout, retention: sessionRetention,
		sessions: make(map[string]*Session),
	}
}

func (m *SessionManager) create(p sessionCreateParams) (*Session, error) {
	if p.Command == "" {
		return nil, errors.New("command must not be empty")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("session manager is closed")
	}
	if len(m.sessions)+m.starting >= m.limit {
		var oldestID string
		var oldestEnd time.Time
		for id, s := range m.sessions {
			s.mu.Lock()
			ended := s.ended
			s.mu.Unlock()
			if !ended.IsZero() && (oldestEnd.IsZero() || ended.Before(oldestEnd)) {
				oldestID, oldestEnd = id, ended
			}
		}
		if oldestID != "" {
			delete(m.sessions, oldestID)
		}
	}
	if len(m.sessions)+m.starting >= m.limit {
		m.mu.Unlock()
		return nil, &protocol.Error{Code: "SESSION_LIMIT", Message: "session limit reached"}
	}
	m.starting++
	m.starts.Add(1)
	m.mu.Unlock()
	defer m.starts.Done()

	id, start := newID(), time.Now()
	term, err := newTerminal(p.Command, p.Args, p.Cwd)
	now := time.Now()
	m.mu.Lock()
	m.starting--
	if err != nil {
		m.mu.Unlock()
		slog.Warn("[session start failed]", "instance", m.instance, "session", id, "reason", "terminal_start", "duration", now.Sub(start).Round(time.Millisecond))
		return nil, errors.New("terminal start failed; check the command, arguments, and working directory")
	}
	s := &Session{
		id: id, instance: m.instance, command: p.Command, cwd: p.Cwd, term: term,
		started: now, lastActive: now, exitCode: -1,
	}
	m.sessions[id] = s
	m.mu.Unlock()
	slog.Info("[session started]", "instance", m.instance, "session", id, "duration", now.Sub(start).Round(time.Millisecond))
	go s.readOutput()
	return s, nil
}

func (m *SessionManager) get(id string) (*Session, error) {
	m.mu.Lock()
	s := m.sessions[id]
	m.mu.Unlock()
	if s == nil {
		return nil, &protocol.Error{Code: "SESSION_NOT_FOUND", Message: "session not found"}
	}
	return s, nil
}

func (m *SessionManager) list() []map[string]any {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].started.Before(sessions[j].started) })
	result := make([]map[string]any, 0, len(sessions))
	for _, s := range sessions {
		s.mu.Lock()
		_, _, _, end := s.out.ReadLimit(0, 0)
		result = append(result, map[string]any{
			"session_id": s.id, "command": s.command, "cwd": s.cwd, "cols": terminalCols, "rows": terminalRows,
			"running": s.runningLocked(), "exit_code": s.exitCode, "started_at": s.started.UTC().Format(time.RFC3339Nano),
			"last_active_at": s.lastActive.UTC().Format(time.RFC3339Nano), "output_bytes": end,
		})
		s.mu.Unlock()
	}
	return result
}

func (m *SessionManager) reap() {
	now := time.Now()
	m.mu.Lock()
	var idle []*Session
	for id, s := range m.sessions {
		s.mu.Lock()
		expired := !s.ended.IsZero() && now.Sub(s.ended) >= m.retention
		s.mu.Unlock()
		if expired {
			delete(m.sessions, id)
		} else if s.beginClose("idle", now.Add(-m.idleTimeout)) {
			idle = append(idle, s)
		}
	}
	m.mu.Unlock()
	for _, s := range idle {
		_ = s.closeTerminal()
	}
}

func (m *SessionManager) closeAll() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.starts.Wait()

	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		s.beginClose("shutdown", time.Time{})
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		_ = s.closeTerminal()
	}
}

func (s *Session) runningLocked() bool {
	return s.ended.IsZero() && s.closeReason == ""
}

func (s *Session) notifyLocked() {
	if s.notify == nil {
		return
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
	// Close the current notification channel so all readers waiting in the same round recheck state.
	close(s.notify)
	s.notify = nil
}

func (s *Session) readOutput() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.term.Read(buf)
		if n > 0 {
			s.mu.Lock()
			_, _ = s.out.Write(buf[:n])
			s.notifyLocked()
			s.mu.Unlock()
		}
		if err != nil {
			break
		}
	}
	code, waitErr := s.term.Wait()
	_ = s.closeTerminal()
	s.mu.Lock()
	s.ended, s.exitCode = time.Now(), code
	reason, duration := s.closeReason, s.ended.Sub(s.started).Round(time.Millisecond)
	s.notifyLocked()
	s.mu.Unlock()
	if reason == "" {
		reason = "natural"
		if waitErr != nil {
			reason = "wait_failed"
		}
	}
	slog.Info("[session ended]", "instance", s.instance, "session", s.id, "reason", reason, "duration", duration, "exit_code", code)
}

func (s *Session) beginClose(reason string, idleBefore time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.runningLocked() || (!idleBefore.IsZero() && s.lastActive.After(idleBefore)) {
		return false
	}
	s.closeReason = reason
	s.notifyLocked()
	return true
}

func (s *Session) closeTerminal() error {
	s.closeOnce.Do(func() {
		start := time.Now()
		s.closeErr = s.term.Close()
		if s.closeErr != nil {
			slog.Warn("[session cleanup incomplete]", "instance", s.instance, "session", s.id, "reason", "terminal_cleanup", "duration", time.Since(start).Round(time.Millisecond), "code", "SESSION_CLEANUP_INCOMPLETE")
		}
	})
	return s.closeErr
}

func (s *Session) write(input string) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	running := s.runningLocked()
	s.mu.Unlock()
	if !running {
		return 0, sessionWriteError("SESSION_ENDED", "session has ended or is closing", 0)
	}
	if input == "" {
		return 0, nil
	}
	n, err := s.term.Write([]byte(input))
	s.mu.Lock()
	if n > 0 {
		s.lastActive = time.Now()
	}
	running = s.runningLocked()
	s.mu.Unlock()
	if err != nil || n != len(input) {
		switch {
		case !running:
			return n, sessionWriteError("SESSION_ENDED", "session has ended or is closing", n)
		case errors.Is(err, os.ErrDeadlineExceeded) || os.IsTimeout(err):
			return n, sessionWriteError("SESSION_WRITE_TIMEOUT", "session write timed out", n)
		default:
			return n, sessionWriteError("REQUEST_FAILED", "session write failed", n)
		}
	}
	return n, nil
}

func sessionWriteError(code, message string, n int) *protocol.Error {
	return &protocol.Error{Code: code, Message: message, Data: map[string]any{"bytes_written": n}}
}

func (s *Session) read(p sessionReadParams) map[string]any {
	limit := min(p.MaxBytes, defaultReadLimit)
	if limit == 0 {
		limit = defaultReadLimit
	}
	expired := p.WaitMS == 0
	var deadline <-chan time.Time
	if !expired {
		timer := time.NewTimer(time.Duration(min(p.WaitMS, maxWaitMS)) * time.Millisecond)
		defer timer.Stop()
		deadline = timer.C
	}
	for {
		s.mu.Lock()
		_, base, _, end := s.out.ReadLimit(0, 0)
		truncated, running := p.Offset < base, s.runningLocked()
		if p.Offset < end || truncated || !running || expired {
			data, start, _, _ := s.out.ReadLimit(p.Offset, limit+utf8.UTFMax-1)
			n := utf8PageLen(data, limit, s.ended.IsZero())
			if n > 0 {
				s.lastActive = time.Now()
			}
			code := s.exitCode
			s.mu.Unlock()
			data = data[:n]
			if p.StripANSI {
				data = stripSessionANSI(data)
			}
			return map[string]any{
				"session_id": s.id, "output": string(data), "next_offset": max(p.Offset, start) + int64(n),
				"base_offset": base, "truncated": truncated, "running": running, "exit_code": code,
			}
		}
		if s.notify == nil {
			s.notify = make(chan struct{}, 1)
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-deadline:
			expired = true
		}
	}
}

func utf8PageLen(data []byte, limit int, mayGrow bool) int {
	n := min(len(data), limit)
	for i := n - 1; i >= max(0, n-utf8.UTFMax+1); i-- {
		if !utf8.RuneStart(data[i]) {
			continue
		}
		if !utf8.FullRune(data[i:n]) {
			if !utf8.FullRune(data[i:]) {
				if mayGrow {
					return i
				}
			} else if _, size := utf8.DecodeRune(data[i:]); i+size > n {
				return i
			}
		}
		break
	}
	return n
}

func stripSessionANSI(data []byte) []byte {
	const (
		text = iota
		escape
		intermediate
		csi
		osc
		oscEscape
	)
	state := text
	result := data[:0]
	for _, b := range data {
		switch state {
		case text:
			if b == 0x1b {
				state = escape
			} else {
				result = append(result, b)
			}
		case escape:
			switch {
			case b == '[':
				state = csi
			case b == ']':
				state = osc
			case b >= 0x20 && b <= 0x2f:
				state = intermediate
			case b != 0x1b:
				state = text
			}
		case intermediate, csi:
			if b == 0x1b {
				state = escape
			} else if b >= 0x30 && b <= 0x7e && (state == intermediate || b >= 0x40) {
				state = text
			}
		case osc:
			if b == 0x07 {
				state = text
			} else if b == 0x1b {
				state = oscEscape
			}
		case oscEscape:
			if b == '\\' || b == 0x07 {
				state = text
			} else if b != 0x1b {
				state = osc
			}
		}
	}
	return result
}

func (m *SessionManager) request(req protocol.Message) (any, error) {
	invalid := errors.New("invalid session request parameters")
	switch req.Method {
	case "session.create":
		var p sessionCreateParams
		if json.Unmarshal(req.Params, &p) != nil {
			return nil, invalid
		}
		s, err := m.create(p)
		if err != nil {
			return nil, err
		}
		return map[string]any{"session_id": s.id, "command": p.Command, "args": append([]string{}, p.Args...), "cwd": p.Cwd, "cols": terminalCols, "rows": terminalRows}, nil
	case "session.write":
		var p sessionWriteParams
		if json.Unmarshal(req.Params, &p) != nil {
			return nil, sessionWriteError("REQUEST_FAILED", invalid.Error(), 0)
		}
		s, err := m.get(p.SessionID)
		if err != nil {
			return nil, sessionWriteError("SESSION_NOT_FOUND", "session not found", 0)
		}
		n, err := s.write(p.Input)
		if err != nil {
			return nil, err
		}
		return map[string]any{"session_id": s.id, "bytes_written": n}, nil
	case "session.read":
		var p sessionReadParams
		if json.Unmarshal(req.Params, &p) != nil || p.Offset < 0 || p.MaxBytes < 0 || p.WaitMS < 0 {
			return nil, invalid
		}
		s, err := m.get(p.SessionID)
		if err != nil {
			return nil, err
		}
		return s.read(p), nil
	case "session.close":
		var p struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal(req.Params, &p) != nil {
			return nil, invalid
		}
		s, err := m.get(p.SessionID)
		if err != nil {
			return nil, err
		}
		s.beginClose("requested", time.Time{})
		err = s.closeTerminal()
		result := map[string]any{"session_id": s.id, "closed": true, "cleanup_incomplete": err != nil}
		var incomplete incompleteTerminalCleanup
		if err != nil && !errors.As(err, &incomplete) {
			return nil, &protocol.Error{Code: "SESSION_CLEANUP_INCOMPLETE", Message: "terminal cleanup failed", Data: result}
		}
		return result, nil
	case "session.list":
		return map[string]any{"sessions": m.list()}, nil
	default:
		return nil, &protocol.Error{Code: "METHOD_NOT_FOUND", Message: "unknown session method"}
	}
}
