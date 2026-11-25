//go:build !windows

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"remote-mcp/internal/protocol"
)

func testSessionEngine(t *testing.T) *Engine {
	t.Helper()
	e := NewEngine("session-test", "")
	t.Cleanup(e.Close)
	return e
}

func sessionTestRequest(e *Engine, method string, params []byte) protocol.Message {
	return e.Handle(protocol.Message{
		Version: protocol.Version, Type: "request", ClientID: "test", RequestID: newID(),
		InstanceID: e.id, Method: method, Params: params,
	})
}

func sessionTestResult(t *testing.T, e *Engine, method string, params any) map[string]any {
	t.Helper()
	data, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	response := sessionTestRequest(e, method, data)
	if response.Error != nil {
		t.Fatalf("%s: %v", method, response.Error)
	}
	return response.Result.(map[string]any)
}

func startTestSession(t *testing.T, e *Engine, command string, args ...string) *Session {
	t.Helper()
	started := sessionTestResult(t, e, "session.create", sessionCreateParams{Command: command, Args: args})
	s, err := e.sessions.get(started["session_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func awaitSessionCondition(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("等待超时：%s", description)
		}
	}
}

func awaitSessionEnd(t *testing.T, s *Session) {
	t.Helper()
	awaitSessionCondition(t, "会话结束", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.ended.IsZero()
	})
}

func readSessionUntil(t *testing.T, e *Engine, id string, matches func(string) bool) (string, int64) {
	t.Helper()
	var collected strings.Builder
	var offset int64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := sessionTestResult(t, e, "session.read", sessionReadParams{SessionID: id, Offset: offset, WaitMS: 200})
		output := got["output"].(string)
		collected.WriteString(output)
		offset = got["next_offset"].(int64)
		if matches(collected.String()) {
			return collected.String(), offset
		}
		if output == "" && !got["running"].(bool) {
			break
		}
	}
	t.Fatalf("未读到预期输出，已读 %q", collected.String())
	return "", 0
}

func TestSessionShellRoundTrip(t *testing.T) {
	e := testSessionEngine(t)
	cwd := t.TempDir()
	args := []string{"-i"}
	created := sessionTestResult(t, e, "session.create", sessionCreateParams{Command: "/bin/sh", Args: args, Cwd: cwd})
	if created["command"] != "/bin/sh" || created["cwd"] != cwd || !reflect.DeepEqual(created["args"], args) || created["cols"] != terminalCols || created["rows"] != terminalRows {
		t.Fatalf("创建响应错误：%v", created)
	}
	id := created["session_id"].(string)
	const input = "echo hi\n"
	written := sessionTestResult(t, e, "session.write", sessionWriteParams{SessionID: id, Input: input})
	if written["session_id"] != id || written["bytes_written"] != len(input) {
		t.Fatalf("写入响应错误：%v", written)
	}
	_, offset := readSessionUntil(t, e, id, func(s string) bool { return strings.Contains(s, "\nhi\r\n") })
	sessions := sessionTestResult(t, e, "session.list", struct{}{})["sessions"].([]map[string]any)
	if len(sessions) != 1 || sessions[0]["session_id"] != id || sessions[0]["running"] != true || sessions[0]["exit_code"] != -1 || sessions[0]["output_bytes"].(int64) < offset {
		t.Fatalf("列表响应错误：%v", sessions)
	}
	closed := sessionTestResult(t, e, "session.close", map[string]string{"session_id": id})
	if closed["session_id"] != id || closed["closed"] != true || closed["cleanup_incomplete"] != false {
		t.Fatalf("关闭响应错误：%v", closed)
	}
}

func TestSessionWaitForOutput(t *testing.T) {
	e := testSessionEngine(t)
	s := startTestSession(t, e, "/bin/sh", "-c", `stty -echo; printf ready; read -r line; printf X; read -r line`)
	_, offset := readSessionUntil(t, e, s.id, func(s string) bool { return s == "ready" })

	readers := make(chan map[string]any, 2)
	for range 2 {
		go func() {
			readers <- s.read(sessionReadParams{Offset: offset, WaitMS: 2000})
		}()
	}
	select {
	case got := <-readers:
		t.Fatalf("尚无输出时读取提前返回：%v", got)
	case <-time.After(40 * time.Millisecond):
	}
	if n, err := s.write("wake\n"); n != 5 || err != nil {
		t.Fatalf("写入唤醒命令 = %d, %v", n, err)
	}
	early := time.NewTimer(time.Second)
	defer early.Stop()
	for range 2 {
		select {
		case got := <-readers:
			if got["output"] != "X" || got["next_offset"] != offset+1 {
				t.Fatalf("输出唤醒结果错误：%v", got)
			}
		case <-early.C:
			t.Fatal("有输出但未提前唤醒读者")
		}
	}
	start := time.Now()
	got := s.read(sessionReadParams{Offset: offset + 1, WaitMS: 100})
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond || elapsed > time.Second || got["output"] != "" || got["next_offset"] != offset+1 {
		t.Fatalf("空输出等待 %s，结果：%v", elapsed, got)
	}
}

func TestSessionOutputTruncation(t *testing.T) {
	e := testSessionEngine(t)
	const discarded = 257
	s := startTestSession(t, e, "/bin/sh", "-c", fmt.Sprintf(`head -c %d /dev/zero | tr '\000' x`, maxOutput+discarded))
	awaitSessionEnd(t, s)
	first := s.read(sessionReadParams{MaxBytes: 2 * defaultReadLimit})
	if first["output"] != strings.Repeat("x", defaultReadLimit) || first["truncated"] != true || first["base_offset"] != int64(discarded) || first["next_offset"] != int64(discarded+defaultReadLimit) {
		t.Fatalf("截断读取：base=%v next=%v truncated=%v len=%d", first["base_offset"], first["next_offset"], first["truncated"], len(first["output"].(string)))
	}
	next := s.read(sessionReadParams{Offset: int64(discarded + 1), MaxBytes: 2})
	if next["output"] != "xx" || next["base_offset"] != int64(discarded) || next["truncated"] != false || next["next_offset"] != int64(discarded+3) {
		t.Fatalf("保留区内读取错误：%v", next)
	}
}

func TestSessionUTF8AndANSI(t *testing.T) {
	s := &Session{exitCode: -1}
	_, _ = s.out.Write([]byte("a\u00e9\u754c\U0001f600z"))
	var offset int64
	for _, page := range []struct {
		limit int
		want  string
	}{{2, "a"}, {4, "\u00e9"}, {6, "\u754c"}, {4, "\U0001f600"}, {1, "z"}} {
		got := s.read(sessionReadParams{Offset: offset, MaxBytes: page.limit})
		str := got["output"].(string)
		if str != page.want || !utf8.ValidString(str) || got["next_offset"] != offset+int64(len(page.want)) {
			t.Fatalf("UTF-8 分页错误：offset=%d limit=%d got=%v", offset, page.limit, got)
		}
		offset = got["next_offset"].(int64)
	}
	_, _ = s.out.Write([]byte{0xe7, 0x95})
	got := s.read(sessionReadParams{Offset: offset})
	if got["output"] != "" || got["next_offset"] != offset {
		t.Fatalf("不完整字符被提前消费：%v", got)
	}
	_, _ = s.out.Write([]byte{0x8c})
	got = s.read(sessionReadParams{Offset: offset})
	if got["output"] != "\u754c" || got["next_offset"] != offset+3 {
		t.Fatalf("分块字符读取错误：%v", got)
	}
	offset += 3
	const ansi = "\x1b[31mred\x1b[0m\x1b]0;title\x07\x1b]8;;link\x1b\\link\x1b(B\r"
	_, _ = s.out.Write([]byte(ansi))
	got = s.read(sessionReadParams{Offset: offset, StripANSI: true})
	if got["output"] != "redlink\r" || got["next_offset"] != offset+int64(len(ansi)) {
		t.Fatalf("ANSI 剥离与偏移错误：%v", got)
	}
	if raw := s.read(sessionReadParams{Offset: offset})["output"]; raw != ansi {
		t.Fatalf("默认读取应保留 ANSI：%q", raw)
	}
}

func TestSessionCloseInterruptsWriteAndRead(t *testing.T) {
	e := testSessionEngine(t)
	s := startTestSession(t, e, "/bin/sh", "-c", `stty -echo -icanon; printf ready; exec /bin/sleep 300`)
	_, offset := readSessionUntil(t, e, s.id, func(s string) bool { return s == "ready" })
	written := make(chan error, 1)
	go func() {
		_, err := s.write(strings.Repeat("x", 8<<20))
		written <- err
	}()
	read := make(chan map[string]any, 1)
	go func() { read <- s.read(sessionReadParams{Offset: offset, WaitMS: 5000}) }()
	select {
	case err := <-written:
		t.Fatalf("写入应在关闭前阻塞：%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	start := time.Now()
	closed := sessionTestResult(t, e, "session.close", map[string]string{"session_id": s.id})
	if closed["cleanup_incomplete"] != false || time.Since(start) >= 3*time.Second {
		t.Fatalf("关闭未及时完成：%v", closed)
	}
	select {
	case err := <-written:
		var pe *protocol.Error
		if !errors.As(err, &pe) || pe.Code != "SESSION_ENDED" || pe.Data["bytes_written"] == nil {
			t.Fatalf("阻塞写入返回错误：%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("关闭未打断写入")
	}
	select {
	case got := <-read:
		if got["running"] != false {
			t.Fatalf("关闭后读取仍报告运行中：%v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("关闭未唤醒读者")
	}
}

func TestSessionProcessCleanup(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		exitCode int
	}{{"requested", -1}, {"natural", 7}} {
		t.Run(tc.name, func(t *testing.T) {
			e := testSessionEngine(t)
			s := startTestSession(t, e, executable, "-test.run=^TestSessionCleanupFixture$", "--", "session-cleanup-fixture")
			got, _ := readSessionUntil(t, e, s.id, func(s string) bool { return strings.HasPrefix(s, "child:") && strings.Contains(s, "\n") })
			pid, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(got, "child:")))
			if err != nil || pid <= 0 {
				t.Fatalf("无法解析子进程：%q", got)
			}
			group, err := syscall.Getpgid(pid)
			if err != nil || group == s.term.Pid() {
				t.Fatalf("未启动不同进程组的子进程：pgid=%d err=%v", group, err)
			}
			if tc.name == "requested" {
				got := sessionTestResult(t, e, "session.close", map[string]string{"session_id": s.id})
				if got["closed"] != true || got["cleanup_incomplete"] != false {
					t.Fatalf("子进程清理失败：%v", got)
				}
			} else if _, err := s.write("exit\n"); err != nil {
				t.Fatal(err)
			}
			awaitSessionEnd(t, s)
			awaitSessionCondition(t, "后台子进程已终止", func() bool { return !sessionPIDAlive(pid) })
			final := s.read(sessionReadParams{})
			if final["running"] != false || final["exit_code"] != tc.exitCode {
				t.Fatalf("退出状态错误：%v", final)
			}
			p, _ := json.Marshal(sessionWriteParams{SessionID: s.id, Input: "x"})
			failed := sessionTestRequest(e, "session.write", p).Error
			if failed == nil || failed.Code != "SESSION_ENDED" || failed.Data["bytes_written"] != 0 {
				t.Fatalf("已结束会话写入错误：%#v", failed)
			}
		})
	}
}

func TestSessionCleanupFixture(t *testing.T) {
	if os.Args[len(os.Args)-1] != "session-cleanup-fixture" {
		return
	}
	child := exec.Command("/bin/sh", "-c", `trap '' HUP; sleep 300 & child=$!; printf 'child:%d\n' "$child"; wait "$child"`)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	os.Exit(7)
}

func sessionPIDAlive(pid int) bool {
	if syscall.Kill(pid, 0) == syscall.ESRCH {
		return false
	}
	if runtime.GOOS == "linux" {
		// 容器中的 PID 1 可能不会回收孤儿僵尸；僵尸已不能执行或持有终端。
		stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
		if end := bytes.LastIndexByte(stat, ')'); err == nil && end >= 0 {
			fields := bytes.Fields(stat[end+1:])
			return len(fields) == 0 || string(fields[0]) != "Z"
		}
	}
	return true
}

func TestSessionCleanupFallback(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("此测试通过禁用 ps 模拟 Darwin 枚举失败")
	}
	t.Setenv("PATH", t.TempDir())
	e := testSessionEngine(t)
	s := startTestSession(t, e, "/bin/sleep", "300")
	got := sessionTestResult(t, e, "session.close", map[string]string{"session_id": s.id})
	if got["closed"] != true || got["cleanup_incomplete"] != true || !errors.Is(s.closeTerminal(), errCleanupIncomplete) {
		t.Fatalf("缺失进程枚举时未报告降级：%v", got)
	}
	awaitSessionEnd(t, s)
	if sessionPIDAlive(s.term.Pid()) {
		t.Fatal("进程枚举失败后未回退到进程组清理")
	}
}

func TestProcStatSessionID(t *testing.T) {
	sid, err := procStatSessionID([]byte("42 (name with ) parens)) S 11 22 33 0"))
	if err != nil || sid != 33 {
		t.Fatalf("解析 /proc stat = %d, %v", sid, err)
	}
}

func TestSessionQuota(t *testing.T) {
	e := testSessionEngine(t)
	oldest := startTestSession(t, e, "/bin/sh", "-c", "exit 0")
	awaitSessionEnd(t, oldest)
	retained := startTestSession(t, e, "/bin/sh", "-c", "exit 0")
	awaitSessionEnd(t, retained)
	for range maxSessions - 2 {
		startTestSession(t, e, "/bin/sleep", "300")
	}
	startTestSession(t, e, "/bin/sleep", "300")
	if _, err := e.sessions.get(oldest.id); err == nil {
		t.Fatal("第九个会话未淘汰最旧结束条目")
	}
	if _, err := e.sessions.get(retained.id); err != nil {
		t.Fatalf("较新的结束条目被提前淘汰：%v", err)
	}
	startTestSession(t, e, "/bin/sleep", "300")
	got := sessionTestRequest(e, "session.create", []byte(`{"command":"/bin/sleep","args":["300"]}`))
	if got.Error == nil || got.Error.Code != "SESSION_LIMIT" {
		t.Fatalf("运行中会话已满却未拒绝创建：%#v", got)
	}
}

func TestSessionConcurrentQuota(t *testing.T) {
	e := testSessionEngine(t)
	e.sessions.limit = 2
	const attempts = 8
	start := make(chan struct{})
	results := make(chan error, attempts)
	for range attempts {
		go func() {
			<-start
			_, err := e.sessions.create(sessionCreateParams{Command: "/bin/sleep", Args: []string{"300"}})
			results <- err
		}()
	}
	close(start)
	var accepted int
	for range attempts {
		err := <-results
		if err == nil {
			accepted++
		} else {
			var pe *protocol.Error
			if !errors.As(err, &pe) || pe.Code != "SESSION_LIMIT" {
				t.Errorf("并发创建返回非配额错误：%v", err)
			}
		}
	}
	if accepted != 2 {
		t.Fatalf("并发创建成功数 = %d，预期 2", accepted)
	}
}

func TestSessionIdleAndRetention(t *testing.T) {
	e := testSessionEngine(t)
	e.sessions.idleTimeout = 50 * time.Millisecond
	e.sessions.retention = 50 * time.Millisecond
	s := startTestSession(t, e, "/bin/sh", "-c", `printf ready; exec /bin/sleep 300`)
	awaitSessionCondition(t, "后台输出", func() bool {
		_, _, _, end := s.out.ReadLimit(0, 0)
		return end == 5
	})
	s.read(sessionReadParams{Offset: 5})
	if _, err := s.write(""); err != nil {
		t.Fatal(err)
	}
	listed := e.sessions.list()[0]
	if listed["last_active_at"] != listed["started_at"] {
		t.Fatalf("后台输出、空读写或列表续期了会话：%v", listed)
	}
	s.read(sessionReadParams{})
	s.mu.Lock()
	lastActive := s.lastActive
	s.mu.Unlock()
	if !lastActive.After(s.started) {
		t.Fatal("成功读取未续期")
	}
	awaitSessionCondition(t, "空闲超时", func() bool { return time.Since(lastActive) >= e.sessions.idleTimeout })
	e.sessions.reap()
	awaitSessionEnd(t, s)
	if got := s.read(sessionReadParams{}); got["running"] != false || got["output"] != "ready" {
		t.Fatalf("空闲回收后的保留输出错误：%v", got)
	}
	awaitSessionCondition(t, "结束条目过期", func() bool {
		e.sessions.reap()
		_, err := e.sessions.get(s.id)
		return err != nil
	})
}

type sessionTestErrorTerminal struct {
	terminal
}

func (sessionTestErrorTerminal) Write([]byte) (int, error) { return 2, os.ErrDeadlineExceeded }

func (sessionTestErrorTerminal) Close() error { return errors.New("close failed") }

func TestSessionWriteErrorData(t *testing.T) {
	e := testSessionEngine(t)
	e.sessions.mu.Lock()
	e.sessions.sessions["test"] = &Session{id: "test", instance: e.id, term: sessionTestErrorTerminal{}, started: time.Now()}
	e.sessions.mu.Unlock()
	for _, tc := range []struct {
		params, code string
		written      int
	}{
		{`{"session_id":"test","input":"abc"}`, "SESSION_WRITE_TIMEOUT", 2},
		{`{"session_id":"missing","input":"abc"}`, "SESSION_NOT_FOUND", 0},
		{`{"session_id":"test","input":123}`, "REQUEST_FAILED", 0},
	} {
		got := sessionTestRequest(e, "session.write", []byte(tc.params)).Error
		if got == nil || got.Code != tc.code || got.Data["bytes_written"] != tc.written {
			t.Fatalf("协议错误未保留写入计数：%#v", got)
		}
	}
	got := sessionTestRequest(e, "session.close", []byte(`{"session_id":"test"}`)).Error
	if got == nil || got.Code != "SESSION_CLEANUP_INCOMPLETE" || got.Data["cleanup_incomplete"] != true {
		t.Fatalf("终端关闭错误未保留清理状态：%#v", got)
	}
}
