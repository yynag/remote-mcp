package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	buildOnce    sync.Once
	buildErr     error
	serverBin    string
	clientBin    string
	transportBin string
	buildDir     string
)

// TestRealMCPFileTransferDirectAndReverse verifies file transfer and sessions over stdio MCP with real direct and relay connections.
func TestRealMCPFileTransferDirectAndReverse(t *testing.T) {
	buildBinaries(t)
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "reverse relay"}[reverse], func(t *testing.T) {
			runRealTransfer(t, reverse)
		})
	}
}

func buildBinaries(t *testing.T) {
	t.Helper()
	buildOnce.Do(func() {
		root := repositoryRoot()
		parent := filepath.Join(root, ".agent", "tmp")
		if err := os.MkdirAll(parent, 0700); err != nil {
			buildErr = err
			return
		}
		dir, err := os.MkdirTemp(parent, "go-mcp-integration-")
		if err != nil {
			buildErr = err
			return
		}
		// The binaries are used only by this test; the temporary directory is removed when the test finishes.
		serverBin = filepath.Join(dir, "server")
		clientBin = filepath.Join(dir, "client")
		transportBin = filepath.Join(dir, "transport")
		buildDir = dir
		for _, item := range []struct {
			output string
			pkg    string
		}{
			{serverBin, "./server"},
			{clientBin, "./client"},
			{transportBin, "./transport"},
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", item.output, item.pkg)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			cancel()
			if err != nil {
				buildErr = fmt.Errorf("build %s failed: %w\n%s", item.pkg, err, output)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

func runRealTransfer(t *testing.T, reverse bool) {
	t.Helper()
	dir := t.TempDir()
	serverPort := freePort(t)
	transportPort := freePort(t)
	instanceID := "integration-" + strconv.Itoa(serverPort)
	guidePath := filepath.Join(dir, "guide.md")
	if err := os.WriteFile(guidePath, []byte("# integration test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := map[string]any{
		"instance_id": instanceID,
		"guide":       "guide.md",
		"listen":      net.JoinHostPort("127.0.0.1", strconv.Itoa(serverPort)),
	}
	if reverse {
		config["gateway"] = net.JoinHostPort("127.0.0.1", strconv.Itoa(transportPort))
	}
	configBytes, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "server.yaml")
	if err = os.WriteFile(configPath, configBytes, 0600); err != nil {
		t.Fatal(err)
	}

	var transport *exec.Cmd
	if reverse {
		transport = exec.Command(transportBin, "--listen", net.JoinHostPort("127.0.0.1", strconv.Itoa(transportPort)))
		transport.Stdout = io.Discard
		transport.Stderr = io.Discard
		if err = transport.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stopProcess(transport) })
		waitTCP(t, transportPort)
	}
	server := exec.Command(serverBin, "--config", configPath)
	server.Stdout = io.Discard
	server.Stderr = io.Discard
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopProcess(server) })
	waitTCP(t, serverPort)

	argument := "--remotes"
	endpoint := net.JoinHostPort("127.0.0.1", strconv.Itoa(serverPort))
	if reverse {
		argument = "--gateway"
		endpoint = net.JoinHostPort("127.0.0.1", strconv.Itoa(transportPort))
	}
	client := exec.Command(clientBin, argument, endpoint)
	client.Stderr = io.Discard
	stdin, err := client.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := client.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopProcess(client) })
	mcp := newMCPStream(t, stdin, stdout)
	if _, err = mcp.call("initialize", map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "integration", "version": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err = mcp.notify("notifications/initialized", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	tools, err := mcp.call("tools/list", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	assertToolSchema(t, tools)
	deadline := time.Now().Add(15 * time.Second)
	for {
		instances := callToolValue(t, mcp, "instance_list", map[string]any{})
		if list, ok := instances.([]any); ok {
			for _, item := range list {
				if value, ok := item.(map[string]any); ok && value["instance_id"] == instanceID && value["online"] == true {
					if _, containsGuide := value["guide"]; containsGuide {
						t.Fatal("instance_list must not include the guide body")
					}
					goto instanceReady
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("instance did not come online")
		}
		time.Sleep(100 * time.Millisecond)
	}

instanceReady:
	guide := callTool(t, mcp, "instance_get", map[string]any{"instance_id": instanceID})
	if !strings.Contains(guide["guide"].(string), "integration test") {
		t.Fatalf("instance guide was not returned through MCP: %v", guide)
	}
	command := callTool(t, mcp, "exec_run", map[string]any{
		"instance_id": instanceID, "command": "printf 'mcp-stdio-exec'",
	})
	if command["stdout"] != "mcp-stdio-exec" {
		t.Fatalf("unexpected exec_run output: %v", command)
	}

	source := filepath.Join(dir, "source.bin")
	remote := filepath.Join(dir, "remote.bin")
	download := filepath.Join(dir, "download.bin")
	writeFixture(t, source, 5*1024*1024+123)
	callTool(t, mcp, "file_upload", map[string]any{
		"instance_id": instanceID, "local_path": source, "remote_path": remote,
	})
	stat := callTool(t, mcp, "file_stat", map[string]any{
		"instance_id": instanceID, "path": remote, "sha256": true,
	})
	wantHash := fileHash(t, source)
	if stat["sha256"] != wantHash {
		t.Fatalf("remote SHA-256 = %v, want %s", stat["sha256"], wantHash)
	}
	callTool(t, mcp, "file_download", map[string]any{
		"instance_id": instanceID, "local_path": download, "remote_path": remote,
	})
	if got := fileHash(t, download); got != wantHash {
		t.Fatalf("downloaded file SHA-256 = %s, want %s", got, wantHash)
	}
	t.Run("session", func(t *testing.T) {
		runRealSession(t, mcp, instanceID)
	})
}

func runRealSession(t *testing.T, mcp *mcpStream, instanceID string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("this session scenario requires Unix sh")
	}
	created := callTool(t, mcp, "session_create", map[string]any{
		"instance_id": instanceID, "command": "sh",
	})
	sessionID, ok := created["session_id"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("session_create did not return a session ID: %v", created)
	}
	const marker = "session-e2e"
	const input = "echo " + marker + "\n"
	written := callTool(t, mcp, "session_write", map[string]any{
		"instance_id": instanceID, "session_id": sessionID, "input": input,
	})
	if written["bytes_written"] != float64(len(input)) {
		t.Fatalf("unexpected session_write byte count: %v", written)
	}

	read := func(offset float64, waitMS int) (string, float64) {
		result := callTool(t, mcp, "session_read", map[string]any{
			"instance_id": instanceID, "session_id": sessionID, "offset": offset, "wait_ms": waitMS,
		})
		page, outputOK := result["output"].(string)
		next, offsetOK := result["next_offset"].(float64)
		if !outputOK || !offsetOK || next != offset+float64(len(page)) || result["truncated"] != false {
			t.Fatalf("unexpected session_read response from offset %v: %v", offset, result)
		}
		return page, next
	}
	var output strings.Builder
	var offset float64
	waitMS := 2000
	deadline := time.Now().Add(3 * time.Second)
	// Prompt, input echo, and command output may arrive in chunks; treat only the marker on its own line as command output.
	for {
		page, next := read(offset, waitMS)
		output.WriteString(page)
		offset = next
		if strings.Contains("\n"+strings.ReplaceAll(output.String(), "\r\n", "\n"), "\n"+marker+"\n") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session_read did not find command output %q; read %q", marker, output.String())
		}
		waitMS = 500
	}
	if offset <= 0 {
		t.Fatalf("session_read next_offset = %v, want greater than 0", offset)
	}
	if page, _ := read(offset, 100); strings.Contains(page, marker) {
		t.Fatalf("session_read from offset %v still returned old content: %q", offset, page)
	}

	listed := callTool(t, mcp, "session_list", map[string]any{"instance_id": instanceID})
	sessions, ok := listed["sessions"].([]any)
	if !ok || len(sessions) != 1 {
		t.Fatalf("unexpected session_list response: %v", listed)
	}
	session, ok := sessions[0].(map[string]any)
	if !ok || session["session_id"] != sessionID || session["running"] != true {
		t.Fatalf("session_list did not find running session %q: %v", sessionID, listed)
	}
	closed := callTool(t, mcp, "session_close", map[string]any{
		"instance_id": instanceID, "session_id": sessionID,
	})
	if closed["closed"] != true || closed["cleanup_incomplete"] != false {
		t.Fatalf("unexpected session_close result: %v", closed)
	}
}

type mcpStream struct {
	t    *testing.T
	in   io.Writer
	out  *bufio.Reader
	next int
}

func newMCPStream(t *testing.T, in io.Writer, out io.Reader) *mcpStream {
	return &mcpStream{t: t, in: in, out: bufio.NewReader(out), next: 1}
}

func (m *mcpStream) call(method string, params any) (map[string]any, error) {
	id := m.next
	m.next++
	message := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	if err := m.write(message); err != nil {
		return nil, err
	}
	for {
		line, err := m.out.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		var response map[string]any
		if err = json.Unmarshal(bytes.TrimSpace(line), &response); err != nil {
			return nil, err
		}
		if response["id"] == nil {
			continue
		}
		if response["error"] != nil {
			return nil, fmt.Errorf("MCP %s returned error: %v", method, response["error"])
		}
		result, ok := response["result"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("MCP %s response is missing result field", method)
		}
		return result, nil
	}
}

func (m *mcpStream) notify(method string, params any) error {
	return m.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (m *mcpStream) write(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(m.in, "%s\n", encoded)
	return err
}

func callTool(t *testing.T, mcp *mcpStream, name string, arguments map[string]any) map[string]any {
	t.Helper()
	value := callToolValue(t, mcp, name, arguments)
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("tool %s returned a non-object: %#v", name, value)
	}
	return result
}

func callToolValue(t *testing.T, mcp *mcpStream, name string, arguments map[string]any) any {
	t.Helper()
	result, err := mcp.call("tools/call", map[string]any{"name": name, "arguments": arguments})
	if err != nil {
		t.Fatal(err)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("tool %s returned unexpected content: %#v", name, result)
	}
	text, ok := content[0].(map[string]any)["text"].(string)
	if !ok {
		t.Fatalf("tool %s has no text result: %#v", name, result)
	}
	var value any
	if err = json.Unmarshal([]byte(text), &value); err != nil {
		t.Fatalf("tool %s returned non-JSON: %v; raw text: %s", name, err, text)
	}
	if object, ok := value.(map[string]any); ok {
		if errorValue, exists := object["error"]; exists {
			t.Fatalf("tool %s remote error: %v", name, errorValue)
		}
	}
	return value
}

func assertToolSchema(t *testing.T, result map[string]any) {
	t.Helper()
	tools, ok := result["tools"].([]any)
	if !ok || len(tools) != 19 {
		t.Fatalf("tools/list returned %d tools, want 19", len(tools))
	}
	seen := make(map[string]bool)
	for _, raw := range tools {
		tool := raw.(map[string]any)
		name := tool["name"].(string)
		seen[name] = true
		if _, ok = tool["inputSchema"].(map[string]any); !ok {
			t.Fatalf("tool %s is missing inputSchema", name)
		}
	}
	for _, name := range []string{
		"instance_list", "exec_run", "job_start", "job_output", "file_upload", "file_download", "transfer_status", "transfer_cancel",
		"session_create", "session_write", "session_read", "session_close", "session_list",
	} {
		if !seen[name] {
			t.Fatalf("tools/list is missing %s", name)
		}
	}
}

func writeFixture(t *testing.T, path string, size int64) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	block := bytes.Repeat([]byte("remote-mcp-fixture-"), 8192)
	for written := int64(0); written < size; {
		count := min(int64(len(block)), size-written)
		if _, err = file.Write(block[:count]); err != nil {
			t.Fatal(err)
		}
		written += count
	}
}

func fileHash(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitTCP(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("port %d did not start listening in time", port)
}

func stopProcess(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill()
		<-done
	}
}

func repositoryRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}
