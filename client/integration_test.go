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

// TestRealMCPFileTransferDirectAndReverse 验证 stdio MCP 和真实直连/中继文件传输。
func TestRealMCPFileTransferDirectAndReverse(t *testing.T) {
	buildBinaries(t)
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "直连", true: "反向中继"}[reverse], func(t *testing.T) {
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
		// 二进制只在本次测试中使用，测试结束后删除临时目录。
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
				buildErr = fmt.Errorf("构建 %s 失败：%w\n%s", item.pkg, err, output)
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
	if err := os.WriteFile(guidePath, []byte("# 集成测试\n"), 0600); err != nil {
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
					goto instanceReady
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("实例未上线")
		}
		time.Sleep(100 * time.Millisecond)
	}

instanceReady:
	guide := callTool(t, mcp, "instance_get", map[string]any{"instance_id": instanceID})
	if !strings.Contains(guide["guide"].(string), "集成测试") {
		t.Fatalf("实例指南未通过 MCP 返回：%v", guide)
	}
	command := callTool(t, mcp, "exec_run", map[string]any{
		"instance_id": instanceID, "command": "printf 'mcp-stdio-exec'",
	})
	if command["stdout"] != "mcp-stdio-exec" {
		t.Fatalf("exec_run 输出异常：%v", command)
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
		t.Fatalf("远端 SHA-256 = %v，期望 %s", stat["sha256"], wantHash)
	}
	callTool(t, mcp, "file_download", map[string]any{
		"instance_id": instanceID, "local_path": download, "remote_path": remote,
	})
	if got := fileHash(t, download); got != wantHash {
		t.Fatalf("下载文件 SHA-256 = %s，期望 %s", got, wantHash)
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
			return nil, fmt.Errorf("MCP %s error: %v", method, response["error"])
		}
		result, ok := response["result"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("MCP %s response has no result", method)
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
		t.Fatalf("工具 %s 返回了非对象：%#v", name, value)
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
		t.Fatalf("工具 %s 返回了异常内容：%#v", name, result)
	}
	text, ok := content[0].(map[string]any)["text"].(string)
	if !ok {
		t.Fatalf("工具 %s 没有文本结果：%#v", name, result)
	}
	var value any
	if err = json.Unmarshal([]byte(text), &value); err != nil {
		t.Fatalf("工具 %s 返回非 JSON：%v；原文：%s", name, err, text)
	}
	if object, ok := value.(map[string]any); ok {
		if errorValue, exists := object["error"]; exists {
			t.Fatalf("工具 %s 远端错误：%v", name, errorValue)
		}
	}
	return value
}

func assertToolSchema(t *testing.T, result map[string]any) {
	t.Helper()
	tools, ok := result["tools"].([]any)
	if !ok || len(tools) != 14 {
		t.Fatalf("tools/list 返回 %d 个工具，期望 14", len(tools))
	}
	seen := make(map[string]bool)
	for _, raw := range tools {
		tool := raw.(map[string]any)
		name := tool["name"].(string)
		seen[name] = true
		if _, ok = tool["inputSchema"].(map[string]any); !ok {
			t.Fatalf("工具 %s 缺少 inputSchema", name)
		}
	}
	for _, name := range []string{"instance_list", "exec_run", "job_start", "job_output", "file_upload", "file_download", "transfer_status", "transfer_cancel"} {
		if !seen[name] {
			t.Fatalf("tools/list 缺少 %s", name)
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
		count := int64(len(block))
		if count > size-written {
			count = size - written
		}
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
	t.Fatalf("端口 %d 未在限定时间内监听", port)
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
