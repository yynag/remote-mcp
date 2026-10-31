package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remote-mcp/internal/protocol"

	"github.com/gorilla/websocket"
)

// TestTransferResumeAfterClientReconnect 验证重连后从已完成分块继续上传和下载。
func TestTransferResumeAfterClientReconnect(t *testing.T) {
	directory := t.TempDir()
	buildBinaries(t)
	serverPort := freePort(t)
	guide := filepath.Join(directory, "guide.md")
	if err := os.WriteFile(guide, []byte("测试指南"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "server.yaml")
	configData := fmt.Appendf(nil, "instance_id: resume-instance\nguide: %q\nlisten: 127.0.0.1:%d\n", guide, serverPort)
	if err := os.WriteFile(config, configData, 0600); err != nil {
		t.Fatal(err)
	}
	serverProcess := exec.Command(serverBin, "--config", config)
	serverProcess.Stdout, serverProcess.Stderr = io.Discard, io.Discard
	if err := serverProcess.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopProcess(serverProcess) })
	waitTCP(t, serverPort)
	websocketURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", serverPort)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first := startTestBridge(t, ctx, websocketURL)

	source := filepath.Join(directory, "source.bin")
	writeTestFile(t, source, 2*defaultChunkSize+123)
	remote := filepath.Join(directory, "remote.bin")
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	whole, chunks, err := hashManifest(source, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	spec := protocol.TransferSpec{
		Path: remote, Direction: "upload", Size: info.Size(), SHA256: whole,
		ChunkSHA256: chunks, ChunkSize: defaultChunkSize, Concurrency: defaultConcurrency,
		MTime: stringInt(info.ModTime().UnixNano()),
	}
	preparedValue, err := first.Request(ctx, "resume-instance", "transfer.prepare", spec)
	if err != nil {
		t.Fatal(err)
	}
	upload, err := decodeStatus(preparedValue)
	if err != nil {
		t.Fatal(err)
	}
	statusValue, err := first.Request(ctx, "resume-instance", "transfer.status", map[string]any{"transfer_id": upload.TransferID})
	if err != nil {
		t.Fatal(err)
	}
	uploadStatus, err := decodeStatus(statusValue)
	if err != nil {
		t.Fatal(err)
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = first.transferChunk(ctx, "resume-instance", uploadStatus, "upload", 0, sourceFile, &transferSession{cancel: func() {}, sockets: make(map[*websocket.Conn]struct{})}); err != nil {
		_ = sourceFile.Close()
		t.Fatal(err)
	}
	_ = sourceFile.Close()
	first.Close()

	second := startTestBridge(t, ctx, websocketURL)
	result, err := second.transfer(ctx, transferArgs{
		InstanceID: "resume-instance", LocalPath: source, RemotePath: remote, TransferID: upload.TransferID,
	}, "upload")
	if err != nil {
		t.Fatal(err)
	}
	completed, err := decodeStatus(result)
	if err != nil || completed.State != "completed" {
		t.Fatalf("续传上传未完成：%+v，错误：%v", completed, err)
	}
	if got := testHash(t, remote); got != whole {
		t.Fatalf("上传后 SHA-256 = %s，期望 %s", got, whole)
	}

	downValue, err := second.Request(ctx, "resume-instance", "transfer.prepare", protocol.TransferSpec{
		Path: remote, Direction: "download", ChunkSize: defaultChunkSize, Concurrency: defaultConcurrency,
	})
	if err != nil {
		t.Fatal(err)
	}
	download, err := decodeStatus(downValue)
	if err != nil {
		t.Fatal(err)
	}
	downStatusValue, err := second.Request(ctx, "resume-instance", "transfer.status", map[string]any{"transfer_id": download.TransferID})
	if err != nil {
		t.Fatal(err)
	}
	downStatus, err := decodeStatus(downStatusValue)
	if err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(directory, "download.bin")
	partial := local + "." + download.TransferID + ".part"
	partialFile, err := os.OpenFile(partial, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = partialFile.Truncate(downStatus.Size); err != nil {
		_ = partialFile.Close()
		t.Fatal(err)
	}
	if err = second.transferChunk(ctx, "resume-instance", downStatus, "download", 0, partialFile, &transferSession{cancel: func() {}, sockets: make(map[*websocket.Conn]struct{})}); err != nil {
		_ = partialFile.Close()
		t.Fatal(err)
	}
	_ = partialFile.Close()
	second.Close()

	third := startTestBridge(t, ctx, websocketURL)
	result, err = third.transfer(ctx, transferArgs{
		InstanceID: "resume-instance", LocalPath: local, RemotePath: remote, TransferID: download.TransferID,
	}, "download")
	if err != nil {
		t.Fatal(err)
	}
	completed, err = decodeStatus(result)
	if err != nil || completed.State != "completed" {
		t.Fatalf("续传下载未完成：%+v，错误：%v", completed, err)
	}
	if got := testHash(t, local); got != whole {
		t.Fatalf("下载后 SHA-256 = %s，期望 %s", got, whole)
	}
}

func startTestBridge(t *testing.T, ctx context.Context, endpoint string) *Bridge {
	t.Helper()
	bridge := NewBridge(Options{URLs: []string{endpoint}})
	if err := bridge.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bridge.Close)
	return bridge
}

func writeTestFile(t *testing.T, path string, size int64) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	block := []byte(strings.Repeat("remote-mcp-resume-data", 4096))
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

func testHash(t *testing.T, path string) string {
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

func stringInt(value int64) string {
	return fmt.Sprintf("%d", value)
}
