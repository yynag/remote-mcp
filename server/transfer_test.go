package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
)

func transferTestDir(t *testing.T) string {
	t.Helper()
	parent := filepath.Join("..", ".agent", "tmp")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "server-transfer-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

func transferEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	e := NewEngine("instance", "guide")
	mux := http.NewServeMux()
	mux.HandleFunc("/data", e.ServeData)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { e.Close(); srv.Close() })
	return e, "ws" + strings.TrimPrefix(srv.URL, "http") + "/data"
}

func transferCall(t *testing.T, e *Engine, method string, params any) map[string]any {
	t.Helper()
	b, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	response := e.Handle(protocol.Message{Version: 1, Type: "request", InstanceID: "instance", ClientID: "client", RequestID: "test", Method: method, Params: b})
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	return response.Result.(map[string]any)
}

func dataSocket(t *testing.T, url string, h protocol.DataHandshake) *websocket.Conn {
	t.Helper()
	c, _, err := (&websocket.Dialer{HandshakeTimeout: time.Second}).Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := c.WriteJSON(h); err != nil {
		t.Fatal(err)
	}
	return c
}

func dataReply(t *testing.T, c *websocket.Conn, kind string) {
	t.Helper()
	var result struct {
		Type  string `json:"type"`
		Error string `json:"error"`
	}
	if err := c.ReadJSON(&result); err != nil {
		t.Fatal(err)
	}
	if result.Type != kind {
		t.Fatalf("expected %s, got %+v", kind, result)
	}
}

// TestTransferChecksumRetryAndAtomicCommit 验证分块重试和原子提交行为。
func TestTransferChecksumRetryAndAtomicCommit(t *testing.T) {
	e, url := transferEngine(t)
	dir := transferTestDir(t)
	path := filepath.Join(dir, "received")
	payload := []byte("the real payload")
	hash := sha256.Sum256(payload)
	sha := hex.EncodeToString(hash[:])
	spec := protocol.TransferSpec{Path: path, Direction: "upload", Size: int64(len(payload)), SHA256: sha, ChunkSHA256: []string{sha}, MTime: "123"}
	prepared := transferCall(t, e, "transfer.prepare", spec)
	id := prepared["transfer_id"].(string)
	h := protocol.DataHandshake{Type: "data", InstanceID: "instance", ClientID: "client", TransferID: id, Direction: "upload", ChunkIndex: 0, Offset: 0, Length: int64(len(payload)), SHA256: sha}
	c := dataSocket(t, url, h)
	dataReply(t, c, "data.ready")
	wrong := append([]byte(nil), payload...)
	wrong[0] ^= 1
	if err := c.WriteMessage(websocket.BinaryMessage, wrong); err != nil {
		t.Fatal(err)
	}
	dataReply(t, c, "data.error")
	if len(transferCall(t, e, "transfer.status", protocol.TransferSpec{TransferID: id})["completed_chunks"].([]int)) != 0 {
		t.Fatal("bad chunk marked complete")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("uncommitted destination exists: %v", err)
	}
	c = dataSocket(t, url, h)
	dataReply(t, c, "data.ready")
	if err := c.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatal(err)
	}
	dataReply(t, c, "data.done")
	if transferCall(t, e, "transfer.commit", protocol.TransferSpec{TransferID: id})["state"] != "completed" {
		t.Fatal("not committed")
	}
	actual, err := os.ReadFile(path)
	if err != nil || string(actual) != string(payload) {
		t.Fatalf("payload: %q, %v", actual, err)
	}
	transferCall(t, e, "transfer.commit", protocol.TransferSpec{TransferID: id})
	if transferCall(t, e, "transfer.cancel", protocol.TransferSpec{TransferID: id})["state"] != "completed" {
		t.Fatal("cancel downgraded committed state")
	}
}

// TestTransferFourSocketLimitAndCancelCleanup 验证四路并发上限及取消清理。
func TestTransferFourSocketLimitAndCancelCleanup(t *testing.T) {
	e, url := transferEngine(t)
	dir := transferTestDir(t)
	path := filepath.Join(dir, "cancelled")
	hash := sha256.Sum256(nil)
	sha := hex.EncodeToString(hash[:])
	spec := protocol.TransferSpec{Path: path, Direction: "upload", Size: 5 * protocol.DefaultChunkSize, SHA256: sha, ChunkSHA256: []string{sha, sha, sha, sha, sha}}
	prepared := transferCall(t, e, "transfer.prepare", spec)
	id := prepared["transfer_id"].(string)
	tr, err := e.transfers.get(id, "client")
	if err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	temp := tr.tmp
	tr.mu.Unlock()
	for i := 0; i < 5; i++ {
		c := dataSocket(t, url, protocol.DataHandshake{Type: "data", InstanceID: "instance", ClientID: "client", TransferID: id,
			Direction: "upload", ChunkIndex: i, Offset: int64(i) * protocol.DefaultChunkSize, Length: protocol.DefaultChunkSize, SHA256: sha})
		if i < 4 {
			dataReply(t, c, "data.ready")
		} else {
			dataReply(t, c, "data.error")
		}
	}
	tr.mu.Lock()
	active := len(tr.active)
	tr.mu.Unlock()
	if active != 4 {
		t.Fatalf("active sockets = %d", active)
	}
	if transferCall(t, e, "transfer.cancel", protocol.TransferSpec{TransferID: id})["state"] != "cancelled" {
		t.Fatal("not cancelled")
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("partial not removed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("destination created: %v", err)
	}
	tr.mu.Lock()
	open := tr.file != nil
	tr.mu.Unlock()
	if open {
		t.Fatal("cancel left file open")
	}
}

// TestTransferRetentionAndResumeManifest 验证续传清单及 30 分钟保留期限。
func TestTransferRetentionAndResumeManifest(t *testing.T) {
	e, _ := transferEngine(t)
	dir := transferTestDir(t)
	hash := sha256.Sum256([]byte("a"))
	sha := hex.EncodeToString(hash[:])
	spec := protocol.TransferSpec{Path: filepath.Join(dir, "resume"), Direction: "upload", Size: 1, SHA256: sha, ChunkSHA256: []string{sha}, MTime: "123"}
	prepared := transferCall(t, e, "transfer.prepare", spec)
	spec.TransferID = prepared["transfer_id"].(string)
	tr, err := e.transfers.get(spec.TransferID, "client")
	if err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	retention := time.Until(tr.expires)
	temp := tr.tmp
	tr.mu.Unlock()
	if retention < 29*time.Minute || retention > 30*time.Minute {
		t.Fatalf("retention = %v", retention)
	}
	bad := spec
	bad.ChunkSHA256 = []string{strings.Repeat("0", 64)}
	if _, err := e.transfers.prepare(bad, "new-client"); err == nil {
		t.Fatal("changed chunk manifest accepted")
	}
	if _, err := e.transfers.prepare(spec, "new-client"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.transfers.get(spec.TransferID, "new-client"); err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	tr.expires = time.Now().Add(-time.Second)
	tr.mu.Unlock()
	if _, err := e.transfers.get(spec.TransferID, "new-client"); err == nil {
		t.Fatal("expired transfer accepted")
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("expired partial left behind: %v", err)
	}
}
