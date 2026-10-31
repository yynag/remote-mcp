package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
)

func transportSocket(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := (&websocket.Dialer{HandshakeTimeout: time.Second}).Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	return c
}

// TestDataPairConsumesBothHellosAndChecksIdentity 检查中继配对和握手身份校验。
func TestDataPairConsumesBothHellosAndChecksIdentity(t *testing.T) {
	r := NewRegistry()
	srv := httptest.NewServer(r.Handler())
	t.Cleanup(func() { r.Close(); srv.Close() })
	base := "ws" + strings.TrimPrefix(srv.URL, "http")
	control := transportSocket(t, base+"/server")
	if err := control.WriteJSON(protocol.Hello{Type: "hello", InstanceID: "server", Guide: "test"}); err != nil {
		t.Fatal(err)
	}
	var welcome protocol.Message
	if err := control.ReadJSON(&welcome); err != nil {
		t.Fatal(err)
	}
	if welcome.Type != "welcome" {
		t.Fatalf("unexpected welcome: %+v", welcome)
	}
	h := protocol.DataHandshake{Type: "data", Role: "client", PairID: "pair", InstanceID: "server", ClientID: "client",
		TransferID: "transfer", ChunkIndex: 7, Direction: "upload", Offset: 7 * protocol.DefaultChunkSize, Length: 4, SHA256: "hash"}
	client := transportSocket(t, base+"/data")
	if err := client.WriteJSON(h); err != nil {
		t.Fatal(err)
	}
	var opened protocol.Message
	if err := control.ReadJSON(&opened); err != nil {
		t.Fatal(err)
	}
	if opened.Type != "data.open" || opened.ClientID != h.ClientID || opened.InstanceID != h.InstanceID {
		t.Fatalf("bad open: %+v", opened)
	}

	bad := h
	bad.Role = "server"
	bad.ChunkIndex++
	wrong := transportSocket(t, base+"/data")
	if err := wrong.WriteJSON(bad); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wrong.ReadMessage(); err == nil {
		t.Fatal("mismatched chunk identity was paired")
	}
	r.dataMu.Lock()
	waiting := len(r.dataWait)
	r.dataMu.Unlock()
	if waiting != 1 {
		t.Fatal("mismatched server stole the pending client")
	}

	server := transportSocket(t, base+"/data")
	h.Role = "server"
	if err := server.WriteJSON(h); err != nil {
		t.Fatal(err)
	}
	if err := server.WriteJSON(map[string]string{"type": "data.ready"}); err != nil {
		t.Fatal(err)
	}
	var ready struct {
		Type string `json:"type"`
	}
	if err := client.ReadJSON(&ready); err != nil {
		t.Fatal(err)
	}
	if ready.Type != "data.ready" {
		t.Fatalf("a consumed hello was relayed: %+v", ready)
	}
	payload := []byte("file")
	if err := client.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatal(err)
	}
	kind, actual, err := server.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.BinaryMessage || string(actual) != string(payload) {
		t.Fatalf("relay: kind=%d data=%q", kind, actual)
	}
	// 后续 hello 不是合法的数据控制帧，不得继续转发。
	if err := client.WriteJSON(h); err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.ReadMessage(); err == nil {
		t.Fatal("relay forwarded a second hello")
	}
}

// TestDataShutdownClosesPendingPair 确认中继退出时会关闭等待配对的连接。
func TestDataShutdownClosesPendingPair(t *testing.T) {
	r := NewRegistry()
	srv := httptest.NewServer(r.Handler())
	t.Cleanup(func() { r.Close(); srv.Close() })
	base := "ws" + strings.TrimPrefix(srv.URL, "http")
	control := transportSocket(t, base+"/server")
	if err := control.WriteJSON(protocol.Hello{Type: "hello", InstanceID: "server", Guide: "test"}); err != nil {
		t.Fatal(err)
	}
	var welcome protocol.Message
	if err := control.ReadJSON(&welcome); err != nil {
		t.Fatal(err)
	}
	client := transportSocket(t, base+"/data")
	if err := client.WriteJSON(protocol.DataHandshake{Type: "data", Role: "client", PairID: "pending", InstanceID: "server", ClientID: "client", TransferID: "transfer", Direction: "upload"}); err != nil {
		t.Fatal(err)
	}
	var opened protocol.Message
	if err := control.ReadJSON(&opened); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if _, _, err := client.ReadMessage(); err == nil {
		t.Fatal("pending data socket survived shutdown")
	}
	r.dataMu.Lock()
	waiting := len(r.dataWait)
	r.dataMu.Unlock()
	if waiting != 0 {
		t.Fatalf("shutdown left %d pending pairs", waiting)
	}
}
