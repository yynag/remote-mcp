package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestBridgeRemainsConnectedWithFrequentApplicationHeartbeats verifies a real
// WebSocket connection remains live beyond the 45-second read deadline.
func TestBridgeRemainsConnectedWithFrequentApplicationHeartbeats(t *testing.T) {
	timeout := 65 * time.Second
	testCtx, cancel := context.WithTimeout(context.Background(), timeout)
	var connections atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connections.Add(1)
		defer conn.Close()
		if err = conn.WriteJSON(wireMessage{Type: "hello", Instance: "heartbeat-test"}); err != nil {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-testCtx.Done():
				return
			case <-ticker.C:
				if err = conn.WriteJSON(wireMessage{Type: "heartbeat"}); err != nil {
					return
				}
			}
		}
	}))
	defer func() {
		cancel()
		server.Close()
	}()

	bridge := NewBridge(Options{URLs: []string{"ws" + strings.TrimPrefix(server.URL, "http")}})
	defer bridge.Close()
	if err := bridge.Start(testCtx); err != nil {
		t.Fatalf("启动 Bridge：%v", err)
	}

	select {
	case <-time.After(51 * time.Second):
	case <-testCtx.Done():
		t.Fatalf("心跳存活测试超时（限定 %s）：%v", timeout, testCtx.Err())
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("WebSocket 被重连，服务端接受连接数=%d，期望 1", got)
	}
	instances := bridge.List()
	if len(instances) != 1 || instances[0]["instance_id"] != "heartbeat-test" || instances[0]["online"] != true {
		t.Fatalf("心跳后实例不在线：%v", instances)
	}
}
