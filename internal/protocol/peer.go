package protocol

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Peer 串行化写操作，避免缓慢或失效的连接无限阻塞任务。
type Peer struct {
	Conn *websocket.Conn
	mu   sync.Mutex
	once sync.Once
	done chan struct{}
}

// NewPeer 创建串行写入并带心跳超时的 WebSocket 连接。
func NewPeer(conn *websocket.Conn) *Peer {
	p := &Peer{Conn: conn, done: make(chan struct{})}
	conn.SetReadLimit(8 * 1024 * 1024)
	_ = conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(45 * time.Second)) })
	return p
}

// Write 通过连接发送一条 JSON 消息。
func (p *Peer) Write(v any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := p.Conn.WriteJSON(v)
	if err != nil {
		p.Close()
	}
	return err
}

// Close 幂等关闭 WebSocket 连接。
func (p *Peer) Close() { p.once.Do(func() { close(p.done); _ = p.Conn.Close() }) }

// 心跳同时使用 WebSocket ping/pong 和应用层心跳，以兼容不同 MCP 客户端。
// Heartbeat 定时发送 WebSocket ping 和应用层心跳。
func (p *Peer) Heartbeat() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
			if err := p.Conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				p.Close()
				return
			}
			if p.Write(Message{Type: "heartbeat", Version: Version}) != nil {
				return
			}
		}
	}
}

// Read 读取一条 JSON 消息并刷新连接读取期限。
func (p *Peer) Read(v any) error {
	err := p.Conn.ReadJSON(v)
	if err == nil {
		_ = p.Conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	}
	return err
}

// Upgrader 接受 WebSocket 升级请求；部署时应限制在可信网络。
var Upgrader = websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, CheckOrigin: func(*http.Request) bool { return true }}
