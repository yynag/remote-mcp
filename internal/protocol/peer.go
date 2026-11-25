package protocol

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Peer serializes writes so a slow or failed connection cannot block tasks indefinitely.
type Peer struct {
	Conn *websocket.Conn
	mu   sync.Mutex
	once sync.Once
	done chan struct{}
}

// NewPeer creates a WebSocket connection with serialized writes and heartbeat timeouts.
func NewPeer(conn *websocket.Conn) *Peer {
	p := &Peer{Conn: conn, done: make(chan struct{})}
	conn.SetReadLimit(8 * 1024 * 1024)
	_ = conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(45 * time.Second)) })
	return p
}

// Write sends a JSON message over the connection.
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

// Close idempotently closes the WebSocket connection.
func (p *Peer) Close() { p.once.Do(func() { close(p.done); _ = p.Conn.Close() }) }

// Heartbeats use both WebSocket ping/pong and application-level heartbeats for compatibility with different MCP clients.
// Heartbeat periodically sends WebSocket pings and application-level heartbeats.
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

// Read reads a JSON message and refreshes the connection read deadline.
func (p *Peer) Read(v any) error {
	err := p.Conn.ReadJSON(v)
	if err == nil {
		_ = p.Conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	}
	return err
}

// Upgrader accepts WebSocket upgrade requests; deployments should restrict it to trusted networks.
var Upgrader = websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, CheckOrigin: func(*http.Request) bool { return true }}
