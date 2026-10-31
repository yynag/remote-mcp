// Package transport 实现 WebSocket 中继和实例连接管理。
package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
)

type instance struct {
	Hello protocol.Hello
	Peer  *protocol.Peer
}

// Registry 管理实例、客户端及配对中的数据连接。
type Registry struct {
	sync.RWMutex
	instances  map[string]*instance
	clients    map[*protocol.Peer]string
	dataMu     sync.Mutex
	dataWait   map[string]pendingData
	dataActive map[*websocket.Conn]struct{}
	dataClosed bool
}

type pendingData struct {
	conn  *websocket.Conn
	h     protocol.DataHandshake
	timer *time.Timer
}

// NewRegistry 创建中继注册表。
func NewRegistry() *Registry {
	return &Registry{instances: make(map[string]*instance), clients: make(map[*protocol.Peer]string), dataWait: make(map[string]pendingData), dataActive: make(map[*websocket.Conn]struct{})}
}

// Handler 创建中继的 WebSocket 路由。
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/server", func(w http.ResponseWriter, req *http.Request) {
		conn, err := protocol.Upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		log.Printf("server connected remote=%s", req.RemoteAddr)
		r.serveServer(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/client", func(w http.ResponseWriter, req *http.Request) {
		conn, err := protocol.Upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		log.Printf("client connected remote=%s", req.RemoteAddr)
		r.serveClient(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/data", r.data)
	return mux
}

// 数据连接按传输和分块标识配对。中继为每一对连接独立转发二进制帧，不缓存文件。
func (r *Registry) data(w http.ResponseWriter, req *http.Request) {
	conn, err := protocol.Upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(protocol.MaxDataFrame)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var h protocol.DataHandshake
	if err = conn.ReadJSON(&h); err != nil {
		_ = conn.Close()
		return
	}
	if h.Type != "data" || h.PairID == "" || h.TransferID == "" || h.ClientID == "" || h.InstanceID == "" || h.ChunkIndex < 0 || (h.Direction != "upload" && h.Direction != "download") {
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if h.Role == "server" {
		r.dataMu.Lock()
		pending, ok := r.dataWait[h.PairID]
		if r.dataClosed || !ok || !sameDataIdentity(pending.h, h) {
			r.dataMu.Unlock()
			_ = conn.Close()
			return
		}
		delete(r.dataWait, h.PairID)
		pending.timer.Stop()
		r.dataActive[pending.conn] = struct{}{}
		r.dataActive[conn] = struct{}{}
		r.dataMu.Unlock()
		relayData(pending.conn, conn)
		r.dataMu.Lock()
		delete(r.dataActive, pending.conn)
		delete(r.dataActive, conn)
		r.dataMu.Unlock()
		return
	}
	if h.Role != "client" || h.PairID == "" {
		_ = conn.Close()
		return
	}
	r.RLock()
	s := r.instances[h.InstanceID]
	r.RUnlock()
	if s == nil {
		_ = conn.Close()
		return
	}
	// 实例的反向连接接收数据连接请求，并负责拨号连接中继的数据端点。
	// 数据连接与控制连接相互独立。
	r.dataMu.Lock()
	if _, exists := r.dataWait[h.PairID]; exists || r.dataClosed {
		r.dataMu.Unlock()
		_ = conn.Close()
		return
	}
	timer := time.AfterFunc(10*time.Second, func() {
		r.dataMu.Lock()
		defer r.dataMu.Unlock()
		if pending, ok := r.dataWait[h.PairID]; ok && pending.conn == conn {
			delete(r.dataWait, h.PairID)
			_ = conn.Close()
		}
	})
	r.dataWait[h.PairID] = pendingData{conn: conn, h: h, timer: timer}
	r.dataMu.Unlock()
	b, _ := json.Marshal(h)
	open := protocol.Message{Version: protocol.Version, Type: "data.open", ClientID: h.ClientID, InstanceID: h.InstanceID, Params: b}
	if err = s.Peer.Write(open); err != nil {
		r.dataMu.Lock()
		if pending, ok := r.dataWait[h.PairID]; ok && pending.conn == conn {
			delete(r.dataWait, h.PairID)
			timer.Stop()
		}
		r.dataMu.Unlock()
		_ = conn.Close()
		return
	}
}

func relayData(client, server *websocket.Conn) {
	done := make(chan struct{}, 2)
	activity := make(chan struct{}, 1)
	copyFrames := func(dst, src *websocket.Conn) {
		defer func() { done <- struct{}{} }()
		src.SetReadLimit(protocol.MaxDataFrame)
		for {
			typ, b, err := src.ReadMessage()
			if err != nil {
				return
			}
			if typ != websocket.BinaryMessage && typ != websocket.TextMessage {
				return
			}
			if typ == websocket.TextMessage {
				var control struct {
					Type string `json:"type"`
				}
				if json.Unmarshal(b, &control) != nil || (control.Type != "data.ready" && control.Type != "data.ack" && control.Type != "data.done" && control.Type != "data.error") {
					return
				}
			}
			_ = dst.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if err = dst.WriteMessage(typ, b); err != nil {
				return
			}
			select {
			case activity <- struct{}{}:
			default:
			}
		}
	}
	go copyFrames(server, client)
	go copyFrames(client, server)
	// 一侧传输时另一侧可能暂时没有数据。超时应作用于连接对，而不是分别限制两侧读取。
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-activity:
			timer.Reset(30 * time.Second)
		case <-done:
			_ = client.Close()
			_ = server.Close()
			<-done
			return
		case <-timer.C:
			_ = client.Close()
			_ = server.Close()
			<-done
			<-done
			return
		}
	}
}

func sameDataIdentity(a, b protocol.DataHandshake) bool {
	return a.Type == b.Type && a.PairID == b.PairID && a.InstanceID == b.InstanceID &&
		a.ClientID == b.ClientID && a.TransferID == b.TransferID && a.Direction == b.Direction &&
		a.ChunkIndex == b.ChunkIndex && a.Offset == b.Offset && a.Length == b.Length && a.SHA256 == b.SHA256
}

// Close 关闭待配对数据连接和所有控制连接。
func (r *Registry) Close() {
	r.dataMu.Lock()
	r.dataClosed = true
	for id, pending := range r.dataWait {
		pending.timer.Stop()
		_ = pending.conn.Close()
		delete(r.dataWait, id)
	}
	for c := range r.dataActive {
		_ = c.Close()
	}
	r.dataMu.Unlock()
	r.RLock()
	defer r.RUnlock()
	for _, s := range r.instances {
		s.Peer.Close()
	}
	for c := range r.clients {
		c.Close()
	}
}

func (r *Registry) snapshotLocked() []protocol.Hello {
	list := make([]protocol.Hello, 0, len(r.instances))
	for _, s := range r.instances {
		list = append(list, s.Hello)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].InstanceID < list[j].InstanceID })
	return list
}

func (r *Registry) broadcast(v any, clientID string) {
	r.RLock()
	peers := make([]*protocol.Peer, 0, len(r.clients))
	for p, id := range r.clients {
		if clientID == "" || clientID == id {
			peers = append(peers, p)
		}
	}
	r.RUnlock()
	for _, p := range peers {
		_ = p.Write(v)
	}
}

func (r *Registry) serveServer(p *protocol.Peer) {
	defer p.Close()
	var hello protocol.Hello
	if err := p.Read(&hello); err != nil || hello.Type != "hello" || hello.InstanceID == "" {
		log.Printf("invalid server hello: %v", err)
		return
	}
	r.Lock()
	old := r.instances[hello.InstanceID]
	r.instances[hello.InstanceID] = &instance{Hello: hello, Peer: p}
	r.Unlock()
	if old != nil {
		old.Peer.Close()
	}
	_ = p.Write(protocol.Message{Version: 1, Type: "welcome", InstanceID: hello.InstanceID})
	r.broadcast(hello, "")
	go p.Heartbeat()
	defer func() {
		r.Lock()
		removed := false
		if s := r.instances[hello.InstanceID]; s != nil && s.Peer == p {
			delete(r.instances, hello.InstanceID)
			removed = true
		}
		r.Unlock()
		if removed {
			r.broadcast(protocol.Message{Type: "instance.offline", InstanceID: hello.InstanceID}, "")
		}
	}()
	for {
		var m protocol.Message
		if err := p.Read(&m); err != nil {
			return
		}
		switch m.Type {
		case "heartbeat":
			_ = p.Write(protocol.Message{Type: "heartbeat_ack"})
		case "response", "event":
			if m.InstanceID == hello.InstanceID && m.ClientID != "" {
				r.broadcast(m, m.ClientID)
			}
		}
	}
}

func (r *Registry) serveClient(p *protocol.Peer) {
	r.Lock()
	r.clients[p] = ""
	// 将初始实例快照与注册、离线变更串行化。
	err := p.Write(protocol.Message{Version: 1, Type: "instances", Data: r.snapshotLocked()})
	r.Unlock()
	defer func() { r.Lock(); delete(r.clients, p); r.Unlock(); p.Close() }()
	if err != nil {
		return
	}
	go p.Heartbeat()
	clientID := ""
	for {
		var m protocol.Message
		if err := p.Read(&m); err != nil {
			return
		}
		switch m.Type {
		case "heartbeat":
			_ = p.Write(protocol.Message{Type: "heartbeat_ack"})
		case "request":
			if err := m.ValidateRequest(); err != nil {
				_ = p.Write(protocol.Failure(m, "INVALID_REQUEST", err))
				continue
			}
			if clientID == "" {
				clientID = m.ClientID
				r.Lock()
				r.clients[p] = clientID
				r.Unlock()
			}
			if m.ClientID != clientID {
				_ = p.Write(protocol.Failure(m, "INVALID_REQUEST", errors.New("client_id must be stable for one connection")))
				continue
			}
			r.routeRequest(p, m)
		}
	}
}

func (r *Registry) routeRequest(p *protocol.Peer, m protocol.Message) {
	if m.Method == "instance.list" {
		r.RLock()
		list := r.snapshotLocked()
		r.RUnlock()
		_ = p.Write(protocol.Success(m, map[string]any{"instances": list}))
		return
	}
	r.RLock()
	s := r.instances[m.InstanceID]
	r.RUnlock()
	if s == nil {
		_ = p.Write(protocol.Failure(m, "INSTANCE_OFFLINE", errors.New("instance is offline")))
		return
	}
	if err := s.Peer.Write(m); err != nil {
		_ = p.Write(protocol.Failure(m, "INSTANCE_OFFLINE", err))
	}
}
