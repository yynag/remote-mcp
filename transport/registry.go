// Package transport implements WebSocket relay and instance connection management.
package main

import (
	"encoding/json"
	"errors"
	"log/slog"
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

// Registry manages instances, clients, and data connections being paired.
type Registry struct {
	sync.RWMutex
	instances  map[string]*instance
	clients    map[*protocol.Peer]string
	dataMu     sync.Mutex
	dataWait   map[string]pendingData
	dataActive map[*websocket.Conn]protocol.DataHandshake
	dataClosed bool
}

type pendingData struct {
	conn  *websocket.Conn
	h     protocol.DataHandshake
	timer *time.Timer
}

// NewRegistry creates a relay registry.
func NewRegistry() *Registry {
	return &Registry{instances: make(map[string]*instance), clients: make(map[*protocol.Peer]string), dataWait: make(map[string]pendingData), dataActive: make(map[*websocket.Conn]protocol.DataHandshake)}
}

// Handler creates the relay's WebSocket routes.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/server", func(w http.ResponseWriter, req *http.Request) {
		conn, err := protocol.Upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		slog.Info("server connected", "remote", req.RemoteAddr)
		r.serveServer(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/client", func(w http.ResponseWriter, req *http.Request) {
		conn, err := protocol.Upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		slog.Info("client connected", "remote", req.RemoteAddr)
		r.serveClient(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/data", r.data)
	return mux
}

// Data connections are paired by transfer and chunk identity. The relay forwards binary frames independently for each pair and does not cache files.
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
			slog.Warn("data connection pairing failed", "instance_id", h.InstanceID, "client_id", h.ClientID, "transfer_id", h.TransferID, "pair_id", h.PairID, "chunk_index", h.ChunkIndex)
			r.dataMu.Unlock()
			_ = conn.Close()
			return
		}
		delete(r.dataWait, h.PairID)
		pending.timer.Stop()
		r.dataActive[pending.conn] = pending.h
		r.dataActive[conn] = h
		r.dataMu.Unlock()
		slog.Info("data connection paired", "instance_id", h.InstanceID, "client_id", h.ClientID, "transfer_id", h.TransferID, "pair_id", h.PairID, "chunk_index", h.ChunkIndex)
		relayData(pending.conn, conn)
		r.dataMu.Lock()
		delete(r.dataActive, pending.conn)
		delete(r.dataActive, conn)
		r.dataMu.Unlock()
		slog.Info("data relay finished", "instance_id", h.InstanceID, "client_id", h.ClientID, "transfer_id", h.TransferID, "pair_id", h.PairID, "chunk_index", h.ChunkIndex)
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
	// The instance's reverse connection receives data connection requests and dials the relay's data endpoint.
	// Data connections are independent of control connections.
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
			slog.Warn("data connection pairing timed out", "instance_id", h.InstanceID, "client_id", h.ClientID, "transfer_id", h.TransferID, "pair_id", h.PairID, "chunk_index", h.ChunkIndex)
		}
	})
	r.dataWait[h.PairID] = pendingData{conn: conn, h: h, timer: timer}
	r.dataMu.Unlock()
	slog.Info("data connection waiting for pairing", "instance_id", h.InstanceID, "client_id", h.ClientID, "transfer_id", h.TransferID, "pair_id", h.PairID, "chunk_index", h.ChunkIndex)
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
	// While one side is transmitting, the other side may temporarily have no data. The timeout applies to the connection pair, not to reads on each side separately.
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

// Close closes pending data connections and all control connections.
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
		slog.Warn("invalid server hello", "error", err)
		return
	}
	r.Lock()
	old := r.instances[hello.InstanceID]
	r.instances[hello.InstanceID] = &instance{Hello: hello, Peer: p}
	r.Unlock()
	if old != nil {
		slog.Info("server instance replaced", "instance_id", hello.InstanceID)
		r.closeDataForInstance(hello.InstanceID)
		old.Peer.Close()
	} else {
		slog.Info("server instance registered", "instance_id", hello.InstanceID)
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
			r.closeDataForInstance(hello.InstanceID)
			slog.Info("server instance went offline", "instance_id", hello.InstanceID)
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
	clientID := ""
	defer func() {
		slog.Info("client disconnected", "client_id", clientID)
		r.Lock()
		delete(r.clients, p)
		r.Unlock()
		p.Close()
	}()
	r.Lock()
	r.clients[p] = ""
	// Serialize the initial instance snapshot with registration and offline changes.
	err := p.Write(protocol.Message{Version: 1, Type: "instances", Data: r.snapshotLocked()})
	r.Unlock()
	if err != nil {
		return
	}
	go p.Heartbeat()
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
				_ = p.Write(protocol.Failure(m, "INVALID_REQUEST", errors.New("client_id must remain unchanged on the same connection")))
				continue
			}
			r.routeRequest(p, m)
		}
	}
}

// closeDataForInstance closes pending and active data connections for an instance so state does not linger after the control connection closes.
func (r *Registry) closeDataForInstance(instanceID string) {
	r.dataMu.Lock()
	defer r.dataMu.Unlock()
	for pairID, pending := range r.dataWait {
		if pending.h.InstanceID != instanceID {
			continue
		}
		pending.timer.Stop()
		_ = pending.conn.Close()
		delete(r.dataWait, pairID)
		slog.Info("cleaned up pending data connection", "instance_id", pending.h.InstanceID, "client_id", pending.h.ClientID, "transfer_id", pending.h.TransferID, "pair_id", pending.h.PairID, "chunk_index", pending.h.ChunkIndex)
	}
	for conn, h := range r.dataActive {
		if h.InstanceID != instanceID {
			continue
		}
		_ = conn.Close()
		delete(r.dataActive, conn)
		slog.Info("cleaned up active data connection", "instance_id", h.InstanceID, "client_id", h.ClientID, "transfer_id", h.TransferID, "pair_id", h.PairID, "chunk_index", h.ChunkIndex)
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
