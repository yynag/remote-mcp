package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
	"remote-mcp/internal/wsdial"
)

type wireMessage struct {
	Version   int             `json:"version,omitempty"`
	Type      string          `json:"type"`
	ClientID  string          `json:"client_id,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	Instance  string          `json:"instance_id,omitempty"`
	Guide     string          `json:"guide,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *protocol.Error `json:"error,omitempty"`
	Event     string          `json:"event,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

type remoteInstance struct {
	guide  string
	online bool
	conn   *connection
}

type rpcReply struct {
	result json.RawMessage
	err    error
}

type pendingRequest struct {
	conn     *connection
	instance string
	result   chan rpcReply
}

type connection struct {
	bridge   *Bridge
	url      string
	gateway  bool
	mu       sync.RWMutex
	writeMu  sync.Mutex
	conn     *websocket.Conn
	stopped  bool
	lastSeen atomic.Int64
}

// Bridge manages direct and relay connections, instance state, and request/response handling.
type Bridge struct {
	clientID    string
	options     Options
	mu          sync.RWMutex
	instances   map[string]*remoteInstance
	pending     map[string]pendingRequest
	connections []*connection
	cache       *JobCache
	transferMu  sync.Mutex
	transfers   map[string]*transferSession
	ctx         context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
}

// NewBridge creates a WebSocket client bridge.
func NewBridge(options Options) *Bridge {
	ctx, cancel := context.WithCancel(context.Background())
	bridge := &Bridge{
		clientID:  newID(),
		options:   options,
		instances: make(map[string]*remoteInstance),
		pending:   make(map[string]pendingRequest),
		cache:     newJobCache(),
		transfers: make(map[string]*transferSession),
		ctx:       ctx,
		cancel:    cancel,
	}
	for _, endpoint := range options.URLs {
		bridge.connections = append(bridge.connections, &connection{bridge: bridge, url: endpoint, gateway: options.Gateway})
	}
	return bridge
}

// Start tries all endpoints in parallel; after at least one connection succeeds it keeps reconnecting in the background.
func (b *Bridge) Start(ctx context.Context) error {
	if len(b.connections) == 0 {
		return errors.New("no remote address to connect to")
	}
	results := make(chan error, len(b.connections))
	for _, conn := range b.connections {
		go func(c *connection) { results <- c.connect(ctx) }(conn)
	}
	connected := 0
	for range b.connections {
		if err := <-results; err == nil {
			connected++
		}
	}
	if connected == 0 {
		for _, conn := range b.connections {
			go conn.reconnect()
		}
		return errors.New("no remote endpoint is available")
	}
	for _, conn := range b.connections {
		go conn.reconnect()
	}
	go b.checkLoop()
	return nil
}

func (c *connection) connect(ctx context.Context) error {
	dialer := wsdial.Control()
	conn, response, err := dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		if response != nil {
			return fmt.Errorf("connect %s: HTTP %s", c.url, response.Status)
		}
		return fmt.Errorf("connect %s: %w", c.url, err)
	}
	wsdial.LogConnection("client.control", conn)
	conn.SetReadLimit(8 * 1024 * 1024)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hello wireMessage
	if err = conn.ReadJSON(&hello); err != nil {
		_ = conn.Close()
		return fmt.Errorf("read endpoint introduction %s: %w", c.url, err)
	}
	if (c.gateway && hello.Type != "instances") || (!c.gateway && hello.Type != "hello") {
		_ = conn.Close()
		return fmt.Errorf("endpoint %s returned an invalid introduction message %q", c.url, hello.Type)
	}
	_ = conn.SetReadDeadline(time.Time{})
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		_ = conn.Close()
		return errors.New("connection is closed")
	}
	c.conn = conn
	c.mu.Unlock()
	c.lastSeen.Store(time.Now().UnixNano())
	c.bridge.receive(c, hello)
	return nil
}

func (c *connection) reconnect() {
	for {
		if c.bridge.ctx.Err() != nil {
			return
		}
		if !c.connected() {
			if err := c.connect(c.bridge.ctx); err != nil {
				slog.Error("remote connection failed", "error", err)
				if !waitContext(c.bridge.ctx, 2*time.Second) {
					return
				}
				continue
			}
		}
		c.readLoop()
		c.disconnect()
		if !waitContext(c.bridge.ctx, 2*time.Second) {
			return
		}
	}
}

func (c *connection) readLoop() {
	conn := c.current()
	if conn == nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	conn.SetPongHandler(func(string) error {
		c.lastSeen.Store(time.Now().UnixNano())
		return conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	})
	for c.bridge.ctx.Err() == nil {
		var message wireMessage
		if err := conn.ReadJSON(&message); err != nil {
			slog.Warn("remote connection lost", "address", c.url, "error", err)
			return
		}
		// Application-level heartbeats also prove the connection is alive, so the read deadline must be refreshed; do not rely on pong alone.
		_ = conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		c.lastSeen.Store(time.Now().UnixNano())
		c.bridge.receive(c, message)
	}
}

func (c *connection) disconnect() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
		c.bridge.disconnected(c)
	}
}

func (c *connection) connected() bool { return c.current() != nil }

func (c *connection) current() *websocket.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stopped {
		return nil
	}
	return c.conn
}

func (c *connection) send(message any) error {
	conn := c.current()
	if conn == nil {
		return errors.New("INSTANCE_OFFLINE: WebSocket is not connected")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteJSON(message); err != nil {
		_ = conn.Close()
		return err
	}
	return nil
}

func (c *connection) close() {
	c.mu.Lock()
	c.stopped = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (b *Bridge) receive(conn *connection, message wireMessage) {
	switch message.Type {
	case "hello":
		b.introduce(conn, []protocol.Hello{{Type: "hello", InstanceID: message.Instance}})
		return
	case "instances":
		b.introduce(conn, decodeInstances(message.Data))
		return
	case "heartbeat":
		_ = conn.send(map[string]any{"type": "heartbeat_ack"})
		return
	case "instance.offline":
		b.markOffline(conn, message.Instance)
		return
	case "event":
		if message.ClientID != b.clientID || message.Instance == "" {
			return
		}
		var data any
		_ = json.Unmarshal(message.Data, &data)
		b.cache.event(message.Instance, message.Event, data)
		return
	case "response":
		b.receiveResponse(conn, message)
	}
}

func decodeInstances(raw json.RawMessage) []protocol.Hello {
	var instances []protocol.Hello
	_ = json.Unmarshal(raw, &instances)
	return instances
}

func (b *Bridge) introduce(conn *connection, instances []protocol.Hello) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conn.gateway {
		for _, instance := range b.instances {
			if instance.conn == conn {
				instance.online = false
			}
		}
	}
	for _, hello := range instances {
		if hello.InstanceID == "" {
			continue
		}
		b.instances[hello.InstanceID] = &remoteInstance{guide: hello.Guide, online: true, conn: conn}
	}
}

func (b *Bridge) markOffline(conn *connection, instanceID string) {
	b.mu.Lock()
	if instance := b.instances[instanceID]; instance != nil && instance.conn == conn {
		instance.online = false
	}
	b.mu.Unlock()
	b.cache.disconnected(instanceID)
}

func (b *Bridge) disconnected(conn *connection) {
	b.mu.Lock()
	for id, instance := range b.instances {
		if instance.conn == conn {
			instance.online = false
			b.cache.disconnected(id)
		}
	}
	b.mu.Unlock()
}

func (b *Bridge) receiveResponse(conn *connection, message wireMessage) {
	if message.ClientID != b.clientID || message.RequestID == "" {
		return
	}
	b.mu.Lock()
	pending, ok := b.pending[message.RequestID]
	if ok && pending.conn == conn {
		delete(b.pending, message.RequestID)
	}
	b.mu.Unlock()
	if !ok || pending.conn != conn {
		return
	}
	if message.Error != nil {
		pending.result <- rpcReply{err: message.Error}
		return
	}
	pending.result <- rpcReply{result: append(json.RawMessage(nil), message.Result...)}
}

// List returns the discovered instances and their current online state.
func (b *Bridge) List() []map[string]any {
	b.mu.RLock()
	list := make([]map[string]any, 0, len(b.instances))
	for id, instance := range b.instances {
		list = append(list, map[string]any{"instance_id": id, "online": instance.online && instance.conn.connected()})
	}
	b.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool { return list[i]["instance_id"].(string) < list[j]["instance_id"].(string) })
	return list
}

// Request sends a control protocol request to the specified instance.
func (b *Bridge) Request(ctx context.Context, instanceID, method string, params any) (any, error) {
	b.mu.RLock()
	instance := b.instances[instanceID]
	b.mu.RUnlock()
	if instance == nil || !instance.online || !instance.conn.connected() {
		return nil, fmt.Errorf("INSTANCE_OFFLINE: %s", instanceID)
	}
	return b.requestOn(ctx, instance.conn, instanceID, method, params)
}

func (b *Bridge) requestOn(ctx context.Context, conn *connection, instanceID, method string, params any) (any, error) {
	requestID := newID()
	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	result := make(chan rpcReply, 1)
	b.mu.Lock()
	if b.ctx.Err() != nil {
		b.mu.Unlock()
		return nil, errors.New("CLIENT_CLOSED")
	}
	b.pending[requestID] = pendingRequest{conn: conn, instance: instanceID, result: result}
	b.mu.Unlock()
	request := wireMessage{Version: protocol.Version, Type: "request", ClientID: b.clientID, RequestID: requestID, Instance: instanceID, Method: method, Params: encoded}
	if err := conn.send(request); err != nil {
		b.mu.Lock()
		delete(b.pending, requestID)
		b.mu.Unlock()
		return nil, fmt.Errorf("send request: %w", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, requestBudget(method, encoded))
		defer cancel()
	}
	select {
	case response := <-result:
		if response.err != nil {
			if remoteErr, ok := response.err.(*protocol.Error); ok && remoteErr.Code == "METHOD_NOT_FOUND" && strings.HasPrefix(method, "session.") {
				return nil, errors.New("this instance does not support interactive sessions; upgrade the server")
			}
			return nil, response.err
		}
		var value any
		if len(response.result) != 0 {
			if err := json.Unmarshal(response.result, &value); err != nil {
				return nil, err
			}
		}
		return value, nil
	case <-ctx.Done():
		b.mu.Lock()
		delete(b.pending, requestID)
		b.mu.Unlock()
		return nil, fmt.Errorf("request %s: %w", method, ctx.Err())
	case <-b.ctx.Done():
		return nil, errors.New("CLIENT_CLOSED")
	}
}

func requestBudget(method string, params json.RawMessage) time.Duration {
	const defaultBudget = 30 * time.Second
	switch method {
	case "exec.run":
		var command struct {
			TimeoutMS int64 `json:"timeout_ms"`
		}
		_ = json.Unmarshal(params, &command)
		if command.TimeoutMS == 0 {
			command.TimeoutMS = 30_000
		}
		return time.Duration(command.TimeoutMS)*time.Millisecond + 15*time.Second
	case "session.read":
		var read struct {
			WaitMS int64 `json:"wait_ms"`
		}
		_ = json.Unmarshal(params, &read)
		return max(defaultBudget, time.Duration(read.WaitMS)*time.Millisecond+15*time.Second)
	default:
		return defaultBudget
	}
}

// ConnectionFor returns the WebSocket address of the instance's current control connection.
func (b *Bridge) ConnectionFor(instanceID string) (string, error) {
	connection, err := b.connectionFor(instanceID)
	if err != nil {
		return "", err
	}
	return connection.url, nil
}

func (b *Bridge) connectionFor(instanceID string) (*connection, error) {
	b.mu.RLock()
	instance := b.instances[instanceID]
	b.mu.RUnlock()
	if instance == nil || !instance.online || !instance.conn.connected() {
		return nil, fmt.Errorf("INSTANCE_OFFLINE: %s", instanceID)
	}
	return instance.conn, nil
}

func (b *Bridge) checkLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case now := <-ticker.C:
			b.cache.reap(now)
			for _, conn := range b.connections {
				if !conn.connected() {
					continue
				}
				lastSeen := time.Unix(0, conn.lastSeen.Load())
				if now.Sub(lastSeen) > 15*time.Second {
					conn.disconnect()
					continue
				}
				if conn.gateway {
					go func(c *connection) {
						ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
						defer cancel()
						result, err := b.requestOn(ctx, c, "*", "instance.list", map[string]any{})
						if err != nil {
							slog.Error("failed to list relay instances", "error", err)
							return
						}
						response, ok := result.(map[string]any)
						if !ok {
							return
						}
						encoded, _ := json.Marshal(response["instances"])
						b.introduce(c, decodeInstances(encoded))
					}(conn)
				}
			}
		}
	}
}

// Close ends WebSocket connections and pending requests.
func (b *Bridge) Close() {
	b.closeOnce.Do(func() {
		b.cancel()
		for _, conn := range b.connections {
			conn.close()
		}
		b.mu.Lock()
		b.pending = make(map[string]pendingRequest)
		b.mu.Unlock()
	})
}

func newID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(err)
	}
	encoded := hex.EncodeToString(bytes[:])
	return strings.Join([]string{encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]}, "-")
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func dataURL(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	parsed.Path = "/data"
	parsed.RawPath = ""
	parsed.RawQuery = ""
	return parsed.String(), nil
}
