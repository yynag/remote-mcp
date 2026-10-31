package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
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

// Bridge 管理直连和中继连接、实例状态以及请求响应。
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

// NewBridge 创建 WebSocket 客户端桥接。
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

// Start 并行尝试所有入口；至少一条连接成功后继续后台重连。
func (b *Bridge) Start(ctx context.Context) error {
	if len(b.connections) == 0 {
		return errors.New("没有可连接的远端地址")
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
		return errors.New("没有远端入口可用")
	}
	for _, conn := range b.connections {
		go conn.reconnect()
	}
	go b.checkLoop()
	return nil
}

func (c *connection) connect(ctx context.Context) error {
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, response, err := dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		if response != nil {
			return fmt.Errorf("连接 %s：HTTP %s", c.url, response.Status)
		}
		return fmt.Errorf("连接 %s：%w", c.url, err)
	}
	conn.SetReadLimit(8 * 1024 * 1024)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hello wireMessage
	if err = conn.ReadJSON(&hello); err != nil {
		_ = conn.Close()
		return fmt.Errorf("读取入口介绍 %s：%w", c.url, err)
	}
	if (c.gateway && hello.Type != "instances") || (!c.gateway && hello.Type != "hello") {
		_ = conn.Close()
		return fmt.Errorf("入口 %s 返回了无效的介绍消息 %q", c.url, hello.Type)
	}
	_ = conn.SetReadDeadline(time.Time{})
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		_ = conn.Close()
		return errors.New("连接已关闭")
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
				log.Printf("远端连接失败：%v", err)
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
			log.Printf("远端连接断开：地址=%s，原因=%v", c.url, err)
			return
		}
		// 应用层心跳同样证明连接存活，必须刷新读取期限，不能只依赖 pong。
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
		return errors.New("INSTANCE_OFFLINE: WebSocket 未连接")
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
		b.introduce(conn, []protocol.Hello{{Type: "hello", InstanceID: message.Instance, Guide: message.Guide}})
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

// List 返回发现到的实例和当前在线状态。
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

// Request 向指定实例发送一条控制协议请求。
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
		return nil, fmt.Errorf("发送请求失败：%w", err)
	}
	budget := 30 * time.Second
	if method == "exec.run" {
		var command struct {
			TimeoutMS int64 `json:"timeout_ms"`
		}
		_ = json.Unmarshal(encoded, &command)
		if command.TimeoutMS == 0 {
			command.TimeoutMS = 30_000
		}
		budget = time.Duration(command.TimeoutMS)*time.Millisecond + 15*time.Second
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	select {
	case response := <-result:
		if response.err != nil {
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
		return nil, fmt.Errorf("请求 %s：%w", method, ctx.Err())
	case <-b.ctx.Done():
		return nil, errors.New("CLIENT_CLOSED")
	}
}

// ConnectionFor 返回实例当前控制连接的 WebSocket 地址。
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
							log.Printf("获取中继实例列表失败：%v", err)
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

// Close 结束 WebSocket 连接和未完成的请求。
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
	parsed.RawQuery = ""
	return parsed.String(), nil
}
