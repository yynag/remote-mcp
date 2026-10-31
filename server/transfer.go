package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
)

func (e *Engine) reverseData(url string, h protocol.DataHandshake) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return
	}
	h.Role = "server"
	if err = dataJSON(c, h); err != nil {
		_ = c.Close()
		return
	}
	e.serveData(c, h)
}

type transfer struct {
	mu      sync.Mutex
	spec    protocol.TransferSpec
	client  string
	tmp     string
	file    *os.File
	source  os.FileInfo
	done    map[int]bool
	active  map[int]*websocket.Conn
	state   string
	expires time.Time
}
type transferStore struct {
	mu sync.Mutex
	m  map[string]*transfer
}

func newTransferStore() *transferStore         { return &transferStore{m: make(map[string]*transfer)} }
func transferError(code, message string) error { return &protocol.Error{Code: code, Message: message} }
func (t *transfer) touch() {
	t.expires = time.Now().Add(protocol.TransferRetentionMinutes * time.Minute)
}
func (t *transfer) cleanup(state string) {
	t.state = state
	for _, c := range t.active {
		_ = c.Close()
	}
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
	}
	if t.tmp != "" {
		_ = os.Remove(t.tmp)
		t.tmp = ""
	}
}
func (s *transferStore) reap(all bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.m {
		t.mu.Lock()
		if all || time.Now().After(t.expires) {
			t.cleanup("expired")
			delete(s.m, id)
		}
		t.mu.Unlock()
	}
}
func (s *transferStore) get(id, client string) (*transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.m[id]
	if t == nil || t.client != client {
		return nil, transferError("TRANSFER_NOT_FOUND", "unknown transfer for client")
	}
	t.mu.Lock()
	if time.Now().After(t.expires) {
		t.cleanup("expired")
		delete(s.m, id)
		t.mu.Unlock()
		return nil, transferError("TRANSFER_EXPIRED", "transfer retention expired")
	}
	t.mu.Unlock()
	return t, nil
}
func hashManifest(f *os.File, size int64) (string, []string, error) {
	whole := sha256.New()
	chunks := make([]string, 0)
	buf := make([]byte, protocol.MaxDataFrame)
	for off := int64(0); off < size; off += protocol.DefaultChunkSize {
		n := min(int64(protocol.DefaultChunkSize), size-off)
		h := sha256.New()
		copied, err := io.CopyBuffer(io.MultiWriter(whole, h), io.NewSectionReader(f, off, n), buf)
		if err != nil {
			return "", nil, err
		}
		if copied != n {
			return "", nil, io.ErrUnexpectedEOF
		}
		chunks = append(chunks, hex.EncodeToString(h.Sum(nil)))
	}
	return hex.EncodeToString(whole.Sum(nil)), chunks, nil
}
func validSHA(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}
func (t *transfer) sourceUnchanged() error {
	info, err := os.Stat(t.spec.Path)
	if err != nil {
		return err
	}
	if !os.SameFile(t.source, info) || info.Size() != t.source.Size() || !info.ModTime().Equal(t.source.ModTime()) {
		return transferError("SOURCE_CHANGED", "source size/mtime changed during transfer")
	}
	return nil
}
func (s *transferStore) prepare(p protocol.TransferSpec, client string) (*transfer, error) {
	if p.Path == "" || (p.Direction != "upload" && p.Direction != "download") {
		return nil, transferError("INVALID_REQUEST", "path and direction upload/download required")
	}
	if (p.ChunkSize != 0 && p.ChunkSize != protocol.DefaultChunkSize) || (p.Concurrency != 0 && p.Concurrency != protocol.DefaultConcurrency) {
		return nil, transferError("INVALID_REQUEST", "transfers use 5 MiB chunks and 4 workers")
	}
	p.ChunkSize = protocol.DefaultChunkSize
	p.Concurrency = protocol.DefaultConcurrency
	path, err := filepath.Abs(p.Path)
	if err != nil {
		return nil, err
	}
	p.Path = filepath.Clean(path)
	// 解析已经存在的父目录符号链接，避免通过别名绕过目标文件锁。
	if parent, e := filepath.EvalSymlinks(filepath.Dir(p.Path)); e == nil {
		p.Path = filepath.Join(parent, filepath.Base(p.Path))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.TransferID != "" {
		t := s.m[p.TransferID]
		if t == nil {
			return nil, transferError("TRANSFER_NOT_FOUND", "unknown transfer for resume")
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if time.Now().After(t.expires) {
			t.cleanup("expired")
			delete(s.m, p.TransferID)
			return nil, transferError("TRANSFER_EXPIRED", "transfer retention expired")
		}
		if t.state != "active" || t.spec.Path != p.Path || t.spec.Direction != p.Direction || (p.Direction == "upload" && (t.spec.Size != p.Size || t.spec.SHA256 != p.SHA256 || t.spec.MTime != p.MTime || t.spec.Overwrite != p.Overwrite || !slices.Equal(t.spec.ChunkSHA256, p.ChunkSHA256))) {
			return nil, transferError("TRANSFER_MISMATCH", "resume does not match original source/target")
		}
		if len(t.active) != 0 {
			return nil, transferError("TRANSFER_BUSY", "close existing data channels before resuming")
		}
		if p.Direction == "download" {
			if err = t.sourceUnchanged(); err != nil {
				return nil, err
			}
		}
		// 客户端重启后可以使用传输 ID 继续传输。这只是路由，不是认证；第一版只运行在可信网络中。
		t.client = client
		t.touch()
		return t, nil
	}
	for _, t := range s.m {
		t.mu.Lock()
		conflict := t.state == "active" && t.spec.Path == p.Path
		t.mu.Unlock()
		if conflict {
			return nil, transferError("TRANSFER_CONFLICT", "path already has an active transfer")
		}
	}
	t := &transfer{spec: p, client: client, done: make(map[int]bool), active: make(map[int]*websocket.Conn), state: "active"}
	t.touch()
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		return nil, err
	}
	t.spec.TransferID = hex.EncodeToString(id)
	if p.Direction == "upload" {
		count := p.Size / protocol.DefaultChunkSize
		if p.Size%protocol.DefaultChunkSize != 0 {
			count++
		}
		if p.Size < 0 || !validSHA(p.SHA256) || int64(len(p.ChunkSHA256)) != count {
			return nil, transferError("INVALID_REQUEST", "invalid size or SHA256 manifest")
		}
		for _, h := range p.ChunkSHA256 {
			if !validSHA(h) {
				return nil, transferError("INVALID_REQUEST", "invalid chunk SHA256")
			}
		}
		if !p.Overwrite {
			if _, e := os.Lstat(p.Path); e == nil {
				return nil, transferError("TARGET_EXISTS", "target exists; overwrite defaults to false")
			} else if !os.IsNotExist(e) {
				return nil, e
			}
		}
		t.file, err = os.CreateTemp(filepath.Dir(p.Path), ".remote-mcp-*.part")
		if err != nil {
			return nil, err
		}
		t.tmp = t.file.Name()
		if err = t.file.Truncate(p.Size); err != nil {
			t.cleanup("failed")
			return nil, err
		}
	} else {
		t.file, err = os.Open(p.Path)
		if err != nil {
			return nil, err
		}
		t.source, err = t.file.Stat()
		if err != nil {
			t.cleanup("failed")
			return nil, err
		}
		if !t.source.Mode().IsRegular() {
			t.cleanup("failed")
			return nil, errors.New("source must be a regular file")
		}
		t.spec.Size = t.source.Size()
		t.spec.MTime = strconv.FormatInt(t.source.ModTime().UnixNano(), 10)
		t.spec.SHA256, t.spec.ChunkSHA256, err = hashManifest(t.file, t.spec.Size)
		if err == nil {
			err = t.sourceUnchanged()
		}
		if err != nil {
			t.cleanup("failed")
			return nil, err
		}
	}
	s.m[t.spec.TransferID] = t
	return t, nil
}
func (t *transfer) status() map[string]any {
	chunks := make([]int, 0, len(t.done))
	for i := range t.done {
		chunks = append(chunks, i)
	}
	sort.Ints(chunks)
	return map[string]any{"transfer_id": t.spec.TransferID, "state": t.state, "completed_chunks": chunks, "expires_at": t.expires, "size": t.spec.Size, "sha256": t.spec.SHA256, "chunk_sha256": t.spec.ChunkSHA256, "chunk_size": t.spec.ChunkSize, "concurrency": t.spec.Concurrency, "mtime": t.spec.MTime, "direction": t.spec.Direction}
}
func (e *Engine) transferRequest(req protocol.Message) (any, error) {
	var p protocol.TransferSpec
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return nil, err
	}
	if req.Method == "transfer.prepare" || req.Method == "file.upload" || req.Method == "file.download" {
		if req.Method == "file.upload" {
			p.Direction = "upload"
		}
		if req.Method == "file.download" {
			p.Direction = "download"
		}
		t, err := e.transfers.prepare(p, req.ClientID)
		if err != nil {
			return nil, err
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		log.Printf("[文件传输已准备] 实例=%s，client=%s，传输=%s，方向=%s，路径=%q，大小=%d，分块数=%d，并发=%d，已完成块=%d", e.id, req.ClientID, t.spec.TransferID, t.spec.Direction, t.spec.Path, t.spec.Size, len(t.spec.ChunkSHA256), t.spec.Concurrency, len(t.done))
		return t.status(), nil
	}
	t, err := e.transfers.get(p.TransferID, req.ClientID)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.touch()
	switch req.Method {
	case "transfer.cancel":
		if t.state != "completed" {
			t.cleanup("cancelled")
		}
		log.Printf("[文件传输已取消] 实例=%s，client=%s，传输=%s，已完成块=%d/%d", e.id, req.ClientID, t.spec.TransferID, len(t.done), len(t.spec.ChunkSHA256))
		return t.status(), nil
	case "transfer.commit":
		if t.state == "completed" {
			return t.status(), nil
		}
		if t.state != "active" || len(t.active) != 0 || len(t.done) != len(t.spec.ChunkSHA256) {
			return nil, transferError("TRANSFER_INCOMPLETE", "not all chunks have been verified")
		}
		if t.spec.Direction == "download" {
			if err = t.sourceUnchanged(); err != nil {
				return nil, err
			}
		} else {
			whole, _, er := hashManifest(t.file, t.spec.Size)
			if er != nil {
				return nil, er
			}
			if whole != t.spec.SHA256 {
				return nil, transferError("SHA256_MISMATCH", "whole file SHA256 mismatch")
			}
			if err = t.file.Sync(); err != nil {
				return nil, err
			}
			if err = t.file.Close(); err != nil {
				return nil, err
			}
			t.file = nil
			if t.spec.Overwrite {
				err = os.Rename(t.tmp, t.spec.Path)
			} else { // 硬链接创建操作是原子的，不会覆盖并发创建的目标文件。
				err = os.Link(t.tmp, t.spec.Path)
				if err == nil {
					_ = os.Remove(t.tmp)
				}
			}
			if err != nil {
				t.file, _ = os.OpenFile(t.tmp, os.O_RDWR, 0600)
				return nil, err
			}
			t.tmp = ""
		}
		if t.file != nil {
			_ = t.file.Close()
			t.file = nil
		}
		t.state = "completed"
		log.Printf("[文件传输已提交] 实例=%s，client=%s，传输=%s，方向=%s，路径=%q，大小=%d，SHA256=%s", e.id, req.ClientID, t.spec.TransferID, t.spec.Direction, t.spec.Path, t.spec.Size, t.spec.SHA256)
	case "transfer.status":
		if t.state == "active" && t.spec.Direction == "download" {
			if err = t.sourceUnchanged(); err != nil {
				return nil, err
			}
		}
	}
	return t.status(), nil
}

// ServeData 处理独立文件分块数据 WebSocket。
func (e *Engine) ServeData(w http.ResponseWriter, r *http.Request) {
	c, err := protocol.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(protocol.MaxDataFrame)
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	var h protocol.DataHandshake
	if err = c.ReadJSON(&h); err != nil {
		log.Printf("[文件数据握手失败] 实例=%s，远端=%s，原因=%v", e.id, r.RemoteAddr, err)
		_ = c.Close()
		return
	}
	log.Printf("[文件数据连接] 实例=%s，client=%s，传输=%s，分块=%d，方向=%s，远端=%s", e.id, h.ClientID, h.TransferID, h.ChunkIndex, h.Direction, r.RemoteAddr)
	e.serveData(c, h)
}
func dataJSON(c *websocket.Conn, v any) error {
	_ = c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return c.WriteJSON(v)
}
func (e *Engine) serveData(c *websocket.Conn, h protocol.DataHandshake) {
	defer c.Close()
	c.SetReadLimit(protocol.MaxDataFrame)
	err := e.dataChunk(c, h)
	if err != nil {
		log.Printf("[文件分块失败] 实例=%s，client=%s，传输=%s，分块=%d，方向=%s，原因=%v", e.id, h.ClientID, h.TransferID, h.ChunkIndex, h.Direction, err)
		_ = dataJSON(c, map[string]any{"type": "data.error", "error": err.Error()})
	}
}
func (e *Engine) dataChunk(c *websocket.Conn, h protocol.DataHandshake) error {
	if h.Type != "data" || h.InstanceID != e.id {
		return errors.New("invalid data identity")
	}
	t, err := e.transfers.get(h.TransferID, h.ClientID)
	if err != nil {
		return err
	}
	t.mu.Lock()
	if t.state != "active" || h.ChunkIndex < 0 || h.ChunkIndex >= len(t.spec.ChunkSHA256) || h.Direction != t.spec.Direction {
		t.mu.Unlock()
		return errors.New("invalid transfer/chunk")
	}
	off := int64(h.ChunkIndex) * protocol.DefaultChunkSize
	length := min(int64(protocol.DefaultChunkSize), t.spec.Size-off)
	if h.Offset != off || h.Length != length || h.SHA256 != t.spec.ChunkSHA256[h.ChunkIndex] || t.active[h.ChunkIndex] != nil || len(t.active) >= protocol.DefaultConcurrency {
		t.mu.Unlock()
		return errors.New("invalid/busy chunk range or SHA256")
	}
	if t.spec.Direction == "download" {
		if err = t.sourceUnchanged(); err != nil {
			t.mu.Unlock()
			return err
		}
	}
	f := t.file
	t.active[h.ChunkIndex] = c
	delete(t.done, h.ChunkIndex)
	t.touch()
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		if t.active[h.ChunkIndex] == c {
			delete(t.active, h.ChunkIndex)
		}
		t.touch()
		t.mu.Unlock()
	}()
	if err = dataJSON(c, map[string]any{"type": "data.ready"}); err != nil {
		return err
	}
	started := time.Now()
	hash := sha256.New()
	buf := make([]byte, protocol.MaxDataFrame)
	for pos := int64(0); pos < length; {
		if h.Direction == "upload" {
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
			typ, b, er := c.ReadMessage()
			if er != nil {
				return er
			}
			if typ != websocket.BinaryMessage || len(b) == 0 || int64(len(b)) > length-pos {
				return errors.New("invalid data frame")
			}
			if _, er = f.WriteAt(b, off+pos); er != nil {
				return er
			}
			_, _ = hash.Write(b)
			pos += int64(len(b))
		} else {
			n := min(int64(len(buf)), length-pos)
			if _, err = f.ReadAt(buf[:n], off+pos); err != nil {
				return err
			}
			_, _ = hash.Write(buf[:n])
			_ = c.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if err = c.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
				return err
			}
			pos += n
		}
		t.mu.Lock()
		t.touch()
		t.mu.Unlock()
	}
	if hex.EncodeToString(hash.Sum(nil)) != h.SHA256 {
		return transferError("SHA256_MISMATCH", "chunk SHA256 mismatch")
	}
	if h.Direction == "download" {
		_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
		var ack struct {
			Type   string `json:"type"`
			SHA256 string `json:"sha256"`
		}
		if err = c.ReadJSON(&ack); err != nil {
			return err
		}
		if ack.Type != "data.ack" || ack.SHA256 != h.SHA256 {
			return errors.New("invalid chunk acknowledgement")
		}
	}
	t.mu.Lock()
	if t.state != "active" {
		t.mu.Unlock()
		return errors.New("transfer cancelled")
	}
	if h.Direction == "download" {
		if err = t.sourceUnchanged(); err != nil {
			t.mu.Unlock()
			return err
		}
	}
	t.done[h.ChunkIndex] = true
	// 先删除活动记录再确认，确保随后可以立即提交。
	delete(t.active, h.ChunkIndex)
	t.mu.Unlock()
	log.Printf("[文件分块完成] 实例=%s，client=%s，传输=%s，分块=%d，方向=%s，字节数=%d，耗时=%s", e.id, h.ClientID, h.TransferID, h.ChunkIndex, h.Direction, length, time.Since(started).Round(time.Millisecond))
	return dataJSON(c, map[string]any{"type": "data.done", "sha256": h.SHA256})
}
