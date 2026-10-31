package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"remote-mcp/internal/protocol"
)

const (
	defaultChunkSize   = protocol.DefaultChunkSize
	defaultConcurrency = protocol.DefaultConcurrency
	dataFrameSize      = protocol.MaxDataFrame
	transferRetries    = 3
)

type transferArgs struct {
	InstanceID string
	LocalPath  string
	RemotePath string
	TransferID string
	Overwrite  bool
}

type transferStatus struct {
	TransferID      string    `json:"transfer_id"`
	State           string    `json:"state"`
	CompletedChunks []int     `json:"completed_chunks"`
	ExpiresAt       time.Time `json:"expires_at"`
	Size            int64     `json:"size"`
	SHA256          string    `json:"sha256"`
	ChunkSHA256     []string  `json:"chunk_sha256"`
	ChunkSize       int64     `json:"chunk_size"`
	Concurrency     int       `json:"concurrency"`
	MTime           string    `json:"mtime"`
	Direction       string    `json:"direction"`
}

type transferSession struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	sockets map[*websocket.Conn]struct{}
}

func (b *Bridge) transfer(ctx context.Context, args transferArgs, direction string) (any, error) {
	if args.InstanceID == "" || args.LocalPath == "" || args.RemotePath == "" {
		return nil, errors.New("instance_id、local_path 和 remote_path 不能为空")
	}
	if direction == "download" && !args.Overwrite {
		if _, err := os.Lstat(args.LocalPath); err == nil {
			return nil, errors.New("TARGET_EXISTS: 本地目标文件已存在")
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	session := &transferSession{cancel: cancel, sockets: make(map[*websocket.Conn]struct{})}
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			session.closeSockets()
		case <-finished:
		}
	}()
	defer close(finished)
	defer cancel()
	preparedID := ""
	committed := false
	partialPath := ""
	defer func() {
		if preparedID != "" && !committed {
			_ = b.cancelRemoteTransfer(args.InstanceID, preparedID)
		}
		b.transferMu.Lock()
		delete(b.transfers, preparedID)
		b.transferMu.Unlock()
		if !committed && partialPath != "" {
			_ = os.Remove(partialPath)
		}
	}()

	var sourceInfo os.FileInfo
	var sourceFile *os.File
	var sourceHash string
	var chunks []string
	var err error
	if direction == "upload" {
		sourceInfo, err = os.Stat(args.LocalPath)
		if err != nil {
			return nil, err
		}
		if !sourceInfo.Mode().IsRegular() {
			return nil, errors.New("上传源必须是普通文件")
		}
		sourceHash, chunks, err = hashManifest(args.LocalPath, sourceInfo.Size())
		if err != nil {
			return nil, err
		}
		current, statErr := os.Stat(args.LocalPath)
		if statErr != nil || !os.SameFile(sourceInfo, current) || sourceInfo.Size() != current.Size() || !sourceInfo.ModTime().Equal(current.ModTime()) {
			return nil, errors.New("SOURCE_CHANGED: 上传源文件在计算校验值时发生变化")
		}
	}
	prepare := protocol.TransferSpec{
		TransferID: args.TransferID, Path: args.RemotePath, Direction: direction,
		Size: sizeOf(sourceInfo), SHA256: sourceHash, ChunkSHA256: chunks,
		ChunkSize: defaultChunkSize, Concurrency: defaultConcurrency, Overwrite: args.Overwrite,
	}
	if sourceInfo != nil {
		prepare.MTime = fmt.Sprintf("%d", sourceInfo.ModTime().UnixNano())
	}
	preparedValue, err := b.Request(ctx, args.InstanceID, "transfer.prepare", prepare)
	if err != nil {
		return nil, err
	}
	prepared, err := decodeStatus(preparedValue)
	if err != nil {
		return nil, err
	}
	preparedID = prepared.TransferID
	b.transferMu.Lock()
	b.transfers[preparedID] = session
	b.transferMu.Unlock()
	statusValue, err := b.Request(ctx, args.InstanceID, "transfer.status", map[string]any{"transfer_id": prepared.TransferID})
	if err != nil {
		return nil, err
	}
	status, err := decodeStatus(statusValue)
	if err != nil {
		return nil, err
	}
	if err = validateStatus(status); err != nil {
		return nil, err
	}
	completed := make(map[int]bool, len(status.CompletedChunks))
	for _, index := range status.CompletedChunks {
		completed[index] = true
	}

	if direction == "upload" {
		sourceFile, err = os.Open(args.LocalPath)
	} else {
		partial := args.LocalPath + "." + prepared.TransferID + ".part"
		partialPath = partial
		if args.TransferID == "" {
			sourceFile, err = os.OpenFile(partial, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
			if err == nil {
				err = sourceFile.Truncate(status.Size)
			}
		} else {
			sourceFile, err = os.OpenFile(partial, os.O_RDWR, 0600)
			if errors.Is(err, syscall.ENOENT) {
				sourceFile, err = os.OpenFile(partial, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
				if err == nil {
					err = sourceFile.Truncate(status.Size)
					completed = make(map[int]bool)
				}
			}
		}
		if err != nil {
			return nil, err
		}
		info, statErr := sourceFile.Stat()
		if statErr != nil || info.Size() != status.Size {
			return nil, errors.New("断点文件大小与远端传输状态不一致")
		}
		for index := range completed {
			if !verifyChunk(sourceFile, status, index) {
				delete(completed, index)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	defer sourceFile.Close()

	indices := make([]int, 0, len(status.ChunkSHA256))
	for index := range status.ChunkSHA256 {
		if !completed[index] {
			indices = append(indices, index)
		}
	}
	if err = runTransferWorkers(ctx, indices, defaultConcurrency, func(index int) error {
		for attempt := 0; attempt <= transferRetries; attempt++ {
			err := b.transferChunk(ctx, args.InstanceID, prepared, direction, index, sourceFile, session)
			if err == nil {
				return nil
			}
			if ctx.Err() != nil || attempt == transferRetries {
				return err
			}
			if !waitContext(ctx, time.Duration(200*(1<<attempt))*time.Millisecond) {
				return ctx.Err()
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if direction == "upload" {
		if err = verifyFile(args.LocalPath, status.SHA256, status.Size); err != nil {
			return nil, err
		}
	} else {
		if err = verifyOpenFile(sourceFile, status.SHA256, status.Size); err != nil {
			return nil, err
		}
		if err = sourceFile.Sync(); err != nil {
			return nil, err
		}
		if err = sourceFile.Close(); err != nil {
			return nil, err
		}
		sourceFile = nil
		partial := args.LocalPath + "." + prepared.TransferID + ".part"
		if args.Overwrite {
			err = os.Rename(partial, args.LocalPath)
		} else {
			err = os.Link(partial, args.LocalPath)
			if err == nil {
				err = os.Remove(partial)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	result, err := b.Request(ctx, args.InstanceID, "transfer.commit", map[string]any{"transfer_id": prepared.TransferID})
	if err != nil {
		statusValue, statusErr := b.Request(ctx, args.InstanceID, "transfer.status", map[string]any{"transfer_id": prepared.TransferID})
		if statusErr == nil {
			status, decodeErr := decodeStatus(statusValue)
			if decodeErr == nil && status.State == "completed" {
				return statusValue, nil
			}
		}
		return nil, err
	}
	committed = true
	return result, nil
}

func (b *Bridge) cancelTransfer(transferID string) {
	b.transferMu.Lock()
	session := b.transfers[transferID]
	b.transferMu.Unlock()
	if session == nil {
		return
	}
	session.cancel()
	session.closeSockets()
}

func (s *transferSession) closeSockets() {
	s.mu.Lock()
	for socket := range s.sockets {
		_ = socket.Close()
	}
	s.mu.Unlock()
}

func (b *Bridge) cancelRemoteTransfer(instanceID, transferID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := b.Request(ctx, instanceID, "transfer.cancel", map[string]any{"transfer_id": transferID})
	return err
}

func (b *Bridge) transferChunk(ctx context.Context, instanceID string, status transferStatus, direction string, index int, file *os.File, session *transferSession) error {
	connection, err := b.connectionFor(instanceID)
	if err != nil {
		return err
	}
	endpoint, err := dataURL(connection.url)
	if err != nil {
		return err
	}
	offset := int64(index) * defaultChunkSize
	length := minInt64(defaultChunkSize, status.Size-offset)
	hash := status.ChunkSHA256[index]
	pairID := newID()
	handshake := protocol.DataHandshake{Type: "data", Role: "client", PairID: pairID, InstanceID: instanceID, ClientID: b.clientID, TransferID: status.TransferID, Direction: direction, ChunkIndex: index, Offset: offset, Length: length, SHA256: hash}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.sockets[ws] = struct{}{}
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		delete(session.sockets, ws)
		session.mu.Unlock()
		_ = ws.Close()
	}()
	_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err = ws.WriteJSON(handshake); err != nil {
		return err
	}
	var ready struct {
		Type  string `json:"type"`
		Error string `json:"error"`
	}
	if err = ws.ReadJSON(&ready); err != nil {
		return err
	}
	if ready.Type != "data.ready" {
		if ready.Error != "" {
			return errors.New(ready.Error)
		}
		return errors.New("数据连接未就绪")
	}
	if direction == "upload" {
		buffer := make([]byte, dataFrameSize)
		for position := int64(0); position < length; {
			count := minInt64(int64(len(buffer)), length-position)
			read, readErr := file.ReadAt(buffer[:count], offset+position)
			if readErr != nil && readErr != io.EOF {
				return readErr
			}
			if read != int(count) {
				return io.ErrUnexpectedEOF
			}
			_ = ws.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if err = ws.WriteMessage(websocket.BinaryMessage, buffer[:read]); err != nil {
				return err
			}
			position += int64(read)
		}
		_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
	} else {
		chunkHash := sha256.New()
		position := int64(0)
		for position < length {
			_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
			kind, data, readErr := ws.ReadMessage()
			if readErr != nil {
				return readErr
			}
			if kind != websocket.BinaryMessage || len(data) == 0 || len(data) > dataFrameSize || int64(len(data)) > length-position {
				return errors.New("无效的数据分块")
			}
			written, writeErr := file.WriteAt(data, offset+position)
			if writeErr != nil {
				return writeErr
			}
			if written != len(data) {
				return io.ErrShortWrite
			}
			_, _ = chunkHash.Write(data)
			position += int64(len(data))
		}
		if hex.EncodeToString(chunkHash.Sum(nil)) != hash {
			return errors.New("SHA256_MISMATCH: 分块校验失败")
		}
		if err = ws.WriteJSON(map[string]any{"type": "data.ack", "sha256": hash}); err != nil {
			return err
		}
	}
	var done struct {
		Type   string `json:"type"`
		SHA256 string `json:"sha256"`
		Error  string `json:"error"`
	}
	if err = ws.ReadJSON(&done); err != nil {
		return err
	}
	if done.Type != "data.done" || done.SHA256 != hash {
		if done.Error != "" {
			return errors.New(done.Error)
		}
		return errors.New("数据分块未完成")
	}
	return nil
}

func hashManifest(path string, size int64) (string, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	whole := sha256.New()
	chunks := make([]string, 0, (size+defaultChunkSize-1)/defaultChunkSize)
	buffer := make([]byte, dataFrameSize)
	for offset := int64(0); offset < size; offset += defaultChunkSize {
		length := minInt64(defaultChunkSize, size-offset)
		chunk := sha256.New()
		reader := io.NewSectionReader(file, offset, length)
		if _, err = io.CopyBuffer(io.MultiWriter(whole, chunk), reader, buffer); err != nil {
			return "", nil, err
		}
		chunks = append(chunks, hex.EncodeToString(chunk.Sum(nil)))
	}
	return hex.EncodeToString(whole.Sum(nil)), chunks, nil
}

func verifyFile(path, expected string, size int64) error {
	actual, _, err := hashManifest(path, size)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("SHA256_MISMATCH: 文件在传输期间发生变化")
	}
	return nil
}

func verifyChunk(file *os.File, status transferStatus, index int) bool {
	offset := int64(index) * defaultChunkSize
	length := minInt64(defaultChunkSize, status.Size-offset)
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, io.NewSectionReader(file, offset, length), make([]byte, dataFrameSize)); err != nil {
		return false
	}
	return hex.EncodeToString(hash.Sum(nil)) == status.ChunkSHA256[index]
}

func verifyOpenFile(file *os.File, expected string, size int64) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, io.LimitReader(file, size), make([]byte, dataFrameSize)); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != expected {
		return errors.New("SHA256_MISMATCH: 文件校验失败")
	}
	return nil
}

func decodeStatus(value any) (transferStatus, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return transferStatus{}, err
	}
	var status transferStatus
	if err = json.Unmarshal(encoded, &status); err != nil {
		return status, err
	}
	return status, nil
}

func validateStatus(status transferStatus) error {
	count := int((status.Size + defaultChunkSize - 1) / defaultChunkSize)
	if status.State != "active" || status.Size < 0 || status.ChunkSize != defaultChunkSize || status.Concurrency != defaultConcurrency || len(status.ChunkSHA256) != count {
		return errors.New("INVALID_TRANSFER_STATUS")
	}
	for _, index := range status.CompletedChunks {
		if index < 0 || index >= count {
			return errors.New("INVALID_TRANSFER_STATUS")
		}
	}
	return nil
}

func runTransferWorkers(ctx context.Context, indices []int, workers int, fn func(int) error) error {
	if len(indices) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make(chan error, workers)
	var group sync.WaitGroup
	for i := 0; i < minInt(len(indices), workers); i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				select {
				case <-ctx.Done():
					results <- ctx.Err()
					return
				case index, ok := <-jobs:
					if !ok {
						results <- nil
						return
					}
					if err := fn(index); err != nil {
						cancel()
						results <- err
						return
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, index := range indices {
			select {
			case jobs <- index:
			case <-ctx.Done():
				return
			}
		}
	}()
	group.Wait()
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return ctx.Err()
}

func sizeOf(info os.FileInfo) int64 {
	if info == nil {
		return 0
	}
	return info.Size()
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
