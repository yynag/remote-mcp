// Package protocol 定义 Remote MCP 的控制通道和文件传输消息。
package protocol

import (
	"encoding/json"
	"fmt"
)

const (
	// Version 是当前控制协议版本。
	Version = 1
	// DefaultChunkSize 是文件传输固定使用的分块大小。
	DefaultChunkSize = 5 * 1024 * 1024
	// DefaultConcurrency 是文件传输固定使用的并行数。
	DefaultConcurrency = 4
	// TransferRetentionMinutes 是中断传输的保留时间。
	TransferRetentionMinutes = 30
	// MaxDataFrame 是单个数据 WebSocket 帧的最大大小。
	MaxDataFrame = 256 * 1024
)

// Hello 是 server 主动发送的实例介绍。
type Hello struct {
	Type       string `json:"type"`
	InstanceID string `json:"instance_id"`
	Guide      string `json:"guide"`
}

// Message 是控制 WebSocket 上传输的 JSON 消息。
type Message struct {
	Version    int             `json:"version,omitempty"`
	Type       string          `json:"type"`
	ClientID   string          `json:"client_id,omitempty"`
	RequestID  string          `json:"request_id,omitempty"`
	InstanceID string          `json:"instance_id,omitempty"`
	Method     string          `json:"method,omitempty"`
	Params     json.RawMessage `json:"params,omitempty"`
	Result     any             `json:"result,omitempty"`
	Error      *Error          `json:"error,omitempty"`
	Event      string          `json:"event,omitempty"`
	Data       any             `json:"data,omitempty"`
}

// Error 是控制协议响应中的远端错误。
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error 返回可读的错误码和错误信息。
func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Failure 根据请求生成失败响应。
func Failure(req Message, code string, err error) Message {
	return Message{Version: Version, Type: "response", ClientID: req.ClientID, RequestID: req.RequestID, InstanceID: req.InstanceID, Error: &Error{Code: code, Message: err.Error()}}
}

// Success 根据请求生成成功响应。
func Success(req Message, result any) Message {
	return Message{Version: Version, Type: "response", ClientID: req.ClientID, RequestID: req.RequestID, InstanceID: req.InstanceID, Result: result}
}

// ValidateRequest 检查请求是否包含协议要求的字段。
func (m Message) ValidateRequest() error {
	if m.Type != "request" || m.Version != Version || m.ClientID == "" || m.RequestID == "" || m.InstanceID == "" || m.Method == "" {
		return fmt.Errorf("request requires version=1, client_id, request_id, instance_id and method")
	}
	return nil
}

// DataHandshake 是文件分块数据 WebSocket 的握手消息。
type DataHandshake struct {
	Type       string `json:"type"`
	InstanceID string `json:"instance_id"`
	ClientID   string `json:"client_id"`
	Direction  string `json:"direction"`
	TransferID string `json:"transfer_id"`
	ChunkIndex int    `json:"chunk_index"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	SHA256     string `json:"sha256"`
	Role       string `json:"role,omitempty"`
	PairID     string `json:"pair_id,omitempty"`
}
