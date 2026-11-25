// Package protocol defines control channel and file transfer messages for Remote MCP.
package protocol

import (
	"encoding/json"
	"fmt"
)

const (
	// Version is the current control protocol version.
	Version = 1
	// DefaultChunkSize is the fixed chunk size used for file transfers.
	DefaultChunkSize = 5 * 1024 * 1024
	// DefaultConcurrency is the fixed concurrency used for file transfers.
	DefaultConcurrency = 4
	// TransferRetentionMinutes is the retention period for interrupted transfers.
	TransferRetentionMinutes = 30
	// MaxDataFrame is the maximum size of a single data WebSocket frame.
	MaxDataFrame = 256 * 1024
)

// Hello is the instance identity sent by the server. The guide body is fetched on demand through instance.get.
type Hello struct {
	Type       string `json:"type"`
	InstanceID string `json:"instance_id"`
	Guide      string `json:"guide,omitempty"`
}

// Message is a JSON message transported over the control WebSocket.
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

// Error is a remote error in a control protocol response.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data,omitempty"`
}

// Error returns the readable error code and message.
func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Failure builds a failure response from a request.
func Failure(req Message, code string, err error) Message {
	return Message{Version: Version, Type: "response", ClientID: req.ClientID, RequestID: req.RequestID, InstanceID: req.InstanceID, Error: &Error{Code: code, Message: err.Error()}}
}

// Success builds a success response from a request.
func Success(req Message, result any) Message {
	return Message{Version: Version, Type: "response", ClientID: req.ClientID, RequestID: req.RequestID, InstanceID: req.InstanceID, Result: result}
}

// ValidateRequest checks that a request contains the fields required by the protocol.
func (m Message) ValidateRequest() error {
	if m.Type != "request" || m.Version != Version || m.ClientID == "" || m.RequestID == "" || m.InstanceID == "" || m.Method == "" {
		return fmt.Errorf("request requires version=1, client_id, request_id, instance_id, and method")
	}
	return nil
}

// DataHandshake is the handshake message for a file chunk data WebSocket.
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
