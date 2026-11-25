package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
	"remote-mcp/internal/protocol"
)

func TestMCPSessionTools(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := newSessionToolTestServer(t, ctx, func(request wireMessage) protocol.Message {
		return protocol.Message{Result: request}
	})
	for _, tc := range []struct {
		name, tool, arguments, params string
	}{
		{"create", "session_create", `{"instance_id":"remote","command":"demo","args":["--flag","two words"],"cwd":"work"}`, `{"args":["--flag","two words"],"command":"demo","cwd":"work"}`},
		{"write", "session_write", `{"instance_id":"remote","session_id":"s","input":"hello\n"}`, `{"input":"hello\n","session_id":"s"}`},
		{"read defaults", "session_read", `{"instance_id":"remote","session_id":"s"}`, `{"max_bytes":65536,"offset":0,"session_id":"s","strip_ansi":false,"wait_ms":0}`},
		{"read caps", "session_read", `{"instance_id":"remote","session_id":"s","offset":4,"max_bytes":65537,"wait_ms":30001,"strip_ansi":true}`, `{"max_bytes":65536,"offset":4,"session_id":"s","strip_ansi":true,"wait_ms":30000}`},
		{"read options", "session_read", `{"instance_id":"remote","session_id":"s","max_bytes":256,"wait_ms":500}`, `{"max_bytes":256,"offset":0,"session_id":"s","strip_ansi":false,"wait_ms":500}`},
		{"close", "session_close", `{"instance_id":"remote","session_id":"s"}`, `{"session_id":"s"}`},
		{"list", "session_list", `{"instance_id":"remote"}`, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, text := invokeSessionTestTool(t, ctx, server, tc.tool, tc.arguments)
			if result.IsError {
				t.Fatalf("%s: %s", tc.tool, text)
			}
			var sent wireMessage
			if err := json.Unmarshal([]byte(text), &sent); err != nil {
				t.Fatal(err)
			}
			wantMethod := strings.Replace(tc.tool, "_", ".", 1)
			if sent.Instance != "remote" || sent.Method != wantMethod {
				t.Fatalf("got %s on %s, want %s on remote", sent.Method, sent.Instance, wantMethod)
			}
			var params bytes.Buffer
			if err := json.Compact(&params, sent.Params); err != nil {
				t.Fatal(err)
			}
			if params.String() != tc.params {
				t.Fatalf("params = %s, want %s", &params, tc.params)
			}
		})
	}
	for tool, arguments := range map[string]string{
		"session_create": `{"instance_id":"remote","command":"demo","args":[1]}`,
		"session_write":  `{"instance_id":"remote","session_id":"s"}`,
		"session_read":   `{"instance_id":"remote","session_id":"s","strip_ansi":1}`,
		"session_close":  `{"instance_id":"remote"}`,
		"session_list":   `{"instance_id":"remote","extra":true}`,
	} {
		t.Run(tool+" invalid", func(t *testing.T) {
			result, text := invokeSessionTestTool(t, ctx, server, tool, arguments)
			if !result.IsError || !strings.Contains(text, "input schema validation failed") {
				t.Fatalf("expected schema rejection, got %s", text)
			}
		})
	}
}

func TestMCPSessionErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := newSessionToolTestServer(t, ctx, func(request wireMessage) protocol.Message {
		switch request.Method {
		case "session.close":
			return protocol.Message{Error: &protocol.Error{Code: "SESSION_NOT_FOUND", Message: "closed"}}
		case "session.write":
			return protocol.Message{Error: &protocol.Error{Code: "SESSION_WRITE_TIMEOUT", Message: "timeout", Data: map[string]any{"bytes_written": 2}}}
		default:
			return protocol.Message{Error: &protocol.Error{Code: "METHOD_NOT_FOUND", Message: "unsupported"}}
		}
	})
	for _, tc := range []struct {
		tool, arguments, want string
	}{
		{"session_list", `{"instance_id":"remote"}`, "this instance does not support interactive sessions; upgrade the server"},
		{"session_close", `{"instance_id":"remote","session_id":"s"}`, "SESSION_NOT_FOUND: closed"},
		{"exec_run", `{"instance_id":"remote","command":"demo"}`, "METHOD_NOT_FOUND: unsupported"},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			result, text := invokeSessionTestTool(t, ctx, server, tc.tool, tc.arguments)
			if !result.IsError || text != tc.want {
				t.Fatalf("got %q, IsError=%v; want error %q", text, result.IsError, tc.want)
			}
		})
	}
	t.Run("partial write", func(t *testing.T) {
		result, text := invokeSessionTestTool(t, ctx, server, "session_write", `{"instance_id":"remote","session_id":"s","input":"hello"}`)
		var failure protocol.Error
		if err := json.Unmarshal([]byte(text), &failure); err != nil {
			t.Fatalf("decode partial-write error %q: %v", text, err)
		}
		if !result.IsError || failure.Code != "SESSION_WRITE_TIMEOUT" || failure.Data["bytes_written"] != float64(2) {
			t.Fatalf("expected partial-write error with 2 bytes written, got %s", text)
		}
	})
}

func TestSessionRequestBudget(t *testing.T) {
	for _, tc := range []struct {
		method, params string
		want           time.Duration
	}{
		{"session.read", `{}`, 30 * time.Second},
		{"session.read", `{"wait_ms":2000}`, 30 * time.Second},
		{"session.read", `{"wait_ms":15000}`, 30 * time.Second},
		{"session.read", `{"wait_ms":15001}`, 30*time.Second + time.Millisecond},
		{"session.read", `{"wait_ms":30000}`, 45 * time.Second},
		{"session.list", `{}`, 30 * time.Second},
		{"exec.run", `{}`, 45 * time.Second},
		{"exec.run", `{"timeout_ms":1000}`, 16 * time.Second},
	} {
		if got := requestBudget(tc.method, json.RawMessage(tc.params)); got != tc.want {
			t.Errorf("requestBudget(%s, %s) = %s, want %s", tc.method, tc.params, got, tc.want)
		}
	}
}

func newSessionToolTestServer(t *testing.T, ctx context.Context, respond func(wireMessage) protocol.Message) *mcpserver.MCPServer {
	t.Helper()
	upgrader := websocket.Upgrader{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		if err = socket.WriteJSON(wireMessage{Type: "hello", Instance: "remote"}); err != nil {
			return
		}
		for {
			var request wireMessage
			if err = socket.ReadJSON(&request); err != nil {
				return
			}
			reply := respond(request)
			reply.Type, reply.ClientID, reply.RequestID = "response", request.ClientID, request.RequestID
			if err = socket.WriteJSON(reply); err != nil {
				return
			}
		}
	}))
	t.Cleanup(remote.Close)
	return NewMCPServer(startTestBridge(t, ctx, "ws"+strings.TrimPrefix(remote.URL, "http")))
}

func invokeSessionTestTool(t *testing.T, ctx context.Context, server *mcpserver.MCPServer, name, arguments string) (*mcp.CallToolResult, string) {
	t.Helper()
	message, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": mcp.CallToolParams{Name: name, Arguments: json.RawMessage(arguments)},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := server.HandleMessage(ctx, message)
	value, ok := response.(mcp.JSONRPCResponse)
	if !ok {
		t.Fatalf("unexpected response: %#v", response)
	}
	result, ok := value.Result.(*mcp.CallToolResult)
	if !ok || len(result.Content) != 1 {
		t.Fatalf("unexpected result: %#v", value.Result)
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content: %#v", result.Content)
	}
	return result, text.Text
}
