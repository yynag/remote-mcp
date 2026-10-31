package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

type toolArguments struct {
	InstanceID   string `json:"instance_id"`
	Command      string `json:"command"`
	Cwd          string `json:"cwd"`
	TimeoutMS    int64  `json:"timeout_ms"`
	JobID        string `json:"job_id"`
	Offset       int64  `json:"offset"`
	StdoutOffset *int64 `json:"stdout_offset"`
	StderrOffset *int64 `json:"stderr_offset"`
	Path         string `json:"path"`
	SHA256       bool   `json:"sha256"`
	LocalPath    string `json:"local_path"`
	RemotePath   string `json:"remote_path"`
	TransferID   string `json:"transfer_id"`
	Overwrite    bool   `json:"overwrite"`
	ChunkSize    int64  `json:"chunk_size"`
	Concurrency  int    `json:"concurrency"`
}

type toolDefinition struct {
	name        string
	description string
	schema      string
}

var toolDefinitions = []toolDefinition{
	{"instance_list", "List discovered instances and their online status.", `{"type":"object","properties":{},"additionalProperties":false}`},
	{"instance_get", "Get the usage guide provided by the selected remote instance. Read it before running commands to confirm the target platform, shell, and trusted paths.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1}},"required":["instance_id"],"additionalProperties":false}`},
	{"exec_run", "Run a command on the remote instance. The command is parsed as-is by the remote platform shell (/bin/sh on Unix, powershell.exe on Windows), not translated between shells. Call instance_get first to confirm the target shell. Use correct quoting for short commands. For complex multiline scripts, prefer writing a local script, confirming a trusted remote destination, uploading it with file_upload, then explicitly invoking an installed interpreter. Do not concatenate unvalidated arguments into a shell command string. Default timeout is 30 seconds.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"command":{"type":"string","minLength":1},"cwd":{"type":"string"},"timeout_ms":{"type":"integer","minimum":0,"maximum":31536000000}},"required":["instance_id","command"],"additionalProperties":false}`},
	{"job_start", "Start a background command and return its job ID. The command is parsed as-is by the remote platform shell (/bin/sh on Unix, powershell.exe on Windows), not translated between shells. Call instance_get first to confirm the target shell. Use correct quoting for short commands. For complex multiline scripts, prefer writing a local script, confirming a trusted remote destination, uploading it with file_upload, then explicitly invoking an installed interpreter. Do not concatenate unvalidated arguments into a shell command string. Default timeout is 30 seconds.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"command":{"type":"string","minLength":1},"cwd":{"type":"string"},"timeout_ms":{"type":"integer","minimum":0,"maximum":31536000000}},"required":["instance_id","command"],"additionalProperties":false}`},
	{"job_status", "Get the status of a job.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"job_id":{"type":"string","minLength":1}},"required":["instance_id","job_id"],"additionalProperties":false}`},
	{"job_output", "Read stdout and stderr from the specified byte offsets. If only offset is provided, it is used for both streams; stream-specific offsets override it.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"job_id":{"type":"string","minLength":1},"offset":{"type":"integer","minimum":0},"stdout_offset":{"type":"integer","minimum":0},"stderr_offset":{"type":"integer","minimum":0}},"required":["instance_id","job_id"],"additionalProperties":false}`},
	{"job_cancel", "Request graceful job termination. The server escalates to forced termination if the job does not exit.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"job_id":{"type":"string","minLength":1}},"required":["instance_id","job_id"],"additionalProperties":false}`},
	{"job_kill", "Forcefully terminate the job process.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"job_id":{"type":"string","minLength":1}},"required":["instance_id","job_id"],"additionalProperties":false}`},
	{"file_stat", "Get remote file metadata, optionally including its SHA-256 hash.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"path":{"type":"string","minLength":1},"sha256":{"type":"boolean"}},"required":["instance_id","path"],"additionalProperties":false}`},
	{"file_list", "List the contents of a remote directory.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"path":{"type":"string","minLength":1}},"required":["instance_id","path"],"additionalProperties":false}`},
	{"file_upload", "Upload a file in 5 MiB chunks with up to four concurrent transfers. Confirm the remote destination is trusted before uploading.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"local_path":{"type":"string","minLength":1},"remote_path":{"type":"string","minLength":1},"transfer_id":{"type":"string"},"overwrite":{"type":"boolean"},"chunk_size":{"type":"integer","const":5242880,"default":5242880},"concurrency":{"type":"integer","const":4,"default":4}},"required":["instance_id","local_path","remote_path"],"additionalProperties":false}`},
	{"file_download", "Download a file in 5 MiB chunks with up to four concurrent transfers.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"local_path":{"type":"string","minLength":1},"remote_path":{"type":"string","minLength":1},"transfer_id":{"type":"string"},"overwrite":{"type":"boolean"},"chunk_size":{"type":"integer","const":5242880,"default":5242880},"concurrency":{"type":"integer","const":4,"default":4}},"required":["instance_id","local_path","remote_path"],"additionalProperties":false}`},
	{"transfer_status", "Get file transfer status and completed chunks.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"transfer_id":{"type":"string","minLength":1}},"required":["instance_id","transfer_id"],"additionalProperties":false}`},
	{"transfer_cancel", "Cancel a file transfer and clean up uncommitted temporary data.", `{"type":"object","properties":{"instance_id":{"type":"string","minLength":1},"transfer_id":{"type":"string","minLength":1}},"required":["instance_id","transfer_id"],"additionalProperties":false}`},
}

// NewMCPServer 注册项目提供的完整 MCP 工具集合。
func NewMCPServer(bridge *Bridge) *mcpserver.MCPServer {
	server := mcpserver.NewMCPServer("remote-mcp", "0.2.0",
		mcpserver.WithRecovery(),
		mcpserver.WithInputSchemaValidation(),
	)
	for _, definition := range toolDefinitions {
		tool := mcp.NewToolWithRawSchema(definition.name, definition.description, json.RawMessage(definition.schema))
		name := definition.name
		server.AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := runTool(ctx, bridge, name, request.GetArguments())
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			encoded, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("编码工具结果失败：%v", err)), nil
			}
			return mcp.NewToolResultText(string(encoded)), nil
		})
	}
	return server
}

func runTool(ctx context.Context, bridge *Bridge, name string, input map[string]any) (any, error) {
	args := toolArguments{}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(encoded, &args); err != nil {
		return nil, fmt.Errorf("参数格式无效：%w", err)
	}
	if name == "instance_list" {
		return bridge.List(), nil
	}
	if name == "file_upload" || name == "file_download" {
		if args.ChunkSize != 0 && args.ChunkSize != defaultChunkSize || args.Concurrency != 0 && args.Concurrency != defaultConcurrency {
			return nil, fmt.Errorf("文件传输固定使用 5 MiB 分块和 4 路并行")
		}
		transferArgs := transferArgs{
			InstanceID: args.InstanceID, LocalPath: args.LocalPath, RemotePath: args.RemotePath,
			TransferID: args.TransferID, Overwrite: args.Overwrite,
		}
		if name == "file_upload" {
			return bridge.transfer(ctx, transferArgs, "upload")
		}
		return bridge.transfer(ctx, transferArgs, "download")
	}
	if name == "job_output" {
		stdoutOffset, stderrOffset := args.Offset, args.Offset
		if args.StdoutOffset != nil {
			stdoutOffset = *args.StdoutOffset
		}
		if args.StderrOffset != nil {
			stderrOffset = *args.StderrOffset
		}
		if cached, ok := bridge.cache.read(args.InstanceID, args.JobID, stdoutOffset, stderrOffset); ok {
			return cached, nil
		}
		return bridge.Request(ctx, args.InstanceID, "job.output", map[string]any{"job_id": args.JobID, "stdout_offset": stdoutOffset, "stderr_offset": stderrOffset})
	}
	if name == "transfer_cancel" {
		bridge.cancelTransfer(args.TransferID)
		return bridge.Request(ctx, args.InstanceID, "transfer.cancel", map[string]any{"transfer_id": args.TransferID})
	}
	method, params, err := requestForTool(name, args)
	if err != nil {
		return nil, err
	}
	return bridge.Request(ctx, args.InstanceID, method, params)
}

func requestForTool(name string, args toolArguments) (string, map[string]any, error) {
	switch name {
	case "instance_get":
		return "instance.get", map[string]any{}, nil
	case "exec_run":
		return "exec.run", commandParams(args), nil
	case "job_start":
		return "job.start", commandParams(args), nil
	case "job_status", "job_cancel", "job_kill":
		return "job." + map[string]string{"job_status": "status", "job_cancel": "cancel", "job_kill": "kill"}[name], map[string]any{"job_id": args.JobID}, nil
	case "file_stat":
		return "file.stat", map[string]any{"path": args.Path, "sha256": args.SHA256}, nil
	case "file_list":
		return "file.list", map[string]any{"path": args.Path}, nil
	case "transfer_status":
		return "transfer.status", map[string]any{"transfer_id": args.TransferID}, nil
	default:
		return "", nil, fmt.Errorf("未知工具：%s", name)
	}
}

func commandParams(args toolArguments) map[string]any {
	params := map[string]any{"command": args.Command}
	if args.Cwd != "" {
		params["cwd"] = args.Cwd
	}
	if args.TimeoutMS != 0 {
		params["timeout_ms"] = args.TimeoutMS
	}
	return params
}
