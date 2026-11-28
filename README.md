# Remote MCP

![Go](https://img.shields.io/badge/Go-1.25.5-blue)
![License](https://img.shields.io/badge/License-MIT-green)

English | [&#20013;&#25991;](README.zh-CN.md)

Remote MCP connects a Go server, a transport, and a Go MCP client over a single WebSocket protocol. The client exposes an MCP server over stdio and supports instance guides, command execution, asynchronous jobs, and file transfer.

## Layout

- `server`: remote command and file proxy application; entry point at `server/main.go`.
- `transport`: WebSocket relay application; entry point at `transport/main.go`; only a Linux amd64 build is required.
- `client`: stdio MCP server application; entry point at `client/main.go`.
- `internal/protocol`: shared protocol module for the client, server, and transport.

## Installation

Go 1.25.5 or newer is required.

Prebuilt binaries are available from [GitHub Releases](https://github.com/yynag/remote-mcp/releases). Release artifacts follow the naming pattern `remote-mcp-{server|transport|client}_{version}_{os}_{arch}`, where `{version}` is the release version and `{os}` and `{arch}` identify the target platform. You can also build from source.

Build the server, transport, and client:

```bash
mkdir -p build
# Windows amd64
# Darwin amd64/arm64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o build/remote-mcp-server.exe ./server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o build/remote-mcp-transport ./transport
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o build/remote-mcp-client ./client
```

Place the server configuration file in the same directory as the binary, and place `guide.md` in that directory as well. The configuration file uses YAML:

```yaml
instance_id: "gpu-01"
guide: "guide.md"
listen: ":9000"
gateway: ""
```

Start the components:

```bash
./build/remote-mcp-transport --listen :9098
./build/remote-mcp-server --config server.yaml
./build/remote-mcp-client --gateway 127.0.0.1:9098
```

The client can also connect directly:

```bash
./build/remote-mcp-client --remotes 127.0.0.1:9000
```

### IPv6 and DNS Addresses

The client's `--gateway` and `--remotes` options, and the server's `gateway` setting, support IPv4, IPv6, and ASCII DNS hostnames. Every address must include a port, and IPv6 addresses must be enclosed in square brackets:

```bash
./build/remote-mcp-client --remotes '[2001:db8::10]:9000'
./build/remote-mcp-client --remotes remote.example.com:9000
./build/remote-mcp-client --gateway relay.example.com:9098
```

The IPv6 addresses above are documentation examples; replace them with actual addresses when deploying. Example server configuration:

```yaml
instance_id: "gpu-01"
guide: "guide.md"
listen: ":9000"
gateway: "relay.example.com:9098"
```

- DNS is resolved by the system at dial time, retaining Go's TCP dual-stack fast fallback behavior; automatic fallback to another address after a WebSocket handshake failure is not guaranteed. Use punycode for non-ASCII domain names; IPv6 addresses with a zone identifier (such as `%en0`) are not supported.
- The server's `listen` accepts an empty host (`:9000`); the actual dual-stack listening behavior of `[::]:9000` and an empty host depends on the system and is not guaranteed to be consistent across platforms. Listening on a hostname does not mean binding every resolved address.
- Control and file data connections both connect directly and explicitly, and do not use environment proxies such as `HTTP_PROXY` or `HTTPS_PROXY`. **Compatibility change: file data connections no longer implicitly use environment proxies.**
- Control and `/data` connections dial independently. Multiple A/AAAA addresses for a DNS name must point to the same stateful service process; distributing traffic across multiple independent server/transport replicas via DNS round-robin is not currently supported.
- Direct connections currently require manually specified addresses. There is no auto-discovery, NAT traversal, or route switching back to the relay after a direct connection fails. A routable IPv6 address does not mean that the firewall permits inbound traffic.
- This IPv6/DNS change was validated through unit tests, source review, and Windows cross-compilation. Real IPv6 connectivity, Windows dual-stack listening, system DNS behavior, and actual fallback latency have not yet been tested.

MCP Host stdio configuration:

```json
{
  "mcpServers": {
    "remote-mcp": {
      "command": "/absolute/path/build/remote-mcp-client",
      "args": ["--gateway", "127.0.0.1:9098"]
    }
  }
}
```

## Tools

All tools require `instance_id` except `instance_list`. Results are JSON.

| Group | Tool | Purpose |
| --- | --- | --- |
| Instance | `instance_list` | List instance IDs with online status. Guide content is not included. |
| Instance | `instance_get` | Read an instance's usage guide on demand; call it before remote commands to confirm the target OS, shell, and workspace. |
| Command | `exec_run` | Run one command in the foreground and return stdout, stderr, state, and exit code; default timeout 30 seconds. |
| Job | `job_start` | Start a background command and return a `job_id`. |
| Job | `job_status` | Get job state, exit code, start time, and duration. |
| Job | `job_output` | Read stdout and stderr from byte offsets; continue from `stdout_next_offset` and `stderr_next_offset`. |
| Job | `job_cancel` | Request graceful termination; the server escalates to forced termination if the job does not exit. |
| Job | `job_kill` | Forcefully terminate the job process. |
| Session | `session_create` | Start `command` in a 120×40 PTY; optional `args` and `cwd`; returns `session_id`. |
| Session | `session_write` | Write `input` to a session; Unix appends `\n` and Windows appends `\r`. |
| Session | `session_read` | Read PTY output by byte offset, with optional `wait_ms` and `strip_ansi`. |
| Session | `session_close` | Close a session and make a best-effort cleanup of its processes. |
| Session | `session_list` | List running and retained sessions on the instance. |
| File | `file_stat` | Get remote file metadata, optionally including SHA-256. |
| File | `file_list` | List a remote directory. |
| File | `file_upload` | Upload a local file to a remote path. |
| File | `file_download` | Download a remote file to a local path. |
| Transfer | `transfer_status` | Get transfer state and completed chunks. |
| Transfer | `transfer_cancel` | Cancel a transfer and clean up uncommitted temporary data. |

`instance_list` returns only instance IDs and online status, not guide content. When the AI needs to use an instance, it calls `instance_get` to read the guide, preventing unrelated guides from consuming context. Jobs and sessions are further described in the sections below.

## Remote Command and Script Execution

Before using `exec_run` or `job_start`, call `instance_get` to read the instance guide and confirm the target platform, shell, available interpreters, and trusted remote paths. Commands are passed as-is to the target platform's shell: `/bin/sh` on Unix and `powershell.exe` on Windows. The system does not translate command syntax and does not guarantee cross-shell compatibility.

For short commands, use the quoting rules of the confirmed shell. For complex or multi-line scripts, write and check the script locally first, confirm a trusted destination path on the remote host, upload it with `file_upload`, and then explicitly invoke an interpreter installed on the remote host (for example, `python` or `pwsh` when applicable). Uploading a script does not automatically select or invoke an interpreter. Do not concatenate unvalidated arguments into shell command strings. These practices reduce unnecessary escaping and injection errors but do not eliminate shell escaping risk.

### guide.md Template

Save the following content as `guide.md` in the same directory as the server binary, then adapt it to the actual environment:

```markdown
# Remote Server Guide

Tell the AI how to use this remote server:

- Operating system and default shell, for example Linux, Windows, PowerShell, WSL, or macOS.
- Available tools, versions, and invocation methods, for example Python, Docker, Bun, Go, CUDA.
- Working, project, data, temporary, and result directories.
- Required containers, virtual environments, paths, or commands.
- Special considerations such as networking, proxies, mirrors, GPUs, disks, and mounted directories.
- Common command examples and what to check when a command fails.
- How to explain to the user before deleting or overwriting files, stopping services, or consuming significant resources.
- How to verify task completion, and where logs and artifacts are located.

The working directory is a usage recommendation, not an access restriction. The tools do not block access to other paths.
```

## Interactive Sessions

Prefer `exec_run` for a single command. Use an interactive session when process state such as the current directory or variables must persist across multiple commands, or when you need to keep writing to stdin. All session tools require `instance_id`:

| Tool | Usage |
| --- | --- |
| `session_create` | Start `command` in a 120×40 pseudo-terminal (PTY) on the remote host. Optional `args` (argument array) and `cwd`; returns `session_id`. |
| `session_write` | Write `input` to `session_id`; returns `bytes_written`. When submitting a command, Unix appends `\n` and Windows appends `\r`. |
| `session_read` | Read output from `session_id` at byte offset `offset` (default `0`). For continuation reads, pass the previous `next_offset` as the new `offset`. `max_bytes` defaults to and is capped at 64 KiB. |
| `session_close` | Close the session identified by `session_id` and make a best-effort cleanup of processes; `cleanup_incomplete=true` indicates incomplete cleanup. |
| `session_list` | List running sessions and sessions still within the retention period on the instance, with their status. |

`wait_ms` defaults to `0` (no waiting). When waiting for new output, `500`–`2000` milliseconds is recommended, with a maximum of `30000` milliseconds. The call returns early when new output arrives, truncation occurs, or the session is no longer running. If the requested offset is earlier than the start of the buffer, the response indicates this with `truncated=true` and `base_offset`. `strip_ansi` defaults to `false`; when enabled, filtering is applied per response chunk, so ANSI escape sequences that span chunks may remain, and `next_offset` still advances by raw bytes.

Sessions are reclaimed by a sweeper after 30 minutes of inactivity; idle time is refreshed only when bytes are actually read or written. Finished sessions are retained for 10 minutes for continuation reads. Each instance supports at most 8 sessions; sessions that are starting, running, closing, and finished-but-retained all count toward the limit. When the limit is reached, the earliest finished session is evicted first; if the instance is still full, creation is rejected. Sessions belong to the instance and are not isolated per client: all clients connected to the same instance can list, read, write, and close its sessions.

Windows uses ConPTY and requires at least Windows 10 1809 / Windows Server 2019. Creation, read/write, natural exit codes, closing with output backlog, quotas, and background child-process reclamation have been verified on Windows 11 + PowerShell 7; closing a session terminates the process tree via a Job Object. By default, processes launched through the shell by PowerShell `Start-Process` may not be in that Job; use `-NoNewWindow` when needed or manage them yourself. Other Windows versions have not been tested. On Unix, closing a session makes a best-effort attempt to clean up processes in the same terminal session (SID), but processes that detach themselves by calling `setsid` are not guaranteed to be cleaned up.

## Build and Test

```bash
go test -race ./...
go vet ./...
```

Relative `guide` paths are read from the server binary directory. When `gateway` is non-empty, the server reverse-connects to the transport but still starts its local listener. The client's `--gateway` and `--remotes` are mutually exclusive; addresses use the `host:port` format (`[address]:port` for IPv6).

File transfer uses 5 MiB chunks, 4-way parallelism, resumable transfers, and SHA-256 verification. There is currently no authentication, authorization, or path sandbox, so deploy on a trusted network or inside a secure tunnel and do not expose the remote command and file ports directly to the public internet. Public deployments must separately implement TLS, authentication and authorization, and firewall restrictions; IPv6 support does not change these security requirements.
