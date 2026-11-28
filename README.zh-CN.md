# Remote MCP

[English](README.md) | 中文

Remote MCP 使用同一套 WebSocket 协议连接 Go server、transport 和 Go MCP client。client 通过标准输入输出提供 MCP Server，支持实例指南、命令执行、异步任务和文件传输。

## 目录

- `server`：远端命令和文件代理应用，入口为 `server/main.go`。
- `transport`：WebSocket 中继应用，入口为 `transport/main.go`，仅需 Linux amd64。
- `client`：stdio MCP Server 应用，入口为 `client/main.go`。
- `internal/protocol`：client、server 和 transport 共享的协议模块。

## 安装指南

需要 Go 1.25.5 或更新版本。

构建 server、transport 和 client：

```bash
mkdir -p build
# windows amd64
# darwin amd64/arm64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o build/remote-mcp-server.exe ./server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o build/remote-mcp-transport ./transport
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o build/remote-mcp-client ./client
```

server 配置文件与二进制放在同一目录，`guide.md` 也放在二进制同目录。配置文件使用 YAML：

```yaml
instance_id: "gpu-01"
guide: "guide.md"
listen: ":9000"
gateway: ""
```

启动：

```bash
./build/remote-mcp-transport --listen :9098
./build/remote-mcp-server --config server.yaml
./build/remote-mcp-client --gateway 127.0.0.1:9098
```

client 也可以直连：

```bash
./build/remote-mcp-client --remotes 127.0.0.1:9000
```

### IPv6 与 DNS 地址

client 的 `--gateway`、`--remotes` 和 server 的 `gateway` 支持 IPv4、IPv6 和 ASCII DNS 主机名，地址均须包含端口；IPv6 必须使用方括号：

```bash
./build/remote-mcp-client --remotes '[2001:db8::10]:9000'
./build/remote-mcp-client --remotes remote.example.com:9000
./build/remote-mcp-client --gateway relay.example.com:9098
```

以上 IPv6 为文档示例地址，部署时请替换为实际地址。server 配置示例：

```yaml
instance_id: "gpu-01"
guide: "guide.md"
listen: ":9000"
gateway: "relay.example.com:9098"
```

- DNS 在拨号时由系统解析，保留 Go 的 TCP 双栈快速回退能力；不保证 WebSocket 握手失败后自动尝试另一地址。中文域名请使用 punycode；不支持带 zone 的 IPv6（如 `%en0`）。
- server 的 `listen` 允许空 host（`:9000`）；`[::]:9000` 和空 host 的实际双栈监听效果依赖系统，不保证跨平台一致；主机名监听不代表绑定所有解析地址。
- 控制和文件数据连接均显式直连，不使用 `HTTP_PROXY`、`HTTPS_PROXY` 等环境代理。**兼容变化：文件数据连接不再隐式使用环境代理。**
- 控制与 `/data` 独立拨号，DNS 多个 A/AAAA 地址须指向同一个有状态服务进程；当前不支持通过 DNS 轮询分流到多个独立 server/transport 副本。
- 当前直连需手动指定地址，没有自动发现、打洞或直连失败后回到中继的路由切换能力。IPv6 可路由也不代表防火墙允许入站。
- 本次 IPv6/DNS 改造以单元测试、源码审查和 Windows 交叉编译验收；真实 IPv6 连通性、Windows 双栈监听、系统 DNS 与实际回退时延尚未实测。

MCP Host 的 stdio 配置：

```json
{
  "mcpServers": {
    "remote-mcp": {
      "command": "/绝对路径/build/remote-mcp-client",
      "args": ["--gateway", "127.0.0.1:9098"]
    }
  }
}
```

## 工具列表

除 `instance_list` 外，所有工具都需要 `instance_id`。返回结果均为 JSON。

| 分组 | 工具 | 用途 |
| --- | --- | --- |
| 实例 | `instance_list` | 列出实例 ID 和在线状态，不包含指南内容。 |
| 实例 | `instance_get` | 按需读取实例使用指南；调用远程命令前先确认目标系统、shell 和工作目录。 |
| 命令 | `exec_run` | 前台执行单条命令，返回 stdout、stderr、状态和退出码；默认超时 30 秒。 |
| 作业 | `job_start` | 启动后台命令并返回 `job_id`。 |
| 作业 | `job_status` | 查询作业状态、退出码、开始时间和耗时。 |
| 作业 | `job_output` | 按字节偏移增量读取 stdout 和 stderr，续读使用 `stdout_next_offset` 和 `stderr_next_offset`。 |
| 作业 | `job_cancel` | 请求优雅终止；作业未退出时由 server 升级为强制终止。 |
| 作业 | `job_kill` | 强制终止作业进程。 |
| 会话 | `session_create` | 在 120×40 PTY 中启动 `command`，可选 `args` 和 `cwd`，返回 `session_id`。 |
| 会话 | `session_write` | 向会话写入 `input`；提交命令时 Unix 追加 `\n`，Windows 追加 `\r`。 |
| 会话 | `session_read` | 按字节偏移读取 PTY 输出，可选 `wait_ms` 和 `strip_ansi`。 |
| 会话 | `session_close` | 关闭会话并尽力清理其进程。 |
| 会话 | `session_list` | 列出实例上运行中及仍在保留期内的会话。 |
| 文件 | `file_stat` | 获取远端文件元数据，可选计算 SHA-256。 |
| 文件 | `file_list` | 列出远端目录内容。 |
| 文件 | `file_upload` | 将本地文件上传到远端路径。 |
| 文件 | `file_download` | 将远端文件下载到本地路径。 |
| 传输 | `transfer_status` | 查询传输状态和已完成分块。 |
| 传输 | `transfer_cancel` | 取消传输并清理未提交的临时数据。 |

`instance_list` 只返回实例 ID 和在线状态，不包含指南正文；AI 需要使用某实例时再调用 `instance_get` 读取指南，避免无关指南占用上下文。作业与会话的细节见后续小节。

## 远程命令与脚本执行

使用 `exec_run` 或 `job_start` 前，先调用 `instance_get` 阅读实例指南，确认目标平台、shell、可用解释器和可信远端路径。命令会原样交由目标平台的 shell 解析：Unix 使用 `/bin/sh`，Windows 使用 `powershell.exe`；系统不会转换命令语法，也不保证跨 shell 兼容。

短命令应使用已确认 shell 对应的引号规则。对于复杂或多行脚本，建议先在本地编写并检查，再确认远端可信目标路径，通过 `file_upload` 上传，然后明确调用远端已安装的解释器（例如适用时使用 `python` 或 `pwsh`）。上传脚本不会自动选择或调用解释器。不要将未经校验的参数拼接进 shell 命令字符串。以上做法可减少不必要的转义和注入错误，但不能消除 shell 转义风险。

### guide.md 模板

将以下内容保存为 server 二进制同目录的 `guide.md`，再按实际环境修改：

```markdown
# 远端服务器使用指南

请告诉 AI 如何使用这台远端服务器：

- 操作系统和默认 shell，例如 Linux、Windows、PowerShell、WSL 或 macOS。
- 可用工具、版本和调用方式，例如 Python、Docker、Bun、Go、CUDA。
- 工作目录、项目目录、数据目录、临时目录和结果目录。
- 必须使用的容器、虚拟环境、路径或命令。
- 网络、代理、镜像、GPU、磁盘和挂载目录等特殊情况。
- 常见命令示例，以及命令失败时应检查什么。
- 删除、覆盖文件、停止服务或大量占用资源前，如何向用户说明。
- 任务完成后的验证方式、日志位置和产物位置。

工作目录是使用建议，不是访问限制。工具不会阻止访问其他路径。
```

## 交互式会话

执行单条命令优先使用 `exec_run`。需要在多条命令之间保留当前目录、变量等进程状态，或持续写入 stdin 时，使用交互式会话。所有会话工具都需要 `instance_id`：

| 工具 | 用法 |
| --- | --- |
| `session_create` | 在远端 120×40 伪终端（PTY）中启动 `command`，可选 `args`（参数数组）和 `cwd`，返回 `session_id`。 |
| `session_write` | 向 `session_id` 写入 `input`，返回 `bytes_written`。提交命令时 Unix 追加 `\n`，Windows 追加 `\r`。 |
| `session_read` | 从 `session_id` 的字节偏移 `offset`（默认 `0`）读取输出。续读时把上次的 `next_offset` 作为新的 `offset`；`max_bytes` 默认且最多为 64 KiB。 |
| `session_close` | 按 `session_id` 关闭会话并尽力清理进程；`cleanup_incomplete=true` 表示清理不完整。 |
| `session_list` | 列出实例上运行中及仍在保留期内的会话，并返回状态。 |

`wait_ms` 默认 `0`（不等待），需要等待新输出时建议 `500`～`2000` 毫秒，最长 `30000` 毫秒；有新输出、发生截断或会话不再运行时会提前返回。若请求偏移早于缓存起点，响应以 `truncated=true` 和 `base_offset` 标明。`strip_ansi` 默认 `false`；开启后按响应分块过滤，跨块的 ANSI 转义序列可能残留，`next_offset` 仍按原始字节推进。

会话空闲 30 分钟后由巡检回收；只有实际读到或写入字节才刷新空闲时间。会话结束后保留 10 分钟供续读。每实例最多 8 个会话，启动中、运行中、关闭中和保留的已结束会话都占名额；满额时先淘汰最早结束的会话，若仍满则拒绝创建。会话属于实例，不按 client 隔离：同一实例的所有 client 均可查看、读取、写入和关闭会话。

Windows 使用 ConPTY，最低支持 Windows 10 1809 / Windows Server 2019。已在 Windows 11 + PowerShell 7 上验证创建、读写、自然退出码、输出积压时关闭、配额与后台子进程回收；关闭会话时通过 Job Object 终止进程树。PowerShell `Start-Process` 默认经 shell 启动的进程可能不在该 Job 中，需要时使用 `-NoNewWindow` 或自行管理。其他 Windows 版本未实测。Unix 关闭时会尽力清理同一终端会话（SID）中的进程，但不保证清理主动调用 `setsid` 脱离的进程。

## 构建和测试

```bash
go test -race ./...
go vet ./...
```

相对 `guide` 路径从 server 二进制目录读取；`gateway` 非空时 server 反向连接 transport，但仍启动本地监听。client 的 `--gateway` 和 `--remotes` 互斥，地址格式为 `host:port`（IPv6 为 `[地址]:port`）。

文件传输使用 5 MiB 分块、4 路并行、断点续传和 SHA-256 校验。当前不提供认证、授权或路径沙箱，请部署在可信网络或安全隧道内，不要直接向公网开放远程命令与文件端口。公网部署需另行落实 TLS、身份认证与授权、防火墙限制；IPv6 支持不改变这些安全要求。
