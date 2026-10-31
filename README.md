# Remote MCP

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

## 构建和测试

```bash
go test -race ./...
go vet ./...
```

相对 `guide` 路径从 server 二进制目录读取；`gateway` 非空时 server 反向连接 transport。client 的 `--gateway` 和 `--remotes` 互斥，地址格式为 IP:PORT。

文件传输使用 5 MiB 分块、4 路并行、断点续传和 SHA-256 校验。第一版不提供认证、授权或路径沙箱，请部署在可信网络中。
