# 项目认识地图

Remote MCP 是 Go 实现的远程命令、异步任务及分块文件传输服务，采用 WebSocket 协议。

- `client/`：stdio MCP 入口，支持直接指定远端或经 gateway 中继，二者互斥。
- `server/`：远端执行与文件服务；配置入口 `config.go`，反向连接 gateway 时仍监听入站。
- `transport/`：有状态 WebSocket 中继，注册和数据配对状态位于单进程内。
- `internal/protocol/`：共享消息与协议。
- `internal/endpoint/`：IPv4、IPv6 和 ASCII DNS 地址校验，区分监听和拨号地址。
- `internal/wsdial/`：控制/数据统一显式直连策略，不读取环境代理。
- `README.md`：构建、配置、安全及 IPv6/DNS 使用说明。
- `.agent/文档/`：实施方案与验收记录；`.agent/tmp/`：本地验证产物。

## 当前验证边界

已完成 IPv6/DNS 阶段一地址与拨号改造，IPv4 兼容；未实现自动 P2P、认证、授权或 TLS。远端 Windows 只有 IPv4，本阶段以单元测试、独立代码审查与交叉编译验收，真实 IPv6 可达性和 Windows 双栈行为未实测。

验收测试：`go test -race -timeout 120s -skip '^TestRealMCPFileTransferDirectAndReverse$' ./...`，另运行 `go vet ./...`。真实进程文件传输集成测试不作为本阶段验收前置条件。
