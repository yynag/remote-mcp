# Windows 远端服务器使用指南

## 环境与工具

- 提供 Windows 11 和 Fedora WSL2；两者共享部分文件，但命令、路径和进程管理不可混用。
- 已安装 PowerShell 7.6.9、Python 3 和 WSL2 Fedora；其他工具使用前检查是否可用。
- Windows 程序及 IDA、JADX 等工具在 Windows 环境运行；Linux 命令和编译工具链使用 Fedora WSL2。

## 文件管理

- Windows 工作区：`C:\Users\admin\Workspace`；Fedora WSL2 工作区：`~/Workspace`，不存在时创建。
- Windows 工作区下，`Research` 存放研究项目，创建前检查是否已有相关项目；`Files` 存放用户文件。
- 项目按主要运行环境放入对应工作区；Linux 编译及密集文件操作使用 WSL2 原生文件系统，避免跨文件系统访问带来的性能开销。
- 仅在跨环境协作时共享文件，避免重复复制整个项目。
- 工作区不是访问限制；操作其他路径前说明目标及影响。文件传输无路径沙箱，上传前确认可信实例、目标路径和覆盖选项。
- 超出用户授权范围的文件修改或破坏性操作须先确认；不得在输出、脚本或日志中暴露密码、令牌或密钥。

## 执行优化

- MCP 的 `exec_run`/`job_start` 使用 `powershell.exe -NoProfile -NonInteractive -Command`，不是 PowerShell 7；需要时显式调用 `pwsh.exe`。SSH 默认使用 `cmd.exe`。
- Linux 任务通过 `wsl.exe -d Fedora -- <命令>` 执行。
- 命令按目标 shell 原样解析，不自动转换语法或转义；不要拼接未经校验的参数。
- 复杂或多行脚本优先写成文件，检查后上传，再显式调用已安装的解释器。
- 长任务使用后台执行并设置合理超时，记录任务 ID 和输出位置；完成后核对退出码及预期结果。
- 网络失败先确认执行状态，避免重复操作；无法恢复时报告错误，不无限重试。
- 安装软件、修改系统、提权或停止服务前，确认已有用户授权。
