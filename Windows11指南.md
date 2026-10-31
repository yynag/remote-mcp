# Windows 远端服务器指南

## 环境

- Windows 工作区：`C:\Users\admin\Workspace`
- 默认命令解释器：`powershell.exe -NoProfile -NonInteractive -Command`。
- 已知工具：PowerShell 7.6.9、Python 3、WSL2 Fedora；使用其他工具前先检查是否安装。
- Windows 原生环境用于运行 Windows 程序、处理 Windows 路径及使用 IDA、JADX 等工具。
- Fedora WSL 用于 Linux 命令、脚本和编译工具链，通过 `wsl.exe -d Fedora -- <命令>` 调用。
- Windows 工作区在 Fedora WSL 中通常对应 `/mnt/c/Users/admin/Workspace`。两种环境共享文件，但命令、路径和进程管理方式不同，不要混用。

## 工作区

- `Research`：研究项目。创建项目前先检查是否已有相关目录；新项目放入独立子目录。
- `Files`：用户文件。
- 默认优先在 Windows 工作区操作。Windows 程序需要的文件保留在 Windows 文件系统；Linux 大量编译产生的临时文件可放在 Fedora 文件系统。
- 工作区是操作建议，不是访问限制。需要在其他路径操作时，先向用户说明目标路径和影响。

## 操作约定

- 执行前确认当前 shell、操作系统和工作目录。SSH 登录默认是 `cmd.exe`；MCP 命令在 Windows 上使用 `powershell.exe`。需要 PowerShell 7 时明确运行 `pwsh.exe`。
- 命令按当前 shell 语法原样执行，不会自动转换或转义。复杂或多行脚本写入文件后，使用明确的解释器调用，例如 `pwsh.exe -File`、`python` 或 `wsl.exe -d Fedora -- bash`。
- 未经用户明确确认，不删除、移动、覆盖或批量改名 `Files` 和已有项目中的内容。安装软件、修改系统配置、提升权限、停止服务等操作前，先说明影响并取得确认。
- 网络故障排除命令和参数错误后最多重试三次；仍失败则停止并报告。长任务设置超时，记录任务 ID，并检查退出码和结果。
- 不在命令输出、脚本或日志中暴露密码、令牌和密钥。
