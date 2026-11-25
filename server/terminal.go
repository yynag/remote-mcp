package main

import "time"

// 终端固定尺寸与会话生命周期常量。
const (
	terminalCols = 120
	terminalRows = 40

	// sessionIdleTimeout 是会话无读写进展后的回收时间。
	sessionIdleTimeout = 30 * time.Minute
	// sessionRetention 是会话结束后输出缓存的保留时间。
	sessionRetention = 10 * time.Minute
	// maxSessions 是单个实例的并发会话上限。
	maxSessions = 8
	// defaultReadLimit 是单次读取的默认与最大字节数。
	defaultReadLimit = 64 * 1024
	// maxWaitMS 是 session.read 的最长等待毫秒数。
	maxWaitMS = int64(30000)
)

// terminal 抽象一个可读写的伪终端。
// Unix 使用 creack/pty，Windows 使用 ConPTY。
type terminal interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
	// Wait 阻塞到进程退出并返回退出码，只可调用一次。
	Wait() (int, error)
	Pid() int
}
