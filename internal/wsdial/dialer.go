// Package wsdial 为控制通道和数据通道提供直连 WebSocket 拨号。
package wsdial

import (
	"log/slog"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

// Control 保留控制通道的 5 秒握手预算。
func Control() *websocket.Dialer { return newDialer(5 * time.Second) }

// Data 保留 Gorilla 原有的默认 45 秒握手预算。
// 调用方 context 中更早的截止时间仍优先生效。
func Data() *websocket.Dialer { return newDialer(45 * time.Second) }

func newDialer(timeout time.Duration) *websocket.Dialer {
	return &websocket.Dialer{
		Proxy:            nil, // 两类通道均直连，不读取环境代理。
		HandshakeTimeout: timeout,
		// 保持 NetDialContext 为 nil：Gorilla 使用网络类型为 "tcp" 的 net.Dialer，
		// 保留系统 DNS 解析和 Go 的双栈回退能力。
	}
}

// LogConnection 仅记录最终套接字的地址族，不记录地址、URL、凭据或传输元数据。
// 仅在拨号成功后调用。
func LogConnection(channel string, conn *websocket.Conn) {
	slog.Info("[WebSocket连接]", "channel", channel, "local_address_family", addressFamily(conn.LocalAddr()), "remote_address_family", addressFamily(conn.RemoteAddr()))
}

func addressFamily(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp == nil || tcp.IP == nil {
		return "unknown"
	}
	if tcp.IP.To4() != nil {
		return "IPv4"
	}
	if tcp.IP.To16() != nil {
		return "IPv6"
	}
	return "unknown"
}
