// Package wsdial provides direct WebSocket dialing for control and data channels.
package wsdial

import (
	"log/slog"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

// Control keeps the 5-second handshake budget for the control channel.
func Control() *websocket.Dialer { return newDialer(5 * time.Second) }

// Data keeps Gorilla's original default 45-second handshake budget.
// An earlier deadline in the caller's context still takes precedence.
func Data() *websocket.Dialer { return newDialer(45 * time.Second) }

func newDialer(timeout time.Duration) *websocket.Dialer {
	return &websocket.Dialer{
		Proxy:            nil, // Both channels connect directly and ignore environment proxies.
		HandshakeTimeout: timeout,
		// Keep NetDialContext nil: Gorilla uses a net.Dialer with network type "tcp",
		// preserving system DNS resolution and Go's dual-stack fallback.
	}
}

// LogConnection logs only the address family of the final socket; it does not log addresses, URLs, credentials, or transfer metadata.
// Call it only after a successful dial.
func LogConnection(channel string, conn *websocket.Conn) {
	slog.Info("[WebSocket connection]", "channel", channel, "local_address_family", addressFamily(conn.LocalAddr()), "remote_address_family", addressFamily(conn.RemoteAddr()))
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
