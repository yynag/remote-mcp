package main

import (
	"context"
	"log/slog"
	"time"

	"remote-mcp/internal/protocol"
	"remote-mcp/internal/wsdial"
)

// Reverse 持续连接中继，并在连接断开后自动重连。
func Reverse(ctx context.Context, e *Engine, address string) {
	for ctx.Err() == nil {
		slog.Info("[连接中继]", "instance", e.id, "address", address)
		dialer := wsdial.Control()
		conn, _, err := dialer.DialContext(ctx, "ws://"+address+"/server", nil)
		if err == nil {
			wsdial.LogConnection("server.control", conn)
			slog.Info("[中继连接成功]", "instance", e.id, "address", address)
			p := protocol.NewPeer(conn)
			done := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					p.Close()
				case <-done:
				}
			}()
			e.ServeReversePeer(p, "ws://"+address+"/data")
			close(done)
			slog.Warn("[中继连接断开]", "instance", e.id, "address", address)
		} else {
			slog.Error("[中继连接失败]", "instance", e.id, "address", address, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
