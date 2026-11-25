package main

import (
	"context"
	"log/slog"
	"time"

	"remote-mcp/internal/protocol"
	"remote-mcp/internal/wsdial"
)

// Reverse continuously connects to the relay and reconnects automatically after a disconnect.
func Reverse(ctx context.Context, e *Engine, address string) {
	for ctx.Err() == nil {
		slog.Info("[connecting to relay]", "instance", e.id, "address", address)
		dialer := wsdial.Control()
		conn, _, err := dialer.DialContext(ctx, "ws://"+address+"/server", nil)
		if err == nil {
			wsdial.LogConnection("server.control", conn)
			slog.Info("[relay connected]", "instance", e.id, "address", address)
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
			slog.Warn("[relay connection lost]", "instance", e.id, "address", address)
		} else {
			slog.Error("[relay connection failed]", "instance", e.id, "address", address, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
