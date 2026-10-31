package main

import (
	"context"
	"log"
	"time"

	"remote-mcp/internal/protocol"
	"remote-mcp/internal/wsdial"
)

// Reverse 持续连接中继，并在连接断开后自动重连。
func Reverse(ctx context.Context, e *Engine, address string) {
	for ctx.Err() == nil {
		log.Printf("[连接中继] 实例=%s，地址=%s", e.id, address)
		dialer := wsdial.Control()
		conn, _, err := dialer.DialContext(ctx, "ws://"+address+"/server", nil)
		if err == nil {
			wsdial.LogConnection("server.control", conn)
			log.Printf("[中继连接成功] 实例=%s，地址=%s", e.id, address)
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
			log.Printf("[中继连接断开] 实例=%s，地址=%s，将在2秒后重连", e.id, address)
		} else {
			log.Printf("[中继连接失败] 实例=%s，地址=%s，错误=%v", e.id, address, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
