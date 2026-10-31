package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"remote-mcp/internal/protocol"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	log.SetPrefix("[remote-mcp-server] ")
	config := flag.String("config", "config.yaml", "服务端 YAML 配置文件路径")
	flag.Parse()
	if flag.NArg() != 0 {
		log.Fatal("unexpected positional arguments")
	}
	cfg, guide, err := LoadConfig(*config)
	if err != nil {
		log.Fatalf("[启动失败] 配置加载失败，文件=%s，原因=%v", *config, err)
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Fatalf("[启动失败] 监听端口失败，实例=%s，地址=%s，原因=%v", cfg.InstanceID, cfg.Listen, err)
	}
	defer listener.Close()
	e := NewEngine(cfg.InstanceID, guide)
	defer e.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := protocol.Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[WebSocket升级失败] 路径=/ws，远端=%s，原因=%v", r.RemoteAddr, err)
			return
		}
		log.Printf("[客户端连接] 模式=直连，远端=%s", conn.RemoteAddr())
		e.ServePeer(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[文件数据连接请求] 远端=%s", r.RemoteAddr)
		e.ServeData(w, r)
	})
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if cfg.Gateway != "" {
		go Reverse(ctx, e, cfg.Gateway)
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()
	mode := "直连"
	if cfg.Gateway != "" {
		mode = "反向连接"
	}
	log.Printf("[启动成功] 实例=%s，模式=%s，监听=%s，transport=%s，指南字节数=%d", cfg.InstanceID, mode, listener.Addr(), cfg.Gateway, len(guide))
	if err = srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Printf("[服务异常退出] 实例=%s，原因=%v", cfg.InstanceID, err)
		os.Exit(1)
	}
	log.Printf("[服务已停止] 实例=%s", cfg.InstanceID)
}
