package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"remote-mcp/internal/protocol"
)

func main() {
	config := flag.String("config", "config.yaml", "服务端 YAML 配置文件路径")
	debug := flag.Bool("debug", false, "启用 DEBUG 日志")
	flag.Parse()
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	log.SetFlags(0)
	log.SetPrefix("[remote-mcp-server] ")
	log.SetOutput(slog.NewLogLogger(slog.Default().Handler(), slog.LevelInfo).Writer())
	if flag.NArg() != 0 {
		slog.Error("不接受位置参数")
		os.Exit(1)
	}
	cfg, guide, err := LoadConfig(*config)
	if err != nil {
		slog.Error("[启动失败] 配置加载失败", "文件", *config, "原因", err)
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		slog.Error("[启动失败] 监听端口失败", "实例", cfg.InstanceID, "地址", cfg.Listen, "原因", err)
		os.Exit(1)
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
			slog.Error("[WebSocket升级失败]", "path", "/ws", "remote", r.RemoteAddr, "error", err)
			return
		}
		slog.Info("[客户端连接]", "mode", "直连", "remote", conn.RemoteAddr())
		e.ServePeer(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("[文件数据连接请求]", "remote", r.RemoteAddr)
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
	slog.Info("[启动成功]", "instance", cfg.InstanceID, "mode", mode, "listen", listener.Addr(), "transport", cfg.Gateway, "guide_bytes", len(guide))
	if err = srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		slog.Error("[服务异常退出]", "实例", cfg.InstanceID, "原因", err)
		os.Exit(1)
	}
	slog.Info("[服务已停止]", "instance", cfg.InstanceID)
}
