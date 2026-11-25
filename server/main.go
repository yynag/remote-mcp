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
	config := flag.String("config", "config.yaml", "server YAML configuration file path")
	debug := flag.Bool("debug", false, "enable DEBUG logging")
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
		slog.Error("positional arguments are not accepted")
		os.Exit(1)
	}
	cfg, guide, err := LoadConfig(*config)
	if err != nil {
		slog.Error("[startup failed] configuration load failed", "file", *config, "error", err)
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		slog.Error("[startup failed] listen failed", "instance", cfg.InstanceID, "address", cfg.Listen, "error", err)
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
			slog.Error("[WebSocket upgrade failed]", "path", "/ws", "remote", r.RemoteAddr, "error", err)
			return
		}
		slog.Info("[client connected]", "mode", "direct", "remote", conn.RemoteAddr())
		e.ServePeer(protocol.NewPeer(conn))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("[file data connection request]", "remote", r.RemoteAddr)
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
	mode := "direct"
	if cfg.Gateway != "" {
		mode = "reverse connection"
	}
	slog.Info("[startup succeeded]", "instance", cfg.InstanceID, "mode", mode, "listen", listener.Addr(), "transport", cfg.Gateway, "guide_bytes", len(guide))
	if err = srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		slog.Error("[service exited unexpectedly]", "instance", cfg.InstanceID, "error", err)
		os.Exit(1)
	}
	slog.Info("[service stopped]", "instance", cfg.InstanceID)
}
