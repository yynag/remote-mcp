package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	mcpserver "github.com/mark3labs/mcp-go/server"
)

func main() {
	options, err := ParseArgs(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	bridge := NewBridge(options)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = bridge.Start(ctx); err != nil {
		bridge.Close()
		log.Fatal(err)
	}
	defer bridge.Close()
	server := NewMCPServer(bridge)
	stdio := mcpserver.NewStdioServer(server)
	if err = stdio.Listen(ctx, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
