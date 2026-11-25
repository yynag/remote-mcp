// Package client provides the MCP stdio service and the remote WebSocket bridge.
package main

import (
	"fmt"
	"net/url"
	"strings"

	"remote-mcp/internal/endpoint"
)

// Options holds the parsed client startup arguments.
type Options struct {
	Gateway bool
	URLs    []string
}

// ParseArgs parses either --gateway or --remotes. Exactly one must be present.
func ParseArgs(args []string) (Options, error) {
	if len(args) != 2 || (args[0] != "--gateway" && args[0] != "--remotes") || args[1] == "" {
		return Options{}, fmt.Errorf("usage: remote-mcp --gateway HOST:PORT | --remotes HOST:PORT,HOST:PORT (IPv6 addresses use brackets)")
	}
	values := []string{args[1]}
	path := "/client"
	if args[0] == "--remotes" {
		values = strings.Split(args[1], ",")
		path = "/ws"
	}
	seen := make(map[string]struct{}, len(values))
	urls := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return Options{}, fmt.Errorf("address must not be empty")
		}
		address, err := endpoint.ParseDial(value)
		if err != nil {
			return Options{}, fmt.Errorf("invalid HOST:PORT address %q: %w", value, err)
		}
		remoteURL := (&url.URL{Scheme: "ws", Host: address, Path: path}).String()
		// DNS names are case-insensitive; normalize only the deduplication key,
		// while dialing keeps the original spelling and trailing dot.
		key := strings.ToLower(remoteURL)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		urls = append(urls, remoteURL)
	}
	if len(urls) == 0 {
		return Options{}, fmt.Errorf("no valid connection address")
	}
	return Options{Gateway: args[0] == "--gateway", URLs: urls}, nil
}
