// Package client 提供 MCP stdio 服务和远端 WebSocket 桥接。
package main

import (
	"fmt"
	"net/url"
	"strings"

	"remote-mcp/internal/endpoint"
)

// Options 是 client 启动参数解析后的结果。
type Options struct {
	Gateway bool
	URLs    []string
}

// ParseArgs 解析 --gateway 或 --remotes。两者必须且只能出现一个。
func ParseArgs(args []string) (Options, error) {
	if len(args) != 2 || (args[0] != "--gateway" && args[0] != "--remotes") || args[1] == "" {
		return Options{}, fmt.Errorf("用法：remote-mcp --gateway HOST:PORT | --remotes HOST:PORT,HOST:PORT（IPv6 使用方括号）")
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
			return Options{}, fmt.Errorf("地址不能为空")
		}
		address, err := endpoint.ParseDial(value)
		if err != nil {
			return Options{}, fmt.Errorf("无效 HOST:PORT 地址 %q：%w", value, err)
		}
		remoteURL := (&url.URL{Scheme: "ws", Host: address, Path: path}).String()
		// DNS 大小写等价；仅规范去重键，拨号保留原始拼写和尾随点。
		key := strings.ToLower(remoteURL)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		urls = append(urls, remoteURL)
	}
	if len(urls) == 0 {
		return Options{}, fmt.Errorf("没有有效的连接地址")
	}
	return Options{Gateway: args[0] == "--gateway", URLs: urls}, nil
}
