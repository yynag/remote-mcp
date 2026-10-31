// Package client 提供 MCP stdio 服务和远端 WebSocket 桥接。
package main

import (
	"fmt"
	"net"
	"strings"
)

// Options 是 client 启动参数解析后的结果。
type Options struct {
	Gateway bool
	URLs    []string
}

// ParseArgs 解析 --gateway 或 --remotes。两者必须且只能出现一个。
func ParseArgs(args []string) (Options, error) {
	if len(args) != 2 || (args[0] != "--gateway" && args[0] != "--remotes") || args[1] == "" {
		return Options{}, fmt.Errorf("用法：remote-mcp --gateway IP:PORT | --remotes IP:PORT,IP:PORT")
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
		host, port, err := net.SplitHostPort(value)
		if err != nil || net.ParseIP(host) == nil || !validPort(port) {
			return Options{}, fmt.Errorf("地址必须是 IP:PORT：%s", value)
		}
		url := "ws://" + net.JoinHostPort(host, port) + path
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}
	if len(urls) == 0 {
		return Options{}, fmt.Errorf("没有有效的连接地址")
	}
	return Options{Gateway: args[0] == "--gateway", URLs: urls}, nil
}

func validPort(value string) bool {
	var port int
	if _, err := fmt.Sscanf(value, "%d", &port); err != nil {
		return false
	}
	return port > 0 && port <= 65535 && fmt.Sprintf("%d", port) == value
}
