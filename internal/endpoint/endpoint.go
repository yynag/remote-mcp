// Package endpoint 校验 host:port 地址，不执行 DNS 查询。
package endpoint

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

// ParseDial 校验远端地址，返回规范化的 host:port 形式。
// 保留 DNS 原始拼写（包括尾随根域点），供解析器使用。
func ParseDial(value string) (string, error) {
	return parse(value, false)
}

// ParseListen 还允许监听地址使用空主机名，表示通配监听。
// 为符合应用的配置端口策略，明确不支持零端口。
func ParseListen(value string) (string, error) {
	return parse(value, true)
}

func parse(value string, allowEmptyHost bool) (string, error) {
	if strings.Contains(value, "%") {
		return "", fmt.Errorf("不支持 IPv6 zone 和百分号转义")
	}
	if strings.ContainsAny(value, "/\\?#@") {
		return "", fmt.Errorf("期望 host:port 形式，而不是 URL")
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("期望 host:port 形式（IPv6 需要方括号）：%w", err)
	}
	if port == "" {
		return "", fmt.Errorf("必须提供端口")
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("端口只能包含 ASCII 十进制数字")
		}
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return "", fmt.Errorf("端口必须在 1..65535 范围内")
	}
	bracketed := strings.HasPrefix(value, "[")
	switch {
	case host == "":
		if !allowEmptyHost || bracketed {
			return "", fmt.Errorf("必须提供主机名（只有监听地址允许空主机名）")
		}
	case strings.Contains(host, ":"):
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() || !bracketed {
			return "", fmt.Errorf("方括号内的 IPv6 地址无效")
		}
		host = ip.String()
	case bracketed:
		return "", fmt.Errorf("方括号仅对 IPv6 地址有效")
	default:
		if ip, err := netip.ParseAddr(host); err == nil {
			host = ip.String()
		} else if !validDNSName(host) {
			return "", fmt.Errorf("无效的 ASCII DNS 主机名或 IP 地址：%q", host)
		}
	}
	return net.JoinHostPort(host, strconv.FormatUint(n, 10)), nil
}

func validDNSName(host string) bool {
	name := strings.TrimSuffix(host, ".")
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	labels := strings.Split(name, ".")
	allNumeric := true
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
		allNumeric = allNumeric && numericLabel(label)
	}
	// 不将格式错误或旧式数字 IP 地址重新解释为 DNS。非末尾的数字标签
	// （如 123.example）可以接受，但纯十进制末尾标签不能作为主机名顶级域。
	// 同时拒绝以 0x 为前缀的旧式整数 IP 形式。
	last := labels[len(labels)-1]
	return !allNumeric && !decimal(last)
}

func decimal(value string) bool {
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return value != ""
}

func numericLabel(label string) bool {
	if decimal(label) {
		return true
	}
	if len(label) <= 2 || !strings.EqualFold(label[:2], "0x") {
		return false
	}
	for _, c := range label[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
