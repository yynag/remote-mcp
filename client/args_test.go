package main

import (
	"net/url"
	"reflect"
	"testing"
)

// TestParseArgs 覆盖中继和多个直连地址的解析与去重。
func TestParseArgs(t *testing.T) {
	options, err := ParseArgs([]string{"--gateway", "127.0.0.1:9098"})
	if err != nil || !options.Gateway || len(options.URLs) != 1 || options.URLs[0] != "ws://127.0.0.1:9098/client" {
		t.Fatalf("gateway 参数解析异常：%+v，错误：%v", options, err)
	}
	options, err = ParseArgs([]string{"--remotes", "127.0.0.1:9000,127.0.0.1:9000"})
	if err != nil || options.Gateway || len(options.URLs) != 1 {
		t.Fatalf("remotes 参数去重异常：%+v，错误：%v", options, err)
	}
}

// TestParseArgsRejectsInvalidEndpoint 确认参数和地址格式错误会被拒绝。
func TestParseArgsRejectsInvalidEndpoint(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--gateway", ":9000"},
		{"--gateway", "127.0.0.1:0"},
		{"--remotes", "127.0.0.1:9000,"},
		{"--gateway", "ws://localhost:9000"},
		{"--gateway", "[fe80::1%en0]:9000"},
		{"--gateway", "localhost:9000/path"},
		{"--gateway", "user@localhost:9000"},
		{"--gateway", "127.0.0.999:9000"},
		{"--gateway", "[localhost]:9000"},
		{"--gateway", "localhost:+9000"},
		{"--gateway", "localhost:9000junk"},
		{"--gateway", "localhost:9000,other:9000"},
		{"--remotes", "localhost:9000", "extra"},
		{"--other", "localhost:9000"},
	} {
		if _, err := ParseArgs(args); err == nil {
			t.Fatalf("无效参数被接受：%v", args)
		}
	}
}

func TestParseArgsEndpointURLs(t *testing.T) {
	for _, tt := range []struct {
		name  string
		flag  string
		input string
		want  []string
	}{
		{"DNS 网关", "--gateway", "localhost:09098", []string{"ws://localhost:9098/client"}},
		{"DNS 拼写", "--gateway", "No-Such-Host.invalid.:9098", []string{"ws://No-Such-Host.invalid.:9098/client"}},
		{"IPv6 网关", "--gateway", "[2001:0DB8::1]:9098", []string{"ws://[2001:db8::1]:9098/client"}},
		{"IPv6 去重", "--remotes", "[::1]:080,[0:0:0:0:0:0:0:1]:80", []string{"ws://[::1]:80/ws"}},
		{"IPv4 去重", "--remotes", "127.0.0.1:080,127.0.0.1:80", []string{"ws://127.0.0.1:80/ws"}},
		{"DNS 大小写去重", "--remotes", "Example.COM:080,example.com:80,EXAMPLE.com:80", []string{"ws://Example.COM:80/ws"}},
		{"DNS 带尾随点的大小写去重", "--remotes", "Example.COM.:80,example.com.:80", []string{"ws://Example.COM.:80/ws"}},
		{"DNS 尾随点视为不同", "--remotes", "host.invalid:80,host.invalid.:80", []string{"ws://host.invalid:80/ws", "ws://host.invalid.:80/ws"}},
		{"混合输入", "--remotes", " localhost:80, [::1]:81 ,127.0.0.1:82", []string{"ws://localhost:80/ws", "ws://[::1]:81/ws", "ws://127.0.0.1:82/ws"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseArgs([]string{tt.flag, tt.input})
			if err != nil || got.Gateway != (tt.flag == "--gateway") || !reflect.DeepEqual(got.URLs, tt.want) {
				t.Fatalf("ParseArgs = %+v，%v；期望 %v", got, err, tt.want)
			}
			for _, raw := range got.URLs {
				u, err := url.Parse(raw)
				if err != nil || u.Scheme != "ws" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
					t.Fatalf("无效端点 URL %q：%v", raw, err)
				}
			}
		})
	}
}
