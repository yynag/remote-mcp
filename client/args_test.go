package main

import "testing"

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
		{"--gateway", "localhost:9000"},
		{"--gateway", "127.0.0.1:0"},
		{"--remotes", "127.0.0.1:9000,"},
	} {
		if _, err := ParseArgs(args); err == nil {
			t.Fatalf("无效参数被接受：%v", args)
		}
	}
}
