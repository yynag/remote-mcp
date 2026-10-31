package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfigRejectsUnknownFields 检查配置字段白名单和相对指南路径。
func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	directory := t.TempDir()
	guide := filepath.Join(directory, "guide.md")
	if err := os.WriteFile(guide, []byte("指南内容"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "server.yaml")
	if err := os.WriteFile(config, []byte("instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\ngateway: ''\n"), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, text, err := LoadConfig(config)
	if err != nil || loaded.InstanceID != "test" || text != "指南内容" {
		t.Fatalf("配置读取异常：%+v，指南：%q，错误：%v", loaded, text, err)
	}
	if err := os.WriteFile(config, []byte("instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\nunexpected: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = LoadConfig(config); err == nil {
		t.Fatal("未知配置字段未被拒绝")
	}
	if err := os.WriteFile(config, []byte("instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\n---\ninstance_id: second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = LoadConfig(config); err == nil {
		t.Fatal("多个 YAML 文档未被拒绝")
	}
}

// TestLoadConfigRejectsInvalidGateway 拒绝非 IP:PORT 的中继地址。
func TestLoadConfigRejectsInvalidGateway(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "guide.md"), []byte("guide"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "server.yaml")
	content := "instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\ngateway: localhost:9098\n"
	if err := os.WriteFile(config, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadConfig(config); err == nil {
		t.Fatal("非 IP gateway 未被拒绝")
	}
}
