// Package server 实现远端命令执行和文件传输服务。
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"remote-mcp/internal/endpoint"

	"gopkg.in/yaml.v3"
)

// Config 是 server 支持的唯一 YAML 配置格式。
type Config struct {
	InstanceID string `yaml:"instance_id"`
	Guide      string `yaml:"guide"`
	Listen     string `yaml:"listen"`
	Gateway    string `yaml:"gateway"`
}

// LoadConfig 读取服务端配置和指南正文。
func LoadConfig(path string) (Config, string, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, "", err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(b))
	decoder.KnownFields(true)
	if err = decoder.Decode(&cfg); err != nil {
		return cfg, "", err
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return cfg, "", fmt.Errorf("配置文件只能包含一个 YAML 文档")
	}
	if cfg.InstanceID == "" || cfg.Guide == "" || cfg.Listen == "" {
		return cfg, "", fmt.Errorf("配置必须包含 instance_id、guide 和 listen")
	}
	listen, err := endpoint.ParseListen(cfg.Listen)
	if err != nil {
		return cfg, "", fmt.Errorf("无效的 listen 地址 %q：%w", cfg.Listen, err)
	}
	cfg.Listen = listen
	if cfg.Gateway != "" {
		gateway, err := endpoint.ParseDial(cfg.Gateway)
		if err != nil {
			return cfg, "", fmt.Errorf("无效的 gateway HOST:PORT 地址 %q：%w", cfg.Gateway, err)
		}
		cfg.Gateway = gateway
	}
	guidePath := cfg.Guide
	if !filepath.IsAbs(guidePath) {
		// 优先从可执行文件所在目录读取指南，便于将二进制、配置和 guide.md 一起部署。
		if executable, executableErr := os.Executable(); executableErr == nil {
			candidate := filepath.Join(filepath.Dir(executable), guidePath)
			if _, candidateErr := os.Stat(candidate); candidateErr == nil {
				guidePath = candidate
			}
		}
		if !filepath.IsAbs(guidePath) {
			guidePath = filepath.Join(filepath.Dir(path), guidePath)
		}
	}
	b, err = os.ReadFile(guidePath)
	if err != nil {
		return cfg, "", err
	}
	if len(b) > 1024*1024 {
		return cfg, "", fmt.Errorf("指南正文超过 1 MiB")
	}
	return cfg, string(b), nil
}
