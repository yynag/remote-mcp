// Package server 实现远端命令执行和文件传输服务。
package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
		return cfg, "", fmt.Errorf("config requires instance_id, guide and listen")
	}
	_, listenPort, splitErr := net.SplitHostPort(cfg.Listen)
	if splitErr != nil || !validPort(listenPort) {
		return cfg, "", fmt.Errorf("invalid listen address: %s", cfg.Listen)
	}
	if cfg.Gateway != "" {
		host, port, splitErr := net.SplitHostPort(cfg.Gateway)
		if splitErr != nil || net.ParseIP(host) == nil || strings.Contains(cfg.Gateway, "/") || !validPort(port) {
			return cfg, "", fmt.Errorf("gateway must be an IP:PORT address")
		}
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
		return cfg, "", fmt.Errorf("guide exceeds 1 MiB")
	}
	return cfg, string(b), nil
}

func validPort(value string) bool {
	port, err := strconv.Atoi(value)
	return err == nil && port > 0 && port <= 65535
}
