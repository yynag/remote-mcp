// Package server implements remote command execution and file transfer services.
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

// Config is the only YAML configuration format supported by the server.
type Config struct {
	InstanceID string `yaml:"instance_id"`
	Guide      string `yaml:"guide"`
	Listen     string `yaml:"listen"`
	Gateway    string `yaml:"gateway"`
}

// LoadConfig reads the server configuration and guide body.
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
		return cfg, "", fmt.Errorf("configuration file must contain exactly one YAML document")
	}
	if cfg.InstanceID == "" || cfg.Guide == "" || cfg.Listen == "" {
		return cfg, "", fmt.Errorf("configuration must include instance_id, guide, and listen")
	}
	listen, err := endpoint.ParseListen(cfg.Listen)
	if err != nil {
		return cfg, "", fmt.Errorf("invalid listen address %q: %w", cfg.Listen, err)
	}
	cfg.Listen = listen
	if cfg.Gateway != "" {
		gateway, err := endpoint.ParseDial(cfg.Gateway)
		if err != nil {
			return cfg, "", fmt.Errorf("invalid gateway HOST:PORT address %q: %w", cfg.Gateway, err)
		}
		cfg.Gateway = gateway
	}
	guidePath := cfg.Guide
	if !filepath.IsAbs(guidePath) {
		// Prefer reading the guide from the executable's directory so the binary, configuration, and guide.md can be deployed together.
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
		return cfg, "", fmt.Errorf("guide body exceeds 1 MiB")
	}
	return cfg, string(b), nil
}
