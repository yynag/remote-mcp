package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadConfigRejectsUnknownFields checks the configuration field allowlist and relative guide path.
func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	directory := t.TempDir()
	guide := filepath.Join(directory, "guide.md")
	if err := os.WriteFile(guide, []byte("guide content"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "server.yaml")
	if err := os.WriteFile(config, []byte("instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\ngateway: ''\n"), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, text, err := LoadConfig(config)
	if err != nil || loaded.InstanceID != "test" || text != "guide content" {
		t.Fatalf("unexpected config load: %+v, guide: %q, error: %v", loaded, text, err)
	}
	if err := os.WriteFile(config, []byte("instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\nunexpected: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = LoadConfig(config); err == nil {
		t.Fatal("unknown configuration field was not rejected")
	}
	if err := os.WriteFile(config, []byte("instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\n---\ninstance_id: second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = LoadConfig(config); err == nil {
		t.Fatal("multiple YAML documents were not rejected")
	}
}

// TestLoadConfigRejectsInvalidGateway rejects invalid relay addresses.
func TestLoadConfigRejectsInvalidGateway(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "guide.md"), []byte("guide"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "server.yaml")
	content := "instance_id: test\nguide: guide.md\nlisten: 127.0.0.1:9000\ngateway: localhost:9098/path\n"
	if err := os.WriteFile(config, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadConfig(config); err == nil {
		t.Fatal("invalid gateway was not rejected")
	}
}

func TestLoadConfigEndpoints(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "guide.md"), []byte("guide"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, listen, gateway, wantListen, wantGateway, errorField string
	}{
		{"IPv4 compatible", "127.0.0.1:9000", "127.0.0.1:9098", "127.0.0.1:9000", "127.0.0.1:9098", ""},
		{"wildcard", ":09000", "", ":9000", "", ""},
		{"IPv6", "[0:0:0:0:0:0:0:0]:9000", "[2001:0DB8::1]:09098", "[::]:9000", "[2001:db8::1]:9098", ""},
		{"DNS preserved", "localhost:9000", "No-Such-Host.invalid.:9098", "localhost:9000", "No-Such-Host.invalid.:9098", ""},
		{"DNS listen root", "Example.invalid.:09000", "localhost:9098", "Example.invalid.:9000", "localhost:9098", ""},
		{"empty gateway host", ":9000", ":9098", "", "", "gateway"},
		{"gateway URL", ":9000", "ws://localhost:9098", "", "", "gateway"},
		{"gateway zone", ":9000", "[fe80::1%en0]:9098", "", "", "gateway"},
		{"gateway bad IP", ":9000", "127.0.0.999:9098", "", "", "gateway"},
		{"gateway port suffix", ":9000", "localhost:9098junk", "", "", "gateway"},
		{"gateway signed port", ":9000", "localhost:+9098", "", "", "gateway"},
		{"listen URL", "ws://localhost:9000", "", "", "", "listen"},
		{"listen illegal host", "bad_host:9000", "", "", "", "listen"},
		{"listen bad IP", "127.0.0.999:9000", "", "", "", "listen"},
		{"listen brackets", "[localhost]:9000", "", "", "", "listen"},
		{"listen zone", "[fe80::1%en0]:9000", "", "", "", "listen"},
		{"listen zero", ":0", "", "", "", "listen"},
		{"listen overflow", ":65536", "", "", "", "listen"},
		{"listen signed port", ":+9000", "", "", "", "listen"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := filepath.Join(directory, "server.yaml")
			content := fmt.Sprintf("instance_id: test\nguide: guide.md\nlisten: %q\ngateway: %q\n", tt.listen, tt.gateway)
			if err := os.WriteFile(config, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			got, text, err := LoadConfig(config)
			if tt.errorField != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errorField) {
					t.Fatalf("expected %s error, got %v", tt.errorField, err)
				}
				return
			}
			if err != nil || got.Listen != tt.wantListen || got.Gateway != tt.wantGateway || text != "guide" {
				t.Fatalf("LoadConfig returned %+v, %q, %v", got, text, err)
			}
		})
	}
}
