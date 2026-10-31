package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"time"

	"remote-mcp/internal/protocol"
)

func fileInfo(info os.FileInfo) map[string]any {
	return map[string]any{"name": info.Name(), "size": info.Size(), "is_dir": info.IsDir(), "mode": info.Mode().String(), "modified_at": info.ModTime().UTC().Format(time.RFC3339Nano)}
}

func fileRequest(req protocol.Message) (any, error) {
	var p struct {
		Path   string `json:"path"`
		SHA256 bool   `json:"sha256"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return nil, err
	}
	if p.Path == "" {
		p.Path = "."
	}
	if req.Method == "file.list" {
		entries, err := os.ReadDir(p.Path)
		if err != nil {
			return nil, err
		}
		out := make([]map[string]any, 0, len(entries))
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			out = append(out, fileInfo(info))
		}
		return map[string]any{"path": p.Path, "entries": out}, nil
	}
	info, err := os.Stat(p.Path)
	if err != nil {
		return nil, err
	}
	result := fileInfo(info)
	result["path"] = p.Path
	if p.SHA256 && info.Mode().IsRegular() {
		f, err := os.Open(p.Path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		hash := sha256.New()
		if _, err = io.Copy(hash, f); err != nil {
			return nil, err
		}
		result["sha256"] = hex.EncodeToString(hash.Sum(nil))
	}
	return result, nil
}
