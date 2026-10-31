package protocol

import "time"

// TransferSpec 是文件传输准备或查询请求中的传输描述。
type TransferSpec struct {
	TransferID  string   `json:"transfer_id,omitempty"`
	Path        string   `json:"path"`
	Size        int64    `json:"size"`
	SHA256      string   `json:"sha256"`
	ChunkSize   int64    `json:"chunk_size"`
	Concurrency int      `json:"concurrency"`
	Direction   string   `json:"direction,omitempty"`
	Overwrite   bool     `json:"overwrite,omitempty"`
	MTime       string   `json:"mtime,omitempty"`
	ChunkSHA256 []string `json:"chunk_sha256,omitempty"`
}

// TransferStatus 描述服务端当前的传输进度。
type TransferStatus struct {
	TransferID      string    `json:"transfer_id"`
	State           string    `json:"state"`
	CompletedChunks []int     `json:"completed_chunks"`
	ExpiresAt       time.Time `json:"expires_at"`
	Size            int64     `json:"size"`
	SHA256          string    `json:"sha256"`
	ChunkSHA256     []string  `json:"chunk_sha256"`
	ChunkSize       int64     `json:"chunk_size"`
	Concurrency     int       `json:"concurrency"`
	MTime           string    `json:"mtime"`
	Direction       string    `json:"direction"`
}
