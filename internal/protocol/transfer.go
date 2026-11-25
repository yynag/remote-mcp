package protocol

import "time"

// TransferSpec is the transfer description in a file transfer prepare or query request.
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

// TransferStatus describes the current transfer progress on the server.
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
