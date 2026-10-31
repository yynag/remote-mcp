package main

import "sync"

const maxOutput = 4 * 1024 * 1024

// output 是有界的字节尾部缓存，偏移量始终对应完整字节流。
type output struct {
	mu   sync.Mutex
	data []byte
	base int64
}

func (b *output) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if n >= maxOutput {
		b.base += int64(len(b.data) + n - maxOutput)
		b.data = append(b.data[:0], p[n-maxOutput:]...)
	} else {
		extra := len(b.data) + n - maxOutput
		if extra > 0 {
			copy(b.data, b.data[extra:])
			b.data = b.data[:len(b.data)-extra]
			b.base += int64(extra)
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}

func (b *output) Read(offset int64) ([]byte, int64, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	end := b.base + int64(len(b.data))
	if offset < b.base {
		offset = b.base
	}
	if offset > end {
		offset = end
	}
	return append([]byte(nil), b.data[offset-b.base:]...), offset, end
}
