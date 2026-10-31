package main

import (
	"encoding/base64"
	"sync"
	"time"
)

const (
	maxJobOutput       = 4 * 1024 * 1024
	jobOutputRetention = 10 * time.Minute
)

type streamCache struct {
	data []byte
	base int64
}

func (s *streamCache) append(offset int64, data []byte) {
	end := s.base + int64(len(s.data))
	if offset > end {
		s.base, s.data = offset, nil
		end = offset
	}
	if offset < end {
		skip := end - offset
		if skip >= int64(len(data)) {
			return
		}
		data = data[skip:]
	}
	s.data = append(s.data, data...)
	if len(s.data) > maxJobOutput {
		extra := len(s.data) - maxJobOutput
		s.base += int64(extra)
		s.data = append([]byte(nil), s.data[extra:]...)
	}
}

func (s *streamCache) read(offset int64) (string, int64, int64, bool) {
	end := s.base + int64(len(s.data))
	start := offset
	if start < s.base {
		start = s.base
	}
	if start > end {
		start = end
	}
	return string(s.data[start-s.base:]), start, end, offset < s.base
}

type cachedJob struct {
	stdout      streamCache
	stderr      streamCache
	completedAt time.Time
	status      any
	gap         bool
}

// JobCache 缓存服务端推送的任务增量输出。
type JobCache struct {
	mu   sync.Mutex
	jobs map[string]*cachedJob
}

func newJobCache() *JobCache { return &JobCache{jobs: make(map[string]*cachedJob)} }

func (c *JobCache) key(instanceID, jobID string) string { return instanceID + "\x00" + jobID }

func (c *JobCache) job(instanceID, jobID string) *cachedJob {
	key := c.key(instanceID, jobID)
	job := c.jobs[key]
	if job == nil {
		job = &cachedJob{}
		c.jobs[key] = job
	}
	return job
}

func (c *JobCache) event(instanceID, event string, data any) {
	value, ok := data.(map[string]any)
	if !ok {
		return
	}
	jobID, ok := value["job_id"].(string)
	if !ok || jobID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.job(instanceID, jobID)
	if event == "job.completed" {
		job.completedAt = time.Now()
		job.status = data
		return
	}
	if event != "job.output" {
		return
	}
	stream, ok := value["stream"].(string)
	offset, okOffset := value["offset"].(float64)
	encoded, okData := value["data_base64"].(string)
	if !ok || !okOffset || !okData || (stream != "stdout" && stream != "stderr") || offset < 0 {
		return
	}
	dataBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return
	}
	if stream == "stdout" {
		job.stdout.append(int64(offset), dataBytes)
	} else {
		job.stderr.append(int64(offset), dataBytes)
	}
}

func (c *JobCache) disconnected(instanceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, job := range c.jobs {
		if len(key) > len(instanceID) && key[:len(instanceID)] == instanceID && key[len(instanceID)] == '\x00' && job.completedAt.IsZero() {
			job.gap = true
		}
	}
}

func (c *JobCache) read(instanceID, jobID string, stdoutOffset, stderrOffset int64) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.jobs[c.key(instanceID, jobID)]
	if job == nil || job.gap {
		return nil, false
	}
	stdout, so, sn, st := job.stdout.read(stdoutOffset)
	stderr, eo, en, et := job.stderr.read(stderrOffset)
	return map[string]any{
		"job_id": jobID, "stdout": stdout, "stderr": stderr,
		"stdout_offset": so, "stderr_offset": eo, "stdout_next_offset": sn,
		"stderr_next_offset": en, "output_truncated": st || et,
		"status": job.status, "source": "client_push_cache",
	}, true
}

func (c *JobCache) reap(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, job := range c.jobs {
		if !job.completedAt.IsZero() && now.Sub(job.completedAt) >= jobOutputRetention {
			delete(c.jobs, key)
		}
	}
}
