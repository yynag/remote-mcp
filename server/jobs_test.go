package main

import (
	"encoding/json"
	"testing"
	"time"

	"remote-mcp/internal/protocol"
)

// TestExecAndJobOutputOffsets 检查命令输出和增量任务输出偏移。
func TestExecAndJobOutputOffsets(t *testing.T) {
	e := NewEngine("test", "guide")
	defer e.Close()
	params, _ := json.Marshal(commandParams{Command: "printf 'alpha'; printf 'beta' >&2"})
	req := protocol.Message{Version: 1, Type: "request", ClientID: "c", RequestID: "r", InstanceID: "test", Method: "exec.run", Params: params}
	result := e.Handle(req)
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	got := result.Result.(map[string]any)
	if got["stdout"] != "alpha" || got["stderr"] != "beta" {
		t.Fatalf("unexpected output: %#v", got)
	}

	params, _ = json.Marshal(commandParams{Command: "printf 'one'; sleep 1; printf 'two'"})
	req.Method = "job.start"
	req.Params = params
	started := e.Handle(req)
	if started.Error != nil {
		t.Fatal(started.Error)
	}
	id := started.Result.(map[string]any)["job_id"].(string)
	job, err := e.getJob(id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-job.done:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not finish")
	}
	b, offset, end := job.stdout.Read(2)
	if string(b) != "etwo" || offset != 2 || end != 6 {
		t.Fatalf("offset read = %q [%d,%d)", b, offset, end)
	}
}
