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
		t.Fatalf("输出不符合预期：%#v", got)
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
		t.Fatal("任务未结束")
	}
	b, offset, end := job.stdout.Read(2)
	if string(b) != "etwo" || offset != 2 || end != 6 {
		t.Fatalf("偏移读取 = %q [%d,%d)", b, offset, end)
	}
}

func TestInstanceGuideIsReturnedOnlyOnDemand(t *testing.T) {
	e := NewEngine("win11prow", "Windows 平台指南正文")
	defer e.Close()

	list := e.Handle(protocol.Message{Version: 1, Type: "request", ClientID: "c", RequestID: "list", InstanceID: "win11prow", Method: "instance.list"})
	if list.Error != nil {
		t.Fatal(list.Error)
	}
	encoded, err := json.Marshal(list.Result)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"instances":[{"type":"hello","instance_id":"win11prow"}]}` {
		t.Fatalf("实例列表包含多余字段或指南：%s", encoded)
	}

	guide := e.Handle(protocol.Message{Version: 1, Type: "request", ClientID: "c", RequestID: "guide", InstanceID: "win11prow", Method: "instance.get"})
	if guide.Error != nil {
		t.Fatal(guide.Error)
	}
	if got := guide.Result.(protocol.Hello).Guide; got != "Windows 平台指南正文" {
		t.Fatalf("按需读取的指南不匹配：%q", got)
	}
}
