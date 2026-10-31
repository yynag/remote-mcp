package protocol

import "testing"

// TestTransferDefaults 固定检查协议的分块和并发参数。
func TestTransferDefaults(t *testing.T) {
	if DefaultChunkSize != 5*1024*1024 {
		t.Fatalf("分块大小 = %d，期望 5 MiB", DefaultChunkSize)
	}
	if DefaultConcurrency != 4 {
		t.Fatalf("并行数 = %d，期望 4", DefaultConcurrency)
	}
	if MaxDataFrame != 256*1024 {
		t.Fatalf("最大数据帧 = %d，期望 256 KiB", MaxDataFrame)
	}
}

// TestRequestValidation 检查控制请求必需字段的校验。
func TestRequestValidation(t *testing.T) {
	m := Message{Version: 1, Type: "request", ClientID: "c", RequestID: "r", InstanceID: "s", Method: "exec.run"}
	if err := m.ValidateRequest(); err != nil {
		t.Fatal(err)
	}
	m.ClientID = ""
	if m.ValidateRequest() == nil {
		t.Fatal("期望请求校验失败")
	}
}
