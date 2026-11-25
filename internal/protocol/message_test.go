package protocol

import "testing"

// TestTransferDefaults pins the protocol chunk and concurrency parameters.
func TestTransferDefaults(t *testing.T) {
	if DefaultChunkSize != 5*1024*1024 {
		t.Fatalf("chunk size = %d, want 5 MiB", DefaultChunkSize)
	}
	if DefaultConcurrency != 4 {
		t.Fatalf("concurrency = %d, want 4", DefaultConcurrency)
	}
	if MaxDataFrame != 256*1024 {
		t.Fatalf("max data frame = %d, want 256 KiB", MaxDataFrame)
	}
}

// TestRequestValidation checks validation of required control request fields.
func TestRequestValidation(t *testing.T) {
	m := Message{Version: 1, Type: "request", ClientID: "c", RequestID: "r", InstanceID: "s", Method: "exec.run"}
	if err := m.ValidateRequest(); err != nil {
		t.Fatal(err)
	}
	m.ClientID = ""
	if m.ValidateRequest() == nil {
		t.Fatal("expected request validation to fail")
	}
}
