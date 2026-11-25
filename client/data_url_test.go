package main

import "testing"

func TestDataURLPreservesAuthority(t *testing.T) {
	for _, tt := range []struct{ endpoint, want string }{
		{"ws://192.0.2.1:8080/client", "ws://192.0.2.1:8080/data"},
		{"ws://[::1]:8080/client", "ws://[::1]:8080/data"},
		{"wss://[2001:db8::1234]:8443/server?key=value", "wss://[2001:db8::1234]:8443/data"},
		{"ws://gateway.example:8080/client", "ws://gateway.example:8080/data"},
		{"wss://gateway.example.:8443/nested/control?key=value", "wss://gateway.example.:8443/data"},
		{"wss://gateway.example/client", "wss://gateway.example/data"},
		{"ws://[2001:db8::1]:8080/old%2Fpath?key=value", "ws://[2001:db8::1]:8080/data"},
	} {
		t.Run(tt.endpoint, func(t *testing.T) {
			got, err := dataURL(tt.endpoint)
			if err != nil || got != tt.want {
				t.Fatalf("dataURL(%q) = %q, %v; want %q", tt.endpoint, got, err, tt.want)
			}
		})
	}
	if _, err := dataURL("ws://[::1"); err == nil {
		t.Fatal("expected an invalid URL error")
	}
}
