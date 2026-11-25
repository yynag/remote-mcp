package endpoint

import (
	"strings"
	"testing"
)

func TestParseEndpoints(t *testing.T) {
	maxName := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	for _, tt := range []struct {
		input string
		want  string
	}{
		{"127.0.0.1:9098", "127.0.0.1:9098"},
		{"0.0.0.0:1", "0.0.0.0:1"},
		{"255.255.255.255:65535", "255.255.255.255:65535"},
		{"127.0.0.1:00080", "127.0.0.1:80"},
		{"[::1]:80", "[::1]:80"},
		{"[2001:0DB8:0000:0000:0000:0000:0000:0001]:0080", "[2001:db8::1]:80"},
		{"[::]:65535", "[::]:65535"},
		{"[::ffff:192.0.2.1]:80", "[::ffff:192.0.2.1]:80"},
		{"localhost:80", "localhost:80"},
		{"localhost.:80", "localhost.:80"},
		{"No-Such-Host.invalid:80", "No-Such-Host.invalid:80"},
		{"Example.COM.:00080", "Example.COM.:80"},
		{"xn--fiqs8s.example:80", "xn--fiqs8s.example:80"},
		{"123.example:80", "123.example:80"},
		{"123abc:80", "123abc:80"},
		{"0x7f.example:80", "0x7f.example:80"},
		{"deadbeef:80", "deadbeef:80"},
		{maxName + ":80", maxName + ":80"},
		{maxName + ".:80", maxName + ".:80"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			for _, parse := range []func(string) (string, error){ParseDial, ParseListen} {
				got, err := parse(tt.input)
				if err != nil || got != tt.want {
					t.Fatalf("parse(%q) = %q, %v; want %q", tt.input, got, err, tt.want)
				}
			}
		})
	}
}

func TestRejectInvalidEndpoints(t *testing.T) {
	for _, input := range []string{
		"", "localhost", "localhost:", "localhost:http", "localhost:0", "localhost:000",
		"localhost:65536", "localhost:9999999999999999999999999", "localhost:+80", "localhost:-1",
		"localhost:80junk", "localhost:0x50", "localhost:８０", "localhost: 80", "localhost:80 ",
		" localhost:80", "local host:80", "local\thost:80", "local\nhost:80", "local\x00host:80",
		"ws://localhost:80", "http://localhost:80", "localhost:80/path", "localhost/path:80",
		"localhost:80?x=y", "localhost?x:80", "localhost:80#fragment", "localhost#fragment:80",
		"user@localhost:80", "localhost\\path:80", "localhost:80,localhost:81",
		"[fe80::1%en0]:80", "[fe80::1%25en0]:80", "host%20name:80",
		"::1:80", "2001:db8::1:80", "[2001:db8:::1]:80", "[::1]80", "[::1]:80:90",
		"[[::1]]:80", "[localhost]:80", "[127.0.0.1]:80", "[]:80", "[v1.foo]:80",
		"-host:80", "host-:80", "host..example:80", ".example:80", "example..:80", ".:80",
		"_service.example:80", "exämple.example:80", "*.example:80", "host!name:80",
		strings.Repeat("a", 64) + ".example:80",
		strings.Repeat("a.", 127) + "a:80",
		// Reject legacy numeric forms and malformed IPv4 so they are not resolved as DNS.
		"123:80", "2130706433:80", "127.1:80", "127.0.1:80", "127.0.0.999:80",
		"256.0.0.1:80", "127.00.0.1:80", "0177.0.0.1:80", "127.0.0.1.:80",
		"0x7f000001:80", "0X7F.0x0.0x0.0x1:80", "example.123:80",
	} {
		t.Run(input, func(t *testing.T) {
			for _, parse := range []func(string) (string, error){ParseDial, ParseListen} {
				if got, err := parse(input); err == nil {
					t.Fatalf("must not accept %q (parsed as %q)", input, got)
				}
			}
		})
	}
}

func TestEmptyHostOnlyAllowedForListen(t *testing.T) {
	if got, err := ParseListen(":00080"); err != nil || got != ":80" {
		t.Fatalf("wildcard listen = %q, %v", got, err)
	}
	if _, err := ParseDial(":80"); err == nil {
		t.Fatal("empty dial host name was accepted")
	}
}

func TestZoneErrorIsExplicit(t *testing.T) {
	if _, err := ParseDial("[fe80::1%en0]:80"); err == nil || !strings.Contains(err.Error(), "zone") {
		t.Fatalf("expected an explicit zone error, got %v", err)
	}
}
