package wsdial

import (
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var factories = []struct {
	name    string
	new     func() *websocket.Dialer
	timeout time.Duration
}{
	{"control", Control, 5 * time.Second},
	{"data", Data, 45 * time.Second},
}

func TestDialerPolicy(t *testing.T) {
	global := websocket.DefaultDialer
	proxy := reflect.ValueOf(global.Proxy).Pointer()
	timeout := global.HandshakeTimeout
	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			d := factory.new()
			if d == global || d.Proxy != nil || d.HandshakeTimeout != factory.timeout {
				t.Fatalf("unexpected dialer policy: %+v", d)
			}
			if d.NetDial != nil || d.NetDialContext != nil || d.NetDialTLSContext != nil {
				t.Fatal("must keep Gorilla's standard TCP/system DNS dialing path")
			}
			d.Proxy = http.ProxyFromEnvironment
			d.HandshakeTimeout = time.Nanosecond
			fresh := factory.new()
			if fresh == d || fresh.Proxy != nil || fresh.HandshakeTimeout != factory.timeout {
				t.Fatal("factory must return independent dialers")
			}
		})
	}
	if websocket.DefaultDialer != global || reflect.ValueOf(global.Proxy).Pointer() != proxy || global.HandshakeTimeout != timeout {
		t.Fatal("global DefaultDialer was modified")
	}
}

func TestDialContextEndpointAndError(t *testing.T) {
	// Even when all common proxy environment variables are set, both channels must dial the original host and port.
	// No real DNS resolution, TCP, or IPv6 connection is used here.
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(key, "http://proxy.invalid:3128")
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	endpoints := []struct{ url, address string }{
		{"ws://gateway.example:8080/client", "gateway.example:8080"},
		{"wss://gateway.example.:8443/data", "gateway.example.:8443"},
		{"ws://[2001:db8::1234]:8080/client", "[2001:db8::1234]:8080"},
		{"wss://[::1]:8443/data", "[::1]:8443"},
		{"ws://gateway.example/data", "gateway.example:80"},
		{"wss://[2001:db8::1]/server", "[2001:db8::1]:443"},
	}
	type contextKey struct{}
	for _, factory := range factories {
		for _, endpoint := range endpoints {
			t.Run(factory.name+"/"+endpoint.url, func(t *testing.T) {
				d := factory.new()
				ctx := context.WithValue(context.Background(), contextKey{}, "caller")
				before := time.Now()
				wantErr := errors.New("injected dial failure")
				calls := 0
				d.NetDialContext = func(got context.Context, network, address string) (net.Conn, error) {
					calls++
					if network != "tcp" || address != endpoint.address {
						t.Errorf("dial(%q, %q), want tcp, %q", network, address, endpoint.address)
					}
					if got.Value(contextKey{}) != "caller" {
						t.Error("value in caller context was lost")
					}
					deadline, ok := got.Deadline()
					if !ok || deadline.Before(before.Add(factory.timeout)) || deadline.After(time.Now().Add(factory.timeout)) {
						t.Errorf("incorrect handshake deadline: %v", deadline)
					}
					return nil, wantErr
				}
				conn, response, err := d.DialContext(ctx, endpoint.url, nil)
				if conn != nil || response != nil || !errors.Is(err, wantErr) || calls != 1 {
					t.Fatalf("DialContext = (%v, %v, %v), calls=%d", conn, response, err, calls)
				}
			})
		}
	}
}

func TestDialContextCancellationAndEarlierDeadline(t *testing.T) {
	for _, factory := range factories {
		for _, state := range []string{"canceled", "expired", "earlier deadline"} {
			t.Run(factory.name+"/"+state, func(t *testing.T) {
				var ctx context.Context
				var cancel context.CancelFunc
				switch state {
				case "canceled":
					ctx, cancel = context.WithCancel(context.Background())
					cancel()
				case "expired":
					ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				default:
					// In particular, the server data channel's 10-second context limit
					// must still be stricter than the data handshake's 45-second budget.
					budget := time.Second
					if factory.name == "data" {
						budget = 10 * time.Second
					}
					ctx, cancel = context.WithTimeout(context.Background(), budget)
				}
				defer cancel()
				d := factory.new()
				calls := 0
				wantErr := errors.New("preserve the earlier deadline")
				d.NetDialContext = func(got context.Context, _, _ string) (net.Conn, error) {
					calls++
					if deadline, ok := ctx.Deadline(); ok {
						actual, hasDeadline := got.Deadline()
						if !hasDeadline || !actual.Equal(deadline) {
							t.Errorf("caller deadline changed: got %v, want %v", actual, deadline)
						}
					}
					if ctx.Err() != nil {
						return nil, got.Err()
					}
					cancel()
					if got.Err() != context.Canceled {
						t.Error("caller cancellation was not propagated")
					}
					return nil, wantErr
				}
				_, _, err := d.DialContext(ctx, "ws://gateway.example:8080/data", nil)
				if state != "earlier deadline" {
					wantErr = ctx.Err()
				}
				if calls != 1 || !errors.Is(err, wantErr) {
					t.Fatalf("error=%v, want %v; dial calls=%d", err, wantErr, calls)
				}
			})
		}
	}
}

func TestAddressFamily(t *testing.T) {
	for _, tt := range []struct {
		name string
		addr net.Addr
		want string
	}{
		{"IPv4", &net.TCPAddr{IP: net.ParseIP("192.0.2.1")}, "IPv4"},
		{"IPv6", &net.TCPAddr{IP: net.ParseIP("2001:db8::1")}, "IPv6"},
		{"mapped IPv4", &net.TCPAddr{IP: net.ParseIP("::ffff:192.0.2.1")}, "IPv4"},
		{"nil", nil, "unknown"},
		{"nil TCP", (*net.TCPAddr)(nil), "unknown"},
		{"empty TCP", &net.TCPAddr{}, "unknown"},
		{"non TCP", &net.UnixAddr{Name: "socket", Net: "unix"}, "unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := addressFamily(tt.addr); got != tt.want {
				t.Errorf("addressFamily = %q, want %q", got, tt.want)
			}
		})
	}
}
