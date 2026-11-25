// Package endpoint validates host:port addresses without performing DNS lookups.
package endpoint

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

// ParseDial validates a remote address and returns it in canonical host:port form.
// It preserves the original DNS spelling (including a trailing root dot) for the resolver.
func ParseDial(value string) (string, error) {
	return parse(value, false)
}

// ParseListen also allows an empty host name for listen addresses, meaning a wildcard listener.
// To match the application's configured port policy, port zero is explicitly unsupported.
func ParseListen(value string) (string, error) {
	return parse(value, true)
}

func parse(value string, allowEmptyHost bool) (string, error) {
	if strings.Contains(value, "%") {
		return "", fmt.Errorf("IPv6 zones and percent escapes are not supported")
	}
	if strings.ContainsAny(value, "/\\?#@") {
		return "", fmt.Errorf("expected host:port form, not a URL")
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("expected host:port form (IPv6 addresses require brackets): %w", err)
	}
	if port == "" {
		return "", fmt.Errorf("port must be provided")
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("port must contain only ASCII decimal digits")
		}
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return "", fmt.Errorf("port must be in the range 1..65535")
	}
	bracketed := strings.HasPrefix(value, "[")
	switch {
	case host == "":
		if !allowEmptyHost || bracketed {
			return "", fmt.Errorf("host name must be provided (only listen addresses allow an empty host name)")
		}
	case strings.Contains(host, ":"):
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() || !bracketed {
			return "", fmt.Errorf("invalid IPv6 address inside brackets")
		}
		host = ip.String()
	case bracketed:
		return "", fmt.Errorf("brackets are only valid for IPv6 addresses")
	default:
		if ip, err := netip.ParseAddr(host); err == nil {
			host = ip.String()
		} else if !validDNSName(host) {
			return "", fmt.Errorf("invalid ASCII DNS host name or IP address: %q", host)
		}
	}
	return net.JoinHostPort(host, strconv.FormatUint(n, 10)), nil
}

func validDNSName(host string) bool {
	name := strings.TrimSuffix(host, ".")
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	labels := strings.Split(name, ".")
	allNumeric := true
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
		allNumeric = allNumeric && numericLabel(label)
	}
	// Do not reinterpret malformed or legacy numeric IP addresses as DNS. Numeric non-final labels
	// (such as 123.example) are acceptable, but a purely decimal final label cannot be a host name's
	// top-level domain. Also reject legacy integer IP forms prefixed with 0x.
	last := labels[len(labels)-1]
	return !allNumeric && !decimal(last)
}

func decimal(value string) bool {
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return value != ""
}

func numericLabel(label string) bool {
	if decimal(label) {
		return true
	}
	if len(label) <= 2 || !strings.EqualFold(label[:2], "0x") {
		return false
	}
	for _, c := range label[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
