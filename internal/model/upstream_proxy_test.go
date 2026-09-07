package model

import "testing"

func TestParseProxyURL(t *testing.T) {
	tests := []struct {
		name    string
		address string
		valid   bool
	}{
		{"http", "http://127.0.0.1:8080", true},
		{"authenticated socks5", "socks5://user:pass@127.0.0.1:1080", true},
		{"socks5h", "socks5h://proxy.example:1080", true},
		{"missing host", "http://:8080", false},
		{"unsupported scheme", "ftp://127.0.0.1:21", false},
		{"invalid port", "http://127.0.0.1:65536", false},
		{"path", "http://127.0.0.1:8080/proxy", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProxyURL(tt.address)
			if (err == nil) != tt.valid {
				t.Fatalf("ParseProxyURL(%q) error = %v, valid = %v", tt.address, err, tt.valid)
			}
		})
	}
}

func TestNormalizeProxyDefaultsLegacyStorage(t *testing.T) {
	s := Storage{}
	if err := s.NormalizeProxy(); err != nil {
		t.Fatal(err)
	}
	if s.APIProxyMode != ProxySystem || s.TransferProxyMode != ProxySystem {
		t.Fatalf("unexpected defaults: %q, %q", s.APIProxyMode, s.TransferProxyMode)
	}
}
