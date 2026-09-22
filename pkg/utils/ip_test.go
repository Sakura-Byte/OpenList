package utils

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

func TestClientIP(t *testing.T) {
	t.Cleanup(func() {
		if err := ConfigureClientIP(nil, nil); err != nil {
			t.Fatalf("reset client IP config: %v", err)
		}
	})

	t.Run("untrusted peer cannot spoof headers", func(t *testing.T) {
		if err := ConfigureClientIP([]string{"192.0.2.10"}, []string{"CF-Connecting-IP", "X-Forwarded-For"}); err != nil {
			t.Fatalf("configure client IP: %v", err)
		}
		req := httptest.NewRequest("GET", "http://example.com", nil)
		req.RemoteAddr = "198.51.100.10:1234"
		req.Header.Set("CF-Connecting-IP", "203.0.113.7")
		req.Header.Set("X-Forwarded-For", "203.0.113.8")

		if got := ClientIP(req); got != "198.51.100.10" {
			t.Fatalf("expected direct peer IP, got %q", got)
		}
	})

	t.Run("trusted peer uses configured header precedence", func(t *testing.T) {
		if err := ConfigureClientIP([]string{"192.0.2.10"}, []string{"CF-Connecting-IP", "X-Forwarded-For"}); err != nil {
			t.Fatalf("configure client IP: %v", err)
		}
		req := httptest.NewRequest("GET", "http://example.com", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("CF-Connecting-IP", "203.0.113.7")
		req.Header.Set("X-Forwarded-For", "203.0.113.8")

		if got := ClientIP(req); got != "203.0.113.7" {
			t.Fatalf("expected Cloudflare client IP, got %q", got)
		}
	})

	t.Run("forwarded chain stops at first untrusted hop", func(t *testing.T) {
		if err := ConfigureClientIP([]string{"192.0.2.0/24"}, []string{"X-Forwarded-For"}); err != nil {
			t.Fatalf("configure client IP: %v", err)
		}
		req := httptest.NewRequest("GET", "http://example.com", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("X-Forwarded-For", "198.51.100.9, 203.0.113.6, 192.0.2.20")

		if got := ClientIP(req); got != "203.0.113.6" {
			t.Fatalf("expected first untrusted hop, got %q", got)
		}
	})

	t.Run("resolved context value is canonical", func(t *testing.T) {
		if err := ConfigureClientIP(nil, nil); err != nil {
			t.Fatalf("configure client IP: %v", err)
		}
		req := httptest.NewRequest("GET", "http://example.com", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req = req.WithContext(context.WithValue(req.Context(), conf.ClientIPKey, "203.0.113.20"))

		if got := ClientIP(req); got != "203.0.113.20" {
			t.Fatalf("expected context client IP, got %q", got)
		}
	})
}

func TestConfigureClientIPRejectsInvalidProxy(t *testing.T) {
	if err := ConfigureClientIP([]string{"not-a-network"}, []string{"X-Forwarded-For"}); err == nil {
		t.Fatal("expected invalid trusted proxy to be rejected")
	}
}
