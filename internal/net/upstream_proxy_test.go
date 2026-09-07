package net

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestProxyForPolicyModes(t *testing.T) {
	old := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	defer func() { conf.Conf = old }()
	conf.Conf.ProxyAddress = "http://127.0.0.1:18080"

	for _, tt := range []struct {
		name   string
		policy model.ProxyPolicy
		want   string
	}{
		{"disabled", model.ProxyPolicy{Mode: model.ProxyDisabled}, ""},
		{"system configuration", model.ProxyPolicy{Mode: model.ProxySystem}, "http://127.0.0.1:18080"},
		{"manual", model.ProxyPolicy{Mode: model.ProxyManual, URL: "http://proxy.example:8080"}, "http://proxy.example:8080"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proxy, err := ProxyForPolicy(tt.policy)
			if err != nil {
				t.Fatal(err)
			}
			got, err := proxyForTest(proxy)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("proxy = %q, want %q", got, tt.want)
			}
		})
	}
}

func proxyForTest(proxy func(*http.Request) (*url.URL, error)) (string, error) {
	if proxy == nil {
		return "", nil
	}
	u, err := proxy(&http.Request{URL: mustURL("http://upstream.example/file")})
	if err != nil || u == nil {
		return "", err
	}
	return u.String(), nil
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}
