package net

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"golang.org/x/net/http/httpproxy"
)

type httpClientContextKey struct{}

func WithHTTPClient(ctx context.Context, client *http.Client) context.Context {
	if client == nil {
		return ctx
	}
	return context.WithValue(ctx, httpClientContextKey{}, client)
}

func HTTPClientFromContext(ctx context.Context) *http.Client { return httpClientForContext(ctx) }

func httpClientForContext(ctx context.Context) *http.Client {
	if c, ok := ctx.Value(httpClientContextKey{}).(*http.Client); ok && c != nil {
		return c
	}
	return HttpClient()
}

// ProxyForPolicy snapshots the system configuration when a storage is initialized.
// Explicit policies never fall back to a direct connection on failure.
func ProxyForPolicy(policy model.ProxyPolicy) (func(*http.Request) (*url.URL, error), error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if policy.Mode == model.ProxyDisabled {
		return nil, nil
	}
	address := policy.URL
	if policy.Mode != model.ProxyManual {
		address = ""
		if conf.Conf != nil {
			address = conf.Conf.ProxyAddress
		}
		if address == "" {
			proxy := httpproxy.FromEnvironment().ProxyFunc()
			return func(r *http.Request) (*url.URL, error) {
				u, err := proxy(r.URL)
				if err != nil {
					return nil, fmt.Errorf("invalid system proxy configuration")
				}
				return u, nil
			}, nil
		}
	}
	u, err := model.ParseProxyURL(address)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream proxy configuration: %w", err)
	}
	return http.ProxyURL(u), nil
}

// ProxyTransport clones the transport before configuring it, preserving TLS and
// connection settings without modifying a client's shared transport.
func ProxyTransport(transport *http.Transport, policy model.ProxyPolicy) (*http.Transport, error) {
	proxy, err := ProxyForPolicy(policy)
	if err != nil {
		return nil, err
	}
	transport = transport.Clone()
	transport.Proxy = func(r *http.Request) (*url.URL, error) {
		if client, ok := r.Context().Value(httpClientContextKey{}).(*http.Client); ok && client != nil {
			request := r.WithContext(context.WithValue(r.Context(), httpClientContextKey{}, (*http.Client)(nil)))
			return proxyURLForTransport(client.Transport, request)
		}
		if proxy == nil {
			return nil, nil
		}
		return proxy(r)
	}
	return transport, nil
}

// proxyErrorTransport keeps invalid configurations fail-closed, including callers
// whose existing client constructors cannot return an error.
type proxyErrorTransport struct{ err error }

func (t proxyErrorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, t.err
}

// TransportWithProxy preserves supported transport wrappers. Unsupported custom
// transports must be adapted by the driver rather than silently bypassing policy.
func TransportWithProxy(transport http.RoundTripper, policy model.ProxyPolicy) http.RoundTripper {
	if transport == nil {
		transport = http.DefaultTransport
	}
	switch t := transport.(type) {
	case *http.Transport:
		result, err := ProxyTransport(t, policy)
		if err != nil {
			return proxyErrorTransport{err}
		}
		return result
	case *safeTransport:
		return &safeTransport{base: TransportWithProxy(t.base, policy)}
	default:
		return proxyErrorTransport{fmt.Errorf("custom HTTP transport requires upstream proxy support")}
	}
}

func (t *safeTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
