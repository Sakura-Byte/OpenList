package model

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type ProxyMode string

const (
	ProxyDisabled ProxyMode = "disabled"
	ProxySystem   ProxyMode = "system"
	ProxyManual   ProxyMode = "manual"
)

// ProxyPolicy describes an outbound proxy, independently of download redirects.
type ProxyPolicy struct {
	Mode ProxyMode
	URL  string
}

// String deliberately omits credentials, including for malformed proxy URLs.
func (p ProxyPolicy) String() string { return string(p.Mode) }

func (p ProxyPolicy) Validate() error {
	switch p.Mode {
	case "", ProxyDisabled, ProxySystem:
		return nil
	case ProxyManual:
		_, err := ParseProxyURL(p.URL)
		return err
	default:
		return fmt.Errorf("invalid proxy mode; expected disabled, system or manual")
	}
}

func ParseProxyURL(address string) (*url.URL, error) {
	u, err := url.Parse(address)
	if err != nil || u == nil || u.Hostname() == "" || strings.ContainsAny(address, "\r\n\t ") {
		return nil, fmt.Errorf("invalid proxy address: an absolute proxy URL with a host is required")
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, fmt.Errorf("invalid proxy protocol; expected http, https, socks5 or socks5h")
	}
	if u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.Opaque != "" {
		return nil, fmt.Errorf("proxy address must not contain a path, query or fragment")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("invalid proxy port")
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return nil, fmt.Errorf("invalid proxy port")
	}
	return u, nil
}

func (s *Storage) APIProxyPolicy() ProxyPolicy {
	return ProxyPolicy{Mode: s.APIProxyMode, URL: s.APIProxyURL}
}

func (s *Storage) TransferProxyPolicy() ProxyPolicy {
	return ProxyPolicy{Mode: s.TransferProxyMode, URL: s.TransferProxyURL}
}

func (s *Storage) NormalizeProxy() error {
	if s.APIProxyMode == "" {
		s.APIProxyMode = ProxySystem
	}
	if s.TransferProxyMode == "" {
		s.TransferProxyMode = ProxySystem
	}
	if err := s.APIProxyPolicy().Validate(); err != nil {
		return fmt.Errorf("API proxy: %w", err)
	}
	if err := s.TransferProxyPolicy().Validate(); err != nil {
		return fmt.Errorf("file transfer proxy: %w", err)
	}
	return nil
}
