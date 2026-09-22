package utils

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

type clientIPConfig struct {
	trustedProxies []netip.Prefix
	headers        []string
}

var clientIPSettings atomic.Pointer[clientIPConfig]

func init() {
	clientIPSettings.Store(&clientIPConfig{})
}

// ConfigureClientIP configures which direct proxies may supply client IP
// headers. An empty trustedProxies list disables proxy header processing.
func ConfigureClientIP(trustedProxies, headers []string) error {
	config := &clientIPConfig{
		trustedProxies: make([]netip.Prefix, 0, len(trustedProxies)),
		headers:        make([]string, 0, len(headers)),
	}
	for _, proxy := range trustedProxies {
		proxy = strings.TrimSpace(proxy)
		if proxy == "" {
			continue
		}
		prefix, err := parseIPPrefix(proxy)
		if err != nil {
			return err
		}
		config.trustedProxies = append(config.trustedProxies, prefix)
	}
	for _, header := range headers {
		header = strings.TrimSpace(header)
		if header != "" {
			config.headers = append(config.headers, header)
		}
	}
	clientIPSettings.Store(config)
	return nil
}

func ClientIP(r *http.Request) string {
	if ip, ok := r.Context().Value(conf.ClientIPKey).(string); ok {
		if addr, valid := parseIP(ip); valid {
			return addr.String()
		}
	}

	remoteIP, ok := parseIP(r.RemoteAddr)
	if !ok {
		return ""
	}
	config := clientIPSettings.Load()
	if config == nil || !config.isTrustedProxy(remoteIP) {
		return remoteIP.String()
	}
	for _, header := range config.headers {
		if ip, valid := config.validateHeader(r.Header.Get(header)); valid {
			return ip.String()
		}
	}
	return remoteIP.String()
}

func parseIPPrefix(value string) (netip.Prefix, error) {
	if strings.Contains(value, "/") {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return netip.Prefix{}, err
		}
		return prefix.Masked(), nil
	}
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return netip.Prefix{}, err
	}
	addr = addr.Unmap()
	return netip.PrefixFrom(addr, addr.BitLen()), nil
}

func parseIP(value string) (netip.Addr, bool) {
	value = strings.TrimSpace(value)
	if addr, err := netip.ParseAddr(value); err == nil {
		return addr.Unmap(), true
	}
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		return netip.Addr{}, false
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func (c *clientIPConfig) isTrustedProxy(ip netip.Addr) bool {
	for _, proxy := range c.trustedProxies {
		if proxy.Contains(ip) {
			return true
		}
	}
	return false
}

func (c *clientIPConfig) validateHeader(value string) (netip.Addr, bool) {
	parts := strings.Split(value, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		ip, ok := parseIP(parts[i])
		if !ok {
			return netip.Addr{}, false
		}
		if i == 0 || !c.isTrustedProxy(ip) {
			return ip, true
		}
	}

	return netip.Addr{}, false
}

func IsLocalIPAddr(ip string) bool {
	return IsLocalIP(net.ParseIP(ip))
}

func IsLocalIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}

	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}

	return ip4[0] == 10 || // 10.0.0.0/8
		(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31) || // 172.16.0.0/12
		(ip4[0] == 169 && ip4[1] == 254) || // 169.254.0.0/16
		(ip4[0] == 192 && ip4[1] == 168) // 192.168.0.0/16
}
