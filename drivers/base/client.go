package base

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/go-resty/resty/v2"
)

var (
	NoRedirectClient *resty.Client
	RestyClient      *resty.Client
	HttpClient       *http.Client
)

var DefaultTimeout = time.Second * 30

// Shared user agents are configured at startup, before storage initialization.
var UserAgent = conf.DefaultUserAgent
var UserAgentNT = conf.DefaultUserAgentNT

func InitClient() {
	UserAgent = conf.Conf.UserAgent
	if UserAgent == "" {
		UserAgent = conf.DefaultUserAgent
	}
	UserAgentNT = conf.Conf.UserAgentNT
	if UserAgentNT == "" {
		UserAgentNT = conf.DefaultUserAgentNT
	}

	NoRedirectClient = resty.New().SetRedirectPolicy(
		resty.RedirectPolicyFunc(func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}),
	).SetTLSClientConfig(&tls.Config{InsecureSkipVerify: conf.Conf.TlsInsecureSkipVerify})
	NoRedirectClient.SetHeader("user-agent", UserAgent)
	net.SetRestyProxyIfConfigured(NoRedirectClient)

	RestyClient = NewRestyClient()
	HttpClient = net.NewHttpClient()
}

func NewRestyClient() *resty.Client {
	client := resty.New().
		SetHeader("user-agent", UserAgent).
		SetRetryCount(3).
		SetRetryResetReaders(true).
		SetTimeout(DefaultTimeout).
		SetTLSClientConfig(&tls.Config{InsecureSkipVerify: conf.Conf.TlsInsecureSkipVerify})

	net.SetRestyProxyIfConfigured(client)
	return client
}
