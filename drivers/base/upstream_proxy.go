package base

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/go-resty/resty/v2"
	restyv3 "resty.dev/v3"
)

type StorageProvider interface{ GetStorage() *model.Storage }

// StorageClients has separate transports for control requests and file content.
// Authentication state may be shared by a driver's own clients, never globally.
type StorageClients struct {
	API, Transfer                    *http.Client
	Resty, TransferResty, NoRedirect *resty.Client
	apiPolicy, transferPolicy        model.ProxyPolicy
}

var storageClientsMu sync.Mutex

func ClientsFor(d StorageProvider) *StorageClients {
	storageClientsMu.Lock()
	defer storageClientsMu.Unlock()
	s := d.GetStorage()
	if clients, ok := s.NetworkClients.(*StorageClients); ok {
		return clients
	}
	c := &StorageClients{apiPolicy: s.APIProxyPolicy(), transferPolicy: s.TransferProxyPolicy()}
	c.API = net.NewHttpClient()
	c.API.Transport = net.TransportWithProxy(c.API.Transport, c.apiPolicy)
	c.Transfer = net.NewHttpClient()
	c.Transfer.Transport = net.TransportWithProxy(c.Transfer.Transport, c.transferPolicy)
	c.Resty = newPolicyRestyClient(c.apiPolicy)
	c.TransferResty = newPolicyRestyClient(c.transferPolicy)
	c.NoRedirect = newPolicyRestyClient(c.apiPolicy).SetRedirectPolicy(resty.NoRedirectPolicy())
	s.NetworkClients = c
	return c
}

func ValidateStorageProxyPolicies(d StorageProvider) error {
	s := d.GetStorage()
	if _, err := net.ProxyForPolicy(s.APIProxyPolicy()); err != nil {
		return fmt.Errorf("API proxy: %w", err)
	}
	if _, err := net.ProxyForPolicy(s.TransferProxyPolicy()); err != nil {
		return fmt.Errorf("file transfer proxy: %w", err)
	}
	return nil
}

func newPolicyRestyClient(policy model.ProxyPolicy) *resty.Client {
	client := net.NewHttpClient()
	client.Transport = net.TransportWithProxy(client.Transport, policy)
	return resty.NewWithClient(client).
		SetHeader("user-agent", UserAgent).
		SetRetryCount(3).
		SetRetryResetReaders(true).
		SetTimeout(DefaultTimeout)
}

func RestyFor(d StorageProvider) *resty.Client         { return ClientsFor(d).Resty }
func TransferRestyFor(d StorageProvider) *resty.Client { return ClientsFor(d).TransferResty }
func NoRedirectFor(d StorageProvider) *resty.Client    { return ClientsFor(d).NoRedirect }
func APIClientFor(d StorageProvider) *http.Client      { return ClientsFor(d).API }
func TransferClientFor(d StorageProvider) *http.Client { return ClientsFor(d).Transfer }
func NewRestyFor(d StorageProvider) *resty.Client {
	return newPolicyRestyClient(ClientsFor(d).apiPolicy)
}
func NewTransferRestyFor(d StorageProvider) *resty.Client {
	return newPolicyRestyClient(ClientsFor(d).transferPolicy)
}

func NewRestyV3For(d StorageProvider) *restyv3.Client {
	client := net.NewHttpClient()
	client.Transport = net.TransportWithProxy(client.Transport, ClientsFor(d).apiPolicy)
	return restyv3.NewWithClient(client).SetTimeout(DefaultTimeout)
}

// UseTransfer is called after a file request is composed, preserving cancellation.
func UseTransfer(d StorageProvider, r *resty.Request) *resty.Request {
	return r.SetContext(net.WithHTTPClient(r.Context(), TransferClientFor(d)))
}

// RouteClient adapts SDKs which use a single client for both API and file requests.
// The classifier belongs to the driver and describes that SDK's wire operations.
func RouteClient(d StorageProvider, original *http.Client, isTransfer func(*http.Request) bool) *http.Client {
	if original == nil {
		original = net.NewHttpClient()
	}
	client := *original
	c := ClientsFor(d)
	client.Transport = &classifiedTransport{
		api:        net.TransportWithProxy(original.Transport, c.apiPolicy),
		transfer:   net.TransportWithProxy(original.Transport, c.transferPolicy),
		isTransfer: isTransfer,
	}
	return &client
}

type classifiedTransport struct {
	api, transfer http.RoundTripper
	isTransfer    func(*http.Request) bool
}

func (t *classifiedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.isTransfer != nil && t.isTransfer(r) {
		return t.transfer.RoundTrip(r)
	}
	return t.api.RoundTrip(r)
}

func (t *classifiedTransport) CloseIdleConnections() {
	for _, transport := range []http.RoundTripper{t.api, t.transfer} {
		if c, ok := transport.(interface{ CloseIdleConnections() }); ok {
			c.CloseIdleConnections()
		}
	}
}

func CloseStorageClients(d StorageProvider) {
	storageClientsMu.Lock()
	defer storageClientsMu.Unlock()
	if c, ok := d.GetStorage().NetworkClients.(*StorageClients); ok {
		for _, client := range []*http.Client{c.API, c.Transfer, c.Resty.GetClient(), c.TransferResty.GetClient(), c.NoRedirect.GetClient()} {
			client.CloseIdleConnections()
		}
	}
}
