package net

import (
	"net/http"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

func NewOSSClient(endpoint, accessKeyID, accessKeySecret string, options ...oss.ClientOption) (*oss.Client, error) {
	proxy, err := ProxyForPolicy(model.ProxyPolicy{Mode: model.ProxySystem})
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{Proxy: proxy}
	clientOptions := []oss.ClientOption{oss.HTTPClient(&http.Client{Transport: transport, Timeout: 48 * time.Hour})}
	clientOptions = append(clientOptions, options...)
	return oss.New(endpoint, accessKeyID, accessKeySecret, clientOptions...)
}
