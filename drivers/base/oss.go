package base

import (
	"net/http"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// OSS uploads interleave file parts with API requests to create/complete uploads.
func NewOSSClientFor(d StorageProvider, endpoint, accessKeyID, accessKeySecret string, options ...oss.ClientOption) (*oss.Client, error) {
	client := RouteClient(d, TransferClientFor(d), func(r *http.Request) bool {
		return r.Method == http.MethodPut && (r.URL.Query().Get("uploadId") == "" || r.URL.Query().Get("partNumber") != "") ||
			r.Method == http.MethodGet && r.URL.Path != "/" && r.URL.Query().Get("uploadId") == ""
	})
	options = append(options, oss.HTTPClient(client))
	return oss.New(endpoint, accessKeyID, accessKeySecret, options...)
}
