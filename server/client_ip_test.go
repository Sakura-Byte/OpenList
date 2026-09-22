package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/middlewares"
	"github.com/gin-gonic/gin"
)

func TestConfiguredClientIPMiddleware(t *testing.T) {
	previous := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.TrustedProxies = []string{"192.0.2.10"}
	conf.Conf.TrustedProxyHeaders = []string{"CF-Connecting-IP", "X-Forwarded-For"}
	t.Cleanup(func() {
		conf.Conf = previous
		if err := utils.ConfigureClientIP(nil, nil); err != nil {
			t.Fatalf("reset client IP config: %v", err)
		}
	})

	gin.SetMode(gin.TestMode)
	router := gin.New()
	configureClientIP(router)
	router.Use(middlewares.ClientIPContext)
	router.GET("/ip", func(c *gin.Context) {
		c.String(http.StatusOK, utils.ClientIP(c.Request))
	})

	for _, test := range []struct {
		name       string
		remoteAddr string
		headers    map[string]string
		want       string
	}{
		{
			name:       "trusted proxy",
			remoteAddr: "192.0.2.10:1234",
			headers: map[string]string{
				"CF-Connecting-IP": "203.0.113.7",
				"X-Forwarded-For":  "198.51.100.8",
			},
			want: "203.0.113.7",
		},
		{
			name:       "untrusted peer",
			remoteAddr: "198.51.100.10:1234",
			headers: map[string]string{
				"CF-Connecting-IP": "203.0.113.7",
				"X-Forwarded-For":  "203.0.113.8",
			},
			want: "198.51.100.10",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ip", nil)
			req.RemoteAddr = test.remoteAddr
			for name, value := range test.headers {
				req.Header.Set(name, value)
			}
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)
			if resp.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d", resp.Code)
			}
			if got := resp.Body.String(); got != test.want {
				t.Fatalf("expected client IP %q, got %q", test.want, got)
			}
		})
	}
}
