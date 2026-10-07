package server

import (
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJMComicAdminRoutesRejectMalformedRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	admin(r.Group("/api/admin"))
	for _, test := range []struct{ method, path, body string }{{"GET", "/api/admin/storage/jmcomic/status?id=invalid", ""}, {"POST", "/api/admin/storage/jmcomic/action", `{"action":"pause"}`}} {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		req.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		r.ServeHTTP(out, req)
		require.Equal(t, http.StatusOK, out.Code)
		require.Contains(t, out.Body.String(), `"code":400`)
	}
}
