package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestASMRONEAdminRoutesRejectInvalidRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	admin(router.Group("/api/admin"))
	for _, test := range []struct{ method, path, body string }{{"GET", "/api/admin/storage/asmrone/status?id=invalid", ""}, {"POST", "/api/admin/storage/asmrone/action", `{"action":"pause"}`}} {
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code)
		require.Contains(t, response.Body.String(), `"code":400`)
	}
}
