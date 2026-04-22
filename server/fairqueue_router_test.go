package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestFairQueueAdminRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	admin(r.Group("/api/admin"))

	tests := []struct {
		name           string
		path           string
		expectNotFound bool
	}{
		{name: "abandon reachable", path: "/api/admin/fairqueue/abandon"},
		{name: "activate reachable", path: "/api/admin/fairqueue/activate"},
		{name: "cancel removed", path: "/api/admin/fairqueue/cancel", expectNotFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(`{}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if tt.expectNotFound {
				if rec.Code != http.StatusNotFound {
					t.Fatalf("expected 404 for %s, got %d", tt.path, rec.Code)
				}
				return
			}

			if rec.Code == http.StatusNotFound {
				t.Fatalf("expected %s to be registered, got 404", tt.path)
			}
		})
	}
}
