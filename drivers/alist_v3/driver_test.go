package alist_v3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/go-resty/resty/v2"
)

func TestInitGuestWithoutMountPermissionCheck(t *testing.T) {
	oldClient := base.RestyClient
	base.RestyClient = resty.New()
	t.Cleanup(func() {
		base.RestyClient.GetClient().CloseIdleConnections()
		base.RestyClient = oldClient
	})

	for _, tc := range []struct {
		name string
		role string
	}{
		{name: "numeric guest role", role: fmt.Sprint(model.GUEST)},
		{name: "guest role array", role: fmt.Sprintf("[%d]", model.GUEST)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var settingsRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/me":
					_, _ = fmt.Fprintf(w, `{"code":200,"data":{"username":"guest","role":%s}}`, tc.role)
				case "/api/public/settings":
					settingsRequests.Add(1)
					_, _ = w.Write([]byte(`{"code":200,"data":{"allow_mounted":"false"}}`))
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			d := &AListV3{Addition: Addition{Address: server.URL + "/"}}
			if err := d.Init(context.Background()); err != nil {
				t.Fatalf("guest initialization failed: %v", err)
			}
			if got := settingsRequests.Load(); got != 0 {
				t.Errorf("public settings requests = %d, want 0", got)
			}
		})
	}
}
