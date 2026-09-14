package alist_v3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/go-resty/resty/v2"
)

func TestMountedSubdirectoryPaths(t *testing.T) {
	oldClient := base.RestyClient
	base.RestyClient = resty.New()
	t.Cleanup(func() {
		base.RestyClient.GetClient().CloseIdleConnections()
		base.RestyClient = oldClient
	})
	for _, root := range []string{"/", "/asmr"} {
		t.Run(root, func(t *testing.T) {
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var req FsGetReq
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request: %v", err)
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				requests = append(requests, req.Path)
				switch r.URL.Path {
				case "/api/fs/list":
					name, isDir := "中文音声", true
					switch req.Path {
					case path.Join(root, "中文音声"):
						name = "子目录"
					case path.Join(root, "中文音声/子目录"):
						name, isDir = "sample.mp3", false
					}
					// Empty paths reproduce upstream's fallback to its root listing.
					_, _ = fmt.Fprintf(w, `{"code":200,"data":{"content":[{"name":%q,"is_dir":%t,"hashinfo":"null"}]}}`, name, isDir)
				case "/api/fs/get":
					_, _ = w.Write([]byte(`{"code":200,"data":{"raw_url":"https://example.com/sample.mp3"}}`))
				default:
					t.Errorf("unexpected endpoint: %s", r.URL.Path)
				}
			}))
			defer server.Close()
			d := &AListV3{
				Storage:  model.Storage{MountPath: "/test-alist" + root, CacheExpiration: 30},
				Addition: Addition{Address: server.URL, RootPath: driver.RootPath{RootFolderPath: root}},
			}
			defer op.Cache.DeleteDirectoryTree(d, "/")
			for _, localPath := range []string{"/", "/中文音声", "/中文音声/子目录"} {
				files, err := op.List(context.Background(), d, localPath, model.ListArgs{SkipHook: true})
				if err != nil {
					t.Fatalf("list %q: %v", localPath, err)
				}
				if got, want := requests[len(requests)-1], path.Join(root, localPath); got != want {
					t.Fatalf("list %q sent upstream path %q, want %q (root listing repeats)", localPath, got, want)
				}
				if len(files) != 1 {
					t.Fatalf("list %q returned %d objects, want 1", localPath, len(files))
				}
			}
			_, _, err := op.Link(context.Background(), d, "/中文音声/子目录/sample.mp3", model.LinkArgs{})
			if err != nil {
				t.Fatalf("link: %v", err)
			}
			if got, want := requests[len(requests)-1], path.Join(root, "中文音声/子目录/sample.mp3"); got != want {
				t.Errorf("link sent upstream path %q, want %q", got, want)
			}
		})
	}
}

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
