package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/OpenListTeam/OpenList/v4/drivers/local"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/server"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const hiddenPathAdminToken = "hidden-path-test-admin-token"

type hiddenPathFixture struct {
	router http.Handler
	root   string
	tokens map[string]string
	users  map[string]*model.User
}

func newHiddenPathFixture(t *testing.T) *hiddenPathFixture {
	t.Helper()
	oldConfig, oldURL, oldLoaded := conf.Conf, conf.URL, conf.StoragesLoaded
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.DistDir = t.TempDir()
	conf.URL = &url.URL{}
	conf.StoragesLoaded = true
	op.Cache.ClearAll()
	t.Cleanup(func() {
		op.Cache.ClearAll()
		conf.Conf, conf.URL, conf.StoragesLoaded = oldConfig, oldURL, oldLoaded
	})
	if err := os.WriteFile(filepath.Join(conf.Conf.DistDir, "index.html"), []byte("<html></html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	testDB, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	db.Init(testDB)
	t.Cleanup(db.Close)
	if err := op.SaveSettingItems([]model.SettingItem{
		{Key: conf.Token, Value: hiddenPathAdminToken},
		{Key: conf.IPDownloadRPS, Value: "0"},
		{Key: conf.IPListRPS, Value: "0"},
		{Key: conf.UserDefaultDownloadRPS, Value: "0"},
		{Key: conf.UserDefaultListRPS, Value: "0"},
	}); err != nil {
		t.Fatal(err)
	}
	users := map[string]*model.User{}
	for _, user := range []*model.User{
		{Username: "hidden-path-admin", Role: model.ADMIN, Permission: 3, BasePath: "/"},
		{Username: "hidden-path-guest", Role: model.GUEST, BasePath: "/"},
		{Username: "member", Role: model.GENERAL, BasePath: "/"},
		{Username: "viewer", Role: model.GENERAL, Permission: 1, BasePath: "/"},
		{Username: "passwordless", Role: model.GENERAL, Permission: 2, BasePath: "/"},
	} {
		if err := op.CreateUser(user); err != nil {
			t.Fatal(err)
		}
		// Reset the role cache via the same public operation used by user updates.
		if err := op.UpdateUser(user); err != nil {
			t.Fatal(err)
		}
		users[user.Username] = user
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "hidden"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hidden.txt", "public.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("fixture "+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	addition, err := json.Marshal(map[string]string{"root_folder_path": root})
	if err != nil {
		t.Fatal(err)
	}
	storageID, err := op.CreateStorage(context.Background(), model.Storage{
		MountPath: "/hidden-path-tests", Driver: "Local", Addition: string(addition),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := op.DeleteStorageById(context.Background(), storageID); err != nil {
			t.Error(err)
		}
	})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	server.Init(router)
	f := &hiddenPathFixture{router: router, root: root, users: users, tokens: map[string]string{}}
	for _, name := range []string{"member", "viewer", "passwordless"} {
		token, err := common.GenerateToken(users[name])
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[name] = token
	}
	return f
}

func (f *hiddenPathFixture) post(t *testing.T, endpoint, token string, body any, code int) *httptest.ResponseRecorder {
	t.Helper()
	return f.request(t, http.MethodPost, endpoint, token, body, code)
}

func (f *hiddenPathFixture) request(t *testing.T, method, endpoint, token string, body any, code int) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(method, endpoint, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	var resp struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: invalid response: %s (%v)", endpoint, rec.Body, err)
	}
	if rec.Code != http.StatusOK || resp.Code != code {
		t.Fatalf("%s: HTTP %d, response %s; want HTTP 200 and code %d", endpoint, rec.Code, rec.Body, code)
	}
	return rec
}

func (f *hiddenPathFixture) missing(t *testing.T, endpoint, token string, body any) {
	t.Helper()
	rec := f.post(t, endpoint, token, body, 404)
	if got, want := rec.Body.String(), `{"code":404,"message":"object not found","data":null}`; got != want {
		t.Fatalf("%s: got %s, want %s", endpoint, got, want)
	}
}

func (f *hiddenPathFixture) writeFile(t *testing.T, name string) {
	t.Helper()
	target := filepath.Join(f.root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("fixture "+name), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *hiddenPathFixture) saveMeta(t *testing.T, meta model.Meta) {
	t.Helper()
	list := f.request(t, http.MethodGet, "/api/admin/meta/list?page=1&per_page=100", hiddenPathAdminToken, nil, 200)
	var resp struct {
		Data struct {
			Content []model.Meta `json:"content"`
		} `json:"data"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	for _, old := range resp.Data.Content {
		if old.Path == meta.Path {
			meta.ID = old.ID
			f.post(t, "/api/admin/meta/update", hiddenPathAdminToken, meta, 200)
			return
		}
	}
	f.post(t, "/api/admin/meta/create", hiddenPathAdminToken, meta, 200)
}

func TestHiddenPathsLookMissing(t *testing.T) {
	f := newHiddenPathFixture(t)
	t.Run("paths without metadata remain visible", func(t *testing.T) {
		f.post(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/public.txt"}, 200)
		for _, endpoint := range []string{"get", "list", "dirs"} {
			f.post(t, "/api/fs/"+endpoint, "", map[string]string{"path": "/hidden-path-tests"}, 200)
		}
	})
	f.saveMeta(t, model.Meta{Path: "/hidden-path-tests", Hide: `^hidden(\.txt)?$`})
	t.Run("guest file details match missing file", func(t *testing.T) {
		f.post(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/public.txt"}, 200)
		hidden := f.post(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/hidden.txt"}, 404)
		missing := f.post(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/missing.txt"}, 404)
		want := `{"code":404,"message":"object not found","data":null}`
		if hidden.Body.String() != want || missing.Body.String() != want {
			t.Fatalf("hidden = %s, missing = %s; want %s", hidden.Body, missing.Body, want)
		}
	})
	t.Run("guest directory listing matches missing directory", func(t *testing.T) {
		hidden := f.post(t, "/api/fs/list", "", map[string]string{"path": "/hidden-path-tests/hidden"}, 404)
		missing := f.post(t, "/api/fs/list", "", map[string]string{"path": "/hidden-path-tests/missing"}, 404)
		want := `{"code":404,"message":"object not found","data":null}`
		if hidden.Body.String() != want || missing.Body.String() != want {
			t.Fatalf("hidden = %s, missing = %s; want %s", hidden.Body, missing.Body, want)
		}
	})
	t.Run("directory picker matches missing directory", func(t *testing.T) {
		hidden := f.post(t, "/api/fs/dirs", "", map[string]string{"path": "/hidden-path-tests/hidden"}, 404)
		missing := f.post(t, "/api/fs/dirs", "", map[string]string{"path": "/hidden-path-tests/missing"}, 404)
		want := `{"code":404,"message":"object not found","data":null}`
		if hidden.Body.String() != want || missing.Body.String() != want {
			t.Fatalf("hidden = %s, missing = %s; want %s", hidden.Body, missing.Body, want)
		}
	})
	t.Run("directory details and wrapped missing errors", func(t *testing.T) {
		f.missing(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/hidden"})
		for _, endpoint := range []string{"get", "list", "dirs"} {
			for _, path := range []string{"/hidden-path-tests/missing/deep/item", "/unmounted-hidden-path-tests/item"} {
				t.Run(endpoint+path, func(t *testing.T) {
					f.missing(t, "/api/fs/"+endpoint, "", map[string]string{"path": path})
				})
			}
		}
	})
	t.Run("parent listings omit hidden entries", func(t *testing.T) {
		f.writeFile(t, "public-dir/item.txt")
		list := f.post(t, "/api/fs/list", "", map[string]string{"path": "/hidden-path-tests"}, 200)
		var resp struct {
			Data struct {
				Content []struct{ Name string } `json:"content"`
				Total   int                     `json:"total"`
			} `json:"data"`
		}
		if err := json.Unmarshal(list.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, obj := range resp.Data.Content {
			names[obj.Name] = true
		}
		if resp.Data.Total != 2 || len(names) != 2 || !names["public.txt"] || !names["public-dir"] {
			t.Fatalf("unexpected public listing: %s", list.Body)
		}
		dirs := f.post(t, "/api/fs/dirs", "", map[string]string{"path": "/hidden-path-tests"}, 200)
		var dirResp struct {
			Data []struct{ Name string } `json:"data"`
		}
		if err := json.Unmarshal(dirs.Body.Bytes(), &dirResp); err != nil {
			t.Fatal(err)
		}
		if len(dirResp.Data) != 1 || dirResp.Data[0].Name != "public-dir" {
			t.Fatalf("unexpected public directories: %s", dirs.Body)
		}
	})
	t.Run("user permissions keep their meaning", func(t *testing.T) {
		for _, name := range []string{"member", "passwordless"} {
			for _, endpoint := range []string{"get", "list", "dirs"} {
				t.Run(name+"/"+endpoint, func(t *testing.T) {
					f.missing(t, "/api/fs/"+endpoint, f.tokens[name], map[string]string{"path": "/hidden-path-tests/hidden"})
				})
			}
		}
		for _, endpoint := range []string{"get", "list", "dirs"} {
			f.post(t, "/api/fs/"+endpoint, f.tokens["viewer"], map[string]string{"path": "/hidden-path-tests/hidden"}, 200)
		}
		f.post(t, "/api/fs/get", f.tokens["viewer"], map[string]string{"path": "/hidden-path-tests/hidden.txt"}, 200)
	})
	t.Run("wildcard hides files and subdirectories without a password prompt", func(t *testing.T) {
		f.writeFile(t, "collection/录音 [测试].m4a")
		f.writeFile(t, "collection/sub/item.txt")
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/collection", Hide: ".+", HSub: true})
		for _, path := range []string{"/hidden-path-tests/collection/录音 [测试].m4a", "/hidden-path-tests/collection/sub/item.txt"} {
			f.missing(t, "/api/fs/get", "", map[string]string{"path": path})
		}
		for _, endpoint := range []string{"get", "list", "dirs"} {
			f.missing(t, "/api/fs/"+endpoint, "", map[string]string{"path": "/hidden-path-tests/collection/sub"})
		}
		// Metadata on a directory describes its children; it does not hide itself.
		f.post(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/collection"}, 200)
		list := f.post(t, "/api/fs/list", "", map[string]string{"path": "/hidden-path-tests/collection"}, 200)
		var resp struct {
			Data struct {
				Content []json.RawMessage `json:"content"`
				Total   int               `json:"total"`
			} `json:"data"`
		}
		if err := json.Unmarshal(list.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Data.Content) != 0 || resp.Data.Total != 0 {
			t.Fatalf("hidden entries leaked: %s", list.Body)
		}
		query := url.Values{"path": {"/hidden-path-tests/collection/录音 [测试].m4a"}}
		get := f.request(t, http.MethodGet, "/api/fs/get?"+query.Encode(), "", nil, 404)
		if get.Body.String() != `{"code":404,"message":"object not found","data":null}` {
			t.Fatalf("query parameters disclosed a different result: %s", get.Body)
		}
	})
	t.Run("hidden objects take precedence over the password prompt", func(t *testing.T) {
		f.writeFile(t, "locked/private/item.txt")
		f.writeFile(t, "locked/private.txt")
		f.writeFile(t, "locked/public.txt")
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/locked", Hide: `^private(\.txt)?$`, Password: "correct", PSub: true})
		for _, password := range []string{"", "wrong", "correct"} {
			t.Run("password="+password, func(t *testing.T) {
				f.missing(t, "/api/fs/get", "", map[string]string{"path": "/hidden-path-tests/locked/private.txt", "password": password})
				for _, endpoint := range []string{"get", "list", "dirs"} {
					f.missing(t, "/api/fs/"+endpoint, "", map[string]string{"path": "/hidden-path-tests/locked/private", "password": password})
				}
			})
		}
		for _, endpoint := range []string{"get", "list", "dirs"} {
			for _, token := range []string{"", f.tokens["viewer"]} {
				f.post(t, "/api/fs/"+endpoint, token, map[string]string{"path": "/hidden-path-tests/locked", "password": "wrong"}, 403)
				f.post(t, "/api/fs/"+endpoint, token, map[string]string{"path": "/hidden-path-tests/locked", "password": "correct"}, 200)
			}
		}
		f.post(t, "/api/fs/get", f.tokens["viewer"], map[string]string{"path": "/hidden-path-tests/locked/private.txt", "password": "correct"}, 200)
	})
	t.Run("read user restrictions still deny access", func(t *testing.T) {
		f.writeFile(t, "restricted/item.txt")
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/restricted", ReadUsers: []uint{f.users["hidden-path-admin"].ID}, ReadUsersSub: true})
		for _, endpoint := range []string{"get", "list", "dirs"} {
			f.post(t, "/api/fs/"+endpoint, "", map[string]string{"path": "/hidden-path-tests/restricted"}, 403)
			f.post(t, "/api/fs/"+endpoint, f.tokens["viewer"], map[string]string{"path": "/hidden-path-tests/restricted"}, 403)
			f.post(t, "/api/fs/"+endpoint, hiddenPathAdminToken, map[string]string{"path": "/hidden-path-tests/restricted"}, 200)
		}
	})
	t.Run("metadata updates restore visibility without stale responses", func(t *testing.T) {
		f.writeFile(t, "changing/target.txt")
		body := map[string]string{"path": "/hidden-path-tests/changing/target.txt"}
		f.post(t, "/api/fs/get", "", body, 200)
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/changing", Hide: `^target\.txt$`})
		f.missing(t, "/api/fs/get", "", body)
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/changing", Hide: ""})
		f.post(t, "/api/fs/get", "", body, 200)
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/changing", Hide: `^different$`})
		f.post(t, "/api/fs/get", "", body, 200)
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/changing", Hide: ".+"})
		f.missing(t, "/api/fs/get", "", body)
	})
	t.Run("hide inheritance remains controlled by existing metadata", func(t *testing.T) {
		f.writeFile(t, "inherit/sub/item.txt")
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/inherit", Hide: ".+", HSub: false})
		body := map[string]string{"path": "/hidden-path-tests/inherit/sub/item.txt"}
		f.post(t, "/api/fs/get", "", body, 200)
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/inherit", Hide: ".+", HSub: true})
		f.missing(t, "/api/fs/get", "", body)
		f.post(t, "/api/fs/get", f.tokens["viewer"], body, 200)
		f.saveMeta(t, model.Meta{Path: "/hidden-path-tests/inherit/sub", Readme: "independent metadata"})
		f.post(t, "/api/fs/get", "", body, 200)
	})
	t.Run("non missing errors retain their server error response", func(t *testing.T) {
		for _, endpoint := range []string{"list", "dirs"} {
			f.post(t, "/api/fs/"+endpoint, "", map[string]string{"path": "/hidden-path-tests/public.txt"}, 500)
		}
	})
}
