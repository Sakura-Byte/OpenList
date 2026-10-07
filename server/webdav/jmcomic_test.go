package webdav

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	_ "github.com/OpenListTeam/OpenList/v4/drivers/jmcomic"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestJMComicWebDAVModesAndCachedRecursiveScan(t *testing.T) {
	var imageBytes bytes.Buffer
	require.NoError(t, png.Encode(&imageBytes, image.NewNRGBA(image.Rect(0, 0, 3, 3))))
	var media, metadata atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/page.png" {
			media.Add(1)
			w.Header().Set("Content-Type", "image/png")
			w.Write(imageBytes.Bytes())
			return
		}
		metadata.Add(1)
		w.WriteHeader(404)
	}))
	defer source.Close()
	conf.Conf = conf.DefaultConfig(t.TempDir())
	base.InitClient()
	op.Cache.ClearAll()
	database, e := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())), &gorm.Config{})
	require.NoError(t, e)
	db.Init(database)
	cacheRoot := t.TempDir()
	sum := sha256.Sum256([]byte(source.URL))
	filename := filepath.Join(cacheRoot, "91", fmt.Sprintf("%x", sum)[:16], "cache.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(filename), 0700))
	cache, e := gorm.Open(sqlite.Open(filename), &gorm.Config{})
	require.NoError(t, e)
	require.NoError(t, cache.Exec(`CREATE TABLE state(id INTEGER PRIMARY KEY,data TEXT NOT NULL)`).Error)
	require.NoError(t, cache.Exec(`INSERT INTO state(id,data) VALUES(1,?)`, `{"paused":true}`).Error)
	conn, _ := cache.DB()
	require.NoError(t, conn.Close())
	addition := func(mode string) string {
		b, _ := json.Marshal(map[string]any{"api_url": source.URL, "cache_root": cacheRoot, "mode": mode})
		return string(b)
	}
	id, e := op.CreateStorage(context.Background(), model.Storage{ID: 91, Driver: "JMComic", MountPath: "/jm-dav", Addition: addition("client"), APIProxyMode: model.ProxyDisabled, TransferProxyMode: model.ProxyDisabled})
	require.NoError(t, e)
	t.Cleanup(func() { _ = op.DeleteStorageById(context.Background(), id); c, _ := database.DB(); c.Close() })
	cache, e = gorm.Open(sqlite.Open(filename), &gorm.Config{})
	require.NoError(t, e)
	conn, _ = cache.DB()
	defer conn.Close()
	albumPath := "/所有漫画/2026/07/21/JM42 Synthetic"
	chapterPath := albumPath + "/001 Chapter [42]"
	require.NoError(t, cache.Exec(`INSERT INTO albums(id,data,path,date,updated,detail,detail_version) VALUES(?,?,?,?,?,?,?)`, "42", `{"id":"42","name":"Synthetic","adddate":"2026-07-21","update_at":1}`, albumPath, "2026-07-21", 1, `{"id":"42","name":"Synthetic","series":[{"id":"42","name":"Chapter","sort":"1"}]}`, 1).Error)
	tree, _ := json.Marshal(map[string]any{"id": "42", "album_id": "42", "version": 1, "pages": []map[string]any{{"name": "0001.png", "url": source.URL + "/page.png", "segments": 0, "gif": false}}})
	require.NoError(t, cache.Exec(`INSERT INTO chapters(id,album_id,data,version) VALUES(?,?,?,?)`, "42", "42", string(tree), 1).Error)
	user := &model.User{ID: 11, Username: "admin", Role: model.ADMIN, BasePath: "/jm-dav"}
	handler := &Handler{Prefix: "/dav", LockSystem: NewMemLS()}
	request := func(method, path string) *http.Request {
		r := httptest.NewRequest(method, (&url.URL{Scheme: "http", Host: "openlist.example", Path: "/dav" + path}).String(), nil)
		return r.WithContext(context.WithValue(r.Context(), conf.UserKey, user))
	}
	for _, method := range []string{"GET", "HEAD"} {
		out := httptest.NewRecorder()
		status, e := handler.handleGetHeadPost(out, request(method, chapterPath+"/0001.png"))
		require.Equal(t, 501, status)
		require.ErrorContains(t, e, "client mode")
	}
	out := httptest.NewRecorder()
	status, e := handler.handleGetHeadPost(out, request("GET", chapterPath+"/README.md"))
	require.NoError(t, e)
	require.Zero(t, status)
	require.Contains(t, out.Body.String(), "Synthetic")
	require.Zero(t, media.Load())
	for _, depth := range []string{"1", "infinity"} {
		r := request("PROPFIND", "/")
		r.Header.Set("Depth", depth)
		out := httptest.NewRecorder()
		status, e := handler.handlePropfind(out, r)
		require.NoError(t, e)
		require.Zero(t, status)
		require.Equal(t, 207, out.Code)
	}
	require.Zero(t, metadata.Load())
	storage, e := db.GetStorageById(id)
	require.NoError(t, e)
	storage.Addition = addition("server")
	storage.WebdavPolicy = "302_redirect"
	require.NoError(t, op.UpdateStorage(context.Background(), *storage))
	require.Eventually(t, func() bool {
		d, e := op.GetStorageByMountPath("/jm-dav")
		return e == nil && d.GetStorage().Status == op.WORK
	}, 3*time.Second, 10*time.Millisecond)
	for _, method := range []string{"GET", "HEAD"} {
		out := httptest.NewRecorder()
		r := request(method, chapterPath+"/0001.png")
		r.Header.Set("Range", "bytes=0-7")
		status, e := handler.handleGetHeadPost(out, r)
		require.NoError(t, e)
		require.Zero(t, status)
		require.Empty(t, out.Header().Get("Location"))
		if method == "GET" {
			require.Equal(t, 206, out.Code)
			require.True(t, strings.HasPrefix(out.Body.String(), "\x89PNG"))
		} else {
			require.Contains(t, []int{200, 206}, out.Code)
			require.Empty(t, out.Body.Bytes())
		}
	}
	require.EqualValues(t, 2, media.Load())
	require.Zero(t, metadata.Load())
}
