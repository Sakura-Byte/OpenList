package jmcomic

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	conf.Conf = conf.DefaultConfig("data")
	base.InitClient()
	os.Exit(m.Run())
}
func newMockServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/setting" {
			http.SetCookie(w, &http.Cookie{Name: "anonymous", Value: "test", Path: "/"})
			replyJSON(w, r, map[string]any{})
			return
		}
		handler(w, r)
	}))
}
func newTestDriver(t *testing.T, api, root, mode string) *JMComic {
	t.Helper()
	sum := sha256.Sum256([]byte(api))
	filename := filepath.Join(root, "91", fmt.Sprintf("%x", sum)[:16], "cache.db")
	s, e := openStore(filename)
	require.NoError(t, e)
	require.NoError(t, s.saveState(context.Background(), syncState{Paused: true}))
	require.NoError(t, s.db.Close())
	d := &JMComic{Storage: model.Storage{ID: 91, Status: op.WORK, MountPath: "/JMComic", APIProxyMode: model.ProxyDisabled, TransferProxyMode: model.ProxyDisabled}, Addition: Addition{APIURL: api, Mode: mode, CacheRoot: root, RequestsPerSecond: 100}}
	require.NoError(t, d.Init(context.Background()))
	t.Cleanup(func() { require.NoError(t, d.Drop(context.Background())); base.CloseStorageClients(d) })
	return d
}
func replyJSON(w http.ResponseWriter, r *http.Request, v any) {
	ts := strings.Split(r.Header.Get("tokenparam"), ",")[0]
	b, _ := json.Marshal(v)
	n := aes.BlockSize - len(b)%aes.BlockSize
	b = append(b, bytes.Repeat([]byte{byte(n)}, n)...)
	cipher, _ := aes.NewCipher([]byte(digest(ts + appSecret)))
	for i := 0; i < len(b); i += aes.BlockSize {
		cipher.Encrypt(b[i:i+aes.BlockSize], b[i:i+aes.BlockSize])
	}
	json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": base64.StdEncoding.EncodeToString(b)})
}
func seedAlbum(t *testing.T, d *JMComic, key, name, date string, updated int64) cachedAlbum {
	t.Helper()
	a := album{ID: id(key), Name: name, Date: date, Updated: number(updated)}
	require.NoError(t, d.store.upsert(context.Background(), []album{a}, ""))
	v, e := d.store.album(context.Background(), key)
	require.NoError(t, e)
	return v
}
func template(key, host string, files []string) string {
	return fmt.Sprintf(`var config={"imghost":%q,"jmid":%q,"cache":""};var result={"images":%s};var scramble_id=220980;`, host, key, encode(files))
}

func TestClientListingMetadataCacheAndRestart(t *testing.T) {
	var details, trees, images atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/album":
			details.Add(1)
			replyJSON(w, r, detail{ID: "42", Name: "Example", Series: []chapter{{ID: "42", Name: "Chapter", Sort: "1"}}})
		case "/chapter_view_template":
			trees.Add(1)
			io.WriteString(w, template("42", "https://images.example", []string{"00001.webp"}))
		default:
			images.Add(1)
			w.WriteHeader(404)
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	a := seedAlbum(t, d, "42", "Example", "2026-07-21", 1791342087)
	require.NoError(t, op.WalkStorageRecursive(context.Background(), d, "/", -1, model.ListArgs{Refresh: true}, false, nil, func(_ string, _ []model.Obj) error { return nil }))
	require.EqualValues(t, 0, details.Load())
	o, e := d.Get(context.Background(), a.Album.canonical())
	require.NoError(t, e)
	require.Equal(t, "album", model.GetReaderInfo(o).Kind)
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() { _, e := d.List(context.Background(), o, model.ListArgs{Refresh: true}); require.NoError(t, e) })
	}
	wg.Wait()
	require.EqualValues(t, 1, details.Load())
	children, e := d.List(context.Background(), o, model.ListArgs{})
	require.NoError(t, e)
	require.Len(t, children, 2)
	require.Equal(t, "README.md", children[0].GetName())
	ch := children[1]
	list, e := d.List(context.Background(), ch, model.ListArgs{})
	require.NoError(t, e)
	require.Len(t, list, 1)
	require.EqualValues(t, 0, trees.Load())
	m, e := d.Other(context.Background(), model.OtherArgs{Obj: ch, Method: "jm_reader"})
	require.NoError(t, e)
	require.Len(t, m.(*driver.JMReaderManifest).Pages, 1)
	require.EqualValues(t, 1, trees.Load())
	require.EqualValues(t, 0, images.Load())
	require.NoError(t, d.Drop(context.Background()))
	require.NoError(t, d.Init(context.Background()))
	_, e = d.Other(context.Background(), model.OtherArgs{Obj: ch, Method: "jm_reader"})
	require.NoError(t, e)
	require.EqualValues(t, 1, details.Load())
	require.EqualValues(t, 1, trees.Load())
	require.ErrorContains(t, d.CheckContentPath(ch.GetPath()+"/0001.png"), "client mode")
}
func TestEmptyTreeAndFailedRefreshPreserveCache(t *testing.T) {
	var bad atomic.Bool
	var requests atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if bad.Load() {
			w.WriteHeader(404)
			return
		}
		io.WriteString(w, template("42", "https://images.example", []string{}))
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "server")
	a := seedAlbum(t, d, "42", "Empty", "2026-10-07", 1)
	tree, e := d.loadTree(context.Background(), a, "42", false, true)
	require.NoError(t, e)
	require.Empty(t, tree.Pages)
	_, e = d.loadTree(context.Background(), a, "42", false, true)
	require.NoError(t, e)
	require.EqualValues(t, 1, requests.Load())
	bad.Store(true)
	_, e = d.loadTree(context.Background(), a, "42", true, true)
	require.Error(t, e)
	_, e = d.loadTree(context.Background(), a, "42", false, true)
	require.NoError(t, e)
	require.EqualValues(t, 2, requests.Load())
	var retry int64
	require.NoError(t, d.store.db.QueryRow(`SELECT retry_at FROM tasks WHERE kind='chapter' AND key='42/42'`).Scan(&retry))
	require.Greater(t, retry, time.Now().Unix())
}
func TestFullIncrementalRenameAndUnavailableConfirmation(t *testing.T) {
	var phase atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/album" {
			if phase.Load() == 3 {
				replyJSON(w, r, map[string]any{"id": r.URL.Query().Get("id"), "name": "Recovered", "series": []chapter{}})
			} else {
				w.WriteHeader(404)
			}
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		items := []album{}
		switch phase.Load() {
		case 0:
			if page == 1 {
				items = []album{{ID: "7", Name: "Original", Date: "2026-07-21", Updated: 100}, {ID: "8", Name: "Keep", Date: "2026-10-07", Updated: 100}}
			}
		case 1:
			if page == 1 {
				items = []album{{ID: "7", Name: "Renamed", Date: "2026-07-21", Updated: 101}, {ID: "9", Name: "Same day", Date: "2026-10-07", Updated: 101}}
			}
		case 2:
			if page == 1 || page == 2 {
				items = []album{{ID: "8", Name: "Keep", Date: "2026-10-07", Updated: 102}}
			}
		case 3:
			if page == 1 {
				items = []album{{ID: "8", Name: "Keep", Date: "2026-10-07", Updated: 103}}
			}
		}
		replyJSON(w, r, map[string]any{"total": 1, "content": items})
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	watermark, e := d.syncCatalog(context.Background(), true, 0)
	require.NoError(t, e)
	require.EqualValues(t, 100, watermark)
	a, e := d.store.album(context.Background(), "7")
	require.NoError(t, e)
	old := a.Album.canonical()
	phase.Store(1)
	_, e = d.syncCatalog(context.Background(), false, watermark)
	require.NoError(t, e)
	a, e = d.store.album(context.Background(), "7")
	require.NoError(t, e)
	require.NotEqual(t, old, a.Album.canonical())
	_, e = d.store.byPath(context.Background(), old)
	require.Error(t, e)
	_, e = d.store.album(context.Background(), "9")
	require.NoError(t, e)
	phase.Store(2)
	_, e = d.syncCatalog(context.Background(), true, watermark)
	require.ErrorContains(t, e, "no progress")
	a, e = d.store.album(context.Background(), "7")
	require.NoError(t, e)
	require.True(t, a.Active)
	phase.Store(3)
	_, e = d.syncCatalog(context.Background(), true, watermark)
	require.NoError(t, e)
	a, e = d.store.album(context.Background(), "7")
	require.NoError(t, e)
	require.True(t, a.Active)
	b, e := d.store.album(context.Background(), "9")
	require.NoError(t, e)
	require.True(t, b.Active) // Valid detail means pagination absence is not removal.
}
func syntheticPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, 3, 13))
	for y := 0; y < 13; y++ {
		for x := 0; x < 3; x++ {
			img.Set(x, y, color.NRGBA{R: uint8(y * 10), G: uint8(x * 50), A: 255})
		}
	}
	var b bytes.Buffer
	require.NoError(t, png.Encode(&b, img))
	return b.Bytes()
}
func TestImageRestorationLimitsAndFormats(t *testing.T) {
	b := syntheticPNG(t)
	out, mime, e := convertImage(context.Background(), b, page{Segments: 4}, 100, 1<<20)
	require.NoError(t, e)
	require.Equal(t, "image/png", mime)
	img, e := png.Decode(bytes.NewReader(out))
	require.NoError(t, e)
	rows := restoreRows(13, 4)
	require.Equal(t, []int{9, 10, 11, 12, 6, 7, 8, 3, 4, 5, 0, 1, 2}, rows)
	for y, source := range rows {
		r, _, _, _ := img.At(0, y).RGBA()
		require.Equal(t, uint32(source*10)*257, r)
	}
	_, _, e = convertImage(context.Background(), b, page{}, 10, 1<<20)
	require.ErrorContains(t, e, "pixel limit")
	_, _, e = convertImage(context.Background(), b, page{}, 100, 1)
	require.ErrorContains(t, e, "output limit")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, e = convertImage(ctx, b, page{}, 100, 1<<20)
	require.ErrorIs(t, e, context.Canceled)
	pal := image.NewPaletted(image.Rect(0, 0, 2, 2), color.Palette{color.Black, color.White})
	var animated bytes.Buffer
	require.NoError(t, gif.Encode(&animated, pal, nil))
	got, mime, e := convertImage(context.Background(), animated.Bytes(), page{GIF: true}, 100, 1<<20)
	require.NoError(t, e)
	require.Equal(t, animated.Bytes(), got)
	require.Equal(t, "image/gif", mime)
	decoded, _ := png.Decode(bytes.NewReader(b))
	var jpg bytes.Buffer
	require.NoError(t, jpeg.Encode(&jpg, decoded, nil))
	_, mime, e = convertImage(context.Background(), jpg.Bytes(), page{Segments: 4}, 100, 1<<20)
	require.NoError(t, e)
	require.Equal(t, "image/png", mime)
}
func TestServerContentRangeHeadReleaseAndNoImageReuse(t *testing.T) {
	b := syntheticPNG(t)
	var requests atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		require.Empty(t, r.Header.Get("Range"))
		w.Header().Set("Content-Type", "image/png")
		w.Write(b)
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "server")
	o := &object{Object: model.Object{Name: "0001.png", Size: 0}, chapterID: "42", page: &page{URL: s.URL, Segments: 4}}
	for _, method := range []string{"GET", "HEAD", "GET"} {
		link, e := d.Link(context.Background(), o, model.LinkArgs{})
		require.NoError(t, e)
		require.True(t, link.SyncClosers.AcquireReference())
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(method, "http://openlist.example/d/0001.png", nil)
		if method == "GET" {
			req.Header.Set("Range", "bytes=0-7")
		}
		require.NoError(t, common.Proxy(rr, req, link, o))
		if method == "GET" {
			require.Equal(t, 206, rr.Code)
			require.Len(t, rr.Body.Bytes(), 8)
		} else {
			require.Equal(t, 200, rr.Code)
			require.Empty(t, rr.Body.Bytes())
			require.NotEmpty(t, rr.Header().Get("Content-Length"))
		}
		m := link.RangeReader.(*memoryBuffer)
		require.NoError(t, link.Close())
		require.Nil(t, m.data)
		require.True(t, link.Expired())
	}
	require.EqualValues(t, 3, requests.Load())
	conversions.Lock()
	require.Zero(t, conversions.active)
	conversions.Unlock()
	m := &memoryBuffer{data: []byte("abc")}
	r, e := m.RangeRead(context.Background(), http_range.Range{Start: 1, Length: 2})
	require.NoError(t, e)
	got, e := io.ReadAll(r)
	require.NoError(t, e)
	require.Equal(t, "bc", string(got))
}
