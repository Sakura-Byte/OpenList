package asmrone

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	conf.Conf = conf.DefaultConfig("data")
	base.InitClient()
	os.Exit(m.Run())
}

func newTestDriver(t *testing.T, apiURL, root string) *ASMRONE {
	t.Helper()
	d := &ASMRONE{Storage: model.Storage{ID: 91, Status: op.WORK, MountPath: "/ASMR_DLSite", APIProxyMode: model.ProxyDisabled, TransferProxyMode: model.ProxyDisabled}, Addition: Addition{APIURL: apiURL, CacheRoot: root, RequestsPerSecond: 100, SyncHours: 24, FullSyncDays: 7}}
	// Pause before initializing so tests never start a catalog or track crawl.
	d.APIURL = apiURL
	filename := filepath.Join(root, "91", fmt.Sprintf("%x", sha256sum(apiURL))[:16], "cache.db")
	store, err := openStore(filename)
	require.NoError(t, err)
	require.NoError(t, store.saveState(context.Background(), syncState{Paused: true}))
	require.NoError(t, store.db.Close())
	require.NoError(t, d.Init(context.Background()))
	t.Cleanup(func() { require.NoError(t, d.Drop(context.Background())); base.CloseStorageClients(d) })
	return d
}
func sha256sum(s string) [32]byte { return sha256.Sum256([]byte(s)) }
func seedWork(t *testing.T, d *ASMRONE, id int64, title string) cachedWork {
	t.Helper()
	w := work{ID: number(id), SourceID: fmt.Sprintf("RJ%06d", id), Title: title, Created: "2026-10-07"}
	require.NoError(t, d.store.upsertPage(context.Background(), []work{w}, ""))
	result, err := d.store.byID(context.Background(), id)
	require.NoError(t, err)
	return result
}

func TestPermanentTracksAndCachedTraversal(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "/api/tracks/12", r.URL.Path)
		require.Equal(t, "2", r.URL.Query().Get("v"))
		io.WriteString(w, `[{"type":"folder","title":"RJ000012","children":[{"type":"audio","title":"track.mp3","size":"123","mediaStreamUrl":"https://media.example/audio"}]}]`)
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	cw := seedWork(t, d, 12, "Original")
	root, err := d.Get(context.Background(), "/")
	require.NoError(t, err)
	var scanned int
	require.NoError(t, op.WalkStorageRecursive(context.Background(), d, "/", -1, model.ListArgs{Refresh: true}, false, nil, func(_ string, objs []model.Obj) error { scanned += len(objs); return nil }))
	require.Greater(t, scanned, 0)
	require.EqualValues(t, 0, requests.Load())
	_, err = d.List(context.Background(), root, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	dir, err := d.Get(context.Background(), "/"+cw.name())
	require.NoError(t, err)
	const callers = 12
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			_, err := d.List(context.Background(), dir, model.ListArgs{Refresh: true})
			require.NoError(t, err)
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, requests.Load())
	obj, err := d.Get(context.Background(), "/"+cw.name()+"/track.mp3")
	require.NoError(t, err)
	require.EqualValues(t, 123, obj.GetSize())
	// Close and reopen the actual SQLite cache with the same storage and URL.
	d.mu.Lock()
	d.state.Paused = true
	require.NoError(t, d.store.saveState(d.ctx, d.state))
	d.mu.Unlock()
	require.NoError(t, d.Drop(context.Background()))
	require.NoError(t, d.Init(context.Background()))
	_, err = d.Get(context.Background(), obj.GetPath())
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	_, err = d.List(context.Background(), dir, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	_, err = d.ListCached(context.Background(), dir, model.ListArgs{})
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
}

func TestEmptyTreeAndRefreshFailurePreservesCache(t *testing.T) {
	var bad atomic.Bool
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if bad.Load() {
			w.WriteHeader(404)
			return
		}
		io.WriteString(w, `[]`)
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	seedWork(t, d, 5, "Empty")
	nodes, err := d.loadTracks(context.Background(), 5, false, true)
	require.NoError(t, err)
	require.Empty(t, nodes)
	_, err = d.loadTracks(context.Background(), 5, false, true)
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	bad.Store(true)
	_, err = d.loadTracks(context.Background(), 5, true, true)
	require.Error(t, err)
	_, err = d.loadTracks(context.Background(), 5, false, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, requests.Load())
	w, err := d.store.byID(context.Background(), 5)
	require.NoError(t, err)
	require.True(t, w.HasTracks)
	require.NotEmpty(t, w.Error)
}

func TestCatalogIncrementalRenameRemovalAndRecovery(t *testing.T) {
	var mu sync.Mutex
	works := []work{{ID: 1, SourceID: "RJ000001", Title: "One", Created: "2026-10-06"}, {ID: 2, SourceID: "RJ000002", Title: "Two", Created: "2026-10-05"}}
	var broken bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		response := catalogPage{Works: works}
		response.Pagination.CurrentPage = 1
		response.Pagination.PageSize = 100
		response.Pagination.TotalCount = len(works)
		if broken {
			response.Pagination.TotalCount++
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	require.NoError(t, d.syncCatalog(context.Background(), true))
	w, err := d.store.byID(context.Background(), 1)
	require.NoError(t, err)
	require.NoError(t, d.store.saveTracks(context.Background(), w, []track{}, time.Now().Unix()))
	mu.Lock()
	works = []work{{ID: 3, SourceID: "RJ000003", Title: "Same day", Created: "2026-10-06"}, {ID: 1, SourceID: "RJ000001", Title: "Renamed", Created: "2026-10-06"}, {ID: 2, SourceID: "RJ000002", Title: "Two", Created: "2026-10-05"}}
	mu.Unlock()
	require.NoError(t, d.syncCatalog(context.Background(), false))
	w, err = d.store.byID(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, "Renamed", w.Title)
	require.True(t, w.HasTracks)
	_, err = d.Get(context.Background(), "/RJ000001 One")
	require.Error(t, err)
	mu.Lock()
	works = works[:1]
	broken = true
	mu.Unlock()
	require.Error(t, d.syncCatalog(context.Background(), true))
	w, err = d.store.byID(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, w.Active)
	mu.Lock()
	broken = false
	mu.Unlock()
	require.NoError(t, d.syncCatalog(context.Background(), true))
	w, err = d.store.byID(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, w.Active)
	require.True(t, w.HasTracks)
	mu.Lock()
	works = append(works, work{ID: 1, SourceID: "RJ000001", Title: "Restored", Created: "2026-10-06"})
	mu.Unlock()
	require.NoError(t, d.syncCatalog(context.Background(), true))
	w, err = d.store.byID(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, w.Active)
	require.True(t, w.HasTracks)
}

func TestProxyRecoveryPreservesRangeAndHead(t *testing.T) {
	var requests atomic.Int32
	var broken atomic.Bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tracks/7":
			requests.Add(1)
			stream := "/old"
			if broken.Load() {
				stream = "/new"
			}
			fmt.Fprintf(w, `[{"type":"audio","title":"audio.mp3","hash":"audio","size":6,"mediaStreamUrl":%q}]`, server.URL+stream)
		case "/old":
			w.WriteHeader(410)
		case "/new":
			require.Equal(t, "bytes=1-2", r.Header.Get("Range"))
			w.Header().Set("Content-Range", "bytes 1-2/6")
			w.WriteHeader(206)
			if r.Method != "HEAD" {
				io.WriteString(w, "bc")
			}
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	cw := seedWork(t, d, 7, "Audio")
	obj, err := d.Get(context.Background(), "/"+cw.name()+"/audio.mp3")
	require.NoError(t, err)
	broken.Store(true)
	link, err := d.Link(context.Background(), obj, model.LinkArgs{Redirect: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	req, _ := http.NewRequest("GET", link.URL, nil)
	req.Header.Set("Range", "bytes=1-2")
	response, err := link.HTTPClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "bc", string(body))
	require.Equal(t, 206, response.StatusCode)
	require.EqualValues(t, 2, requests.Load())
	link, err = d.Link(context.Background(), obj, model.LinkArgs{})
	require.NoError(t, err)
	req, _ = http.NewRequest("HEAD", link.URL, nil)
	req.Header.Set("Range", "bytes=1-2")
	response, err = link.HTTPClient.Do(req)
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, 206, response.StatusCode)
	require.EqualValues(t, 2, requests.Load())
	// Reusing a stale URL during cooldown must not trigger another metadata request.
	req, _ = http.NewRequest("GET", server.URL+"/old", nil)
	response, err = link.HTTPClient.Do(req)
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, 410, response.StatusCode)
	require.EqualValues(t, 2, requests.Load())
}

func TestReadmeAndDuplicateNames(t *testing.T) {
	nodes := normalizeTracks([]track{{Type: "audio", Title: "a/b.mp3"}, {Type: "audio", Title: "a/b.mp3"}, {Type: "audio", Title: "a／b (2).mp3"}, {Type: "text", Title: ".."}})
	require.Equal(t, "a／b.mp3", nodes[0].Title)
	require.Equal(t, "a／b (3).mp3", nodes[1].Title)
	require.Equal(t, "untitled", nodes[3].Title)
	d := newTestDriver(t, "http://127.0.0.1:1", t.TempDir())
	w := seedWork(t, d, 8, "Info")
	obj, err := d.Get(driver.WithCachedTraversal(context.Background()), "/"+w.name()+"/README.md")
	require.NoError(t, err)
	link, err := d.Link(driver.WithCachedTraversal(context.Background()), obj, model.LinkArgs{})
	require.NoError(t, err)
	reader, err := link.RangeReader.RangeRead(context.Background(), http_range.Range{Start: 0, Length: 6})
	require.NoError(t, err)
	data, _ := io.ReadAll(reader)
	reader.Close()
	require.Equal(t, "# Info", string(data))
}
