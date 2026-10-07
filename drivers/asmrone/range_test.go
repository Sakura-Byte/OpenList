package asmrone

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
)

func TestRangeNames(t *testing.T) {
	for _, test := range []struct{ source, want string }{
		{"RJ009999", "RJ000000~RJ009999"},
		{"RJ010000", "RJ010000~RJ019999"},
		{"RJ117765", "RJ110000~RJ119999"},
		{"VJ117765", "RJ110000~RJ119999"},
		{" bj119999 ", "RJ110000~RJ119999"},
		{"RJ120000", "RJ120000~RJ129999"},
		{"RJ999999", "RJ990000~RJ999999"},
		{"RJ01000000", "RJ01000000~RJ01009999"},
		{"VJ01011776", "RJ01010000~RJ01019999"},
		{"BJ1011776", "RJ01010000~RJ01019999"},
		{"", "RJ110000~RJ119999"},
		{"RJ", "其他作品"},
		{"RJ-123", "其他作品"},
		{"RJ123/title", "其他作品"},
		{"AB117765", "其他作品"},
	} {
		t.Run(test.source, func(t *testing.T) {
			require.Equal(t, test.want, (work{ID: 117765, SourceID: test.source}).rangeName())
		})
	}
}

func TestRangeFoldersSharePrefixesAndScanOnlyCache(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "/api/tracks/1", r.URL.Path)
		io.WriteString(w, `[{"type":"folder","title":"nested","children":[{"type":"audio","title":"audio.mp3","size":123,"mediaStreamUrl":"https://media.example/audio"}]}]`)
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	ctx := context.Background()
	works := []work{
		{ID: 1, SourceID: "RJ117765", Title: "RJ title"},
		{ID: 2, SourceID: "VJ117765", Title: "VJ title"},
		{ID: 3, SourceID: "BJ119999", Title: "BJ title"},
		{ID: 4, SourceID: "RJ120000", Title: "Next range"},
		{ID: 5, SourceID: "RJ01011776", Title: "Eight digits"},
	}
	require.NoError(t, d.store.upsertPage(ctx, works, "current"))
	root, err := d.Get(ctx, "/")
	require.NoError(t, err)
	ranges, err := d.List(ctx, root, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	var names []string
	for _, folder := range ranges {
		require.True(t, folder.IsDir())
		names = append(names, folder.GetName())
	}
	require.ElementsMatch(t, []string{"RJ110000~RJ119999", "RJ120000~RJ129999", "RJ01010000~RJ01019999"}, names)
	rangeDir, err := d.Get(ctx, "/RJ110000~RJ119999")
	require.NoError(t, err)
	listed, err := d.List(ctx, rangeDir, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	require.Len(t, listed, 3)
	for _, obj := range listed {
		require.True(t, strings.HasPrefix(obj.GetPath(), "/RJ110000~RJ119999/"))
		resolved, err := d.Get(ctx, obj.GetPath())
		require.NoError(t, err)
		require.Equal(t, obj.GetID(), resolved.GetID())
	}
	require.NoError(t, op.WalkStorageRecursive(ctx, d, "/", -1, model.ListArgs{Refresh: true}, false, nil, func(_ string, _ []model.Obj) error { return nil }))
	require.Zero(t, requests.Load())
	_, err = d.Get(ctx, "/RJ120000~RJ129999/RJ117765 RJ title")
	require.Error(t, err)
	_, err = d.Get(ctx, "/RJ117765 RJ title")
	require.Error(t, err)
	file, err := d.Get(ctx, "/RJ110000~RJ119999/RJ117765 RJ title/nested/audio.mp3")
	require.NoError(t, err)
	require.EqualValues(t, 123, file.GetSize())
	require.EqualValues(t, 1, requests.Load())
	parent, err := d.Get(ctx, "/RJ110000~RJ119999/RJ117765 RJ title/nested")
	require.NoError(t, err)
	files, err := d.List(ctx, parent, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, file.GetPath(), files[0].GetPath())
	link, err := d.Link(ctx, file, model.LinkArgs{})
	require.NoError(t, err)
	require.Equal(t, "https://media.example/audio", link.URL)
	require.EqualValues(t, 1, requests.Load())
	_, err = d.Get(driver.WithCachedTraversal(ctx), "/RJ110000~RJ119999/BJ119999 BJ title/README.md")
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
}

func TestRangeChangesSearchAndHideEmptyFolder(t *testing.T) {
	d := newTestDriver(t, "http://127.0.0.1:1", t.TempDir())
	ctx := context.Background()
	w := work{ID: 42, SourceID: "RJ117765", Title: "Original"}
	require.NoError(t, d.store.upsertPage(ctx, []work{w}, "first"))
	old, err := d.store.byID(ctx, 42)
	require.NoError(t, err)
	w.SourceID, w.Title = "BJ120001", "Renamed"
	require.NoError(t, d.store.upsertPage(ctx, []work{w}, "next"))
	_, err = d.Get(ctx, "/RJ110000~RJ119999")
	require.Error(t, err)
	// A fetch begun before a rename still belongs to the current cached path.
	require.NoError(t, d.store.saveTracks(ctx, old, []track{{Title: "track.mp3", Type: "audio"}}, time.Now().Unix()))
	var nodes []model.SearchNode
	req := model.SearchReq{Parent: "/", Keywords: "track.mp3"}
	require.NoError(t, d.SearchCached(ctx, req, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
	require.Equal(t, []model.SearchNode{{Parent: "/RJ120000~RJ129999/BJ120001 Renamed", Name: "track.mp3"}}, nodes)
	require.NoError(t, d.store.finishFull(ctx, "missing"))
	nodes = nil
	require.NoError(t, d.SearchCached(ctx, req, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
	require.Empty(t, nodes)
	ranges, err := d.store.ranges(ctx)
	require.NoError(t, err)
	require.Empty(t, ranges)
	require.NoError(t, d.store.upsertPage(ctx, []work{w}, "restored"))
	restored, err := d.store.byID(ctx, 42)
	require.NoError(t, err)
	require.True(t, restored.HasTracks)
	nodes = nil
	require.NoError(t, d.SearchCached(ctx, req, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
	require.Len(t, nodes, 1)
}

func TestGroupedProxyRecoveryWithoutHash(t *testing.T) {
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tracks/71":
			requests.Add(1)
			fmt.Fprintf(w, `[{"type":"folder","title":"nested","children":[{"type":"audio","title":"audio.mp3","mediaStreamUrl":%q}]}]`, server.URL+"/new")
		case "/old":
			w.WriteHeader(http.StatusGone)
		case "/new":
			require.Equal(t, "bytes=1-2", r.Header.Get("Range"))
			w.WriteHeader(http.StatusPartialContent)
			io.WriteString(w, "bc")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	ctx := context.Background()
	require.NoError(t, d.store.upsertPage(ctx, []work{{ID: 71, SourceID: "BJ117765", Title: "Audio"}}, ""))
	w, err := d.store.byID(ctx, 71)
	require.NoError(t, err)
	require.NoError(t, d.store.saveTracks(ctx, w, []track{{Type: "folder", Title: "nested", Children: []track{{Type: "audio", Title: "audio.mp3", Stream: server.URL + "/old"}}}}, time.Now().Unix()))
	file, err := d.Get(ctx, w.path()+"/nested/audio.mp3")
	require.NoError(t, err)
	link, err := d.Link(ctx, file, model.LinkArgs{})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodGet, link.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=1-2")
	res, err := link.HTTPClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "bc", string(body))
	require.Equal(t, http.StatusPartialContent, res.StatusCode)
	require.EqualValues(t, 1, requests.Load())
}
