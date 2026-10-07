package jmcomic

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestAllKnownTagsCountsDeduplicateAndTrackCacheChanges(t *testing.T) {
	var calls atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); t.Error("unexpected upstream request") }))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	ctx := context.Background()
	require.NoError(t, d.store.saveTagCatalog(ctx, []string{"Known", "Empty"}))
	d.mu.Lock()
	d.state.LastTags = time.Now().Unix()
	d.mu.Unlock()
	a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	b := seedAlbum(t, d, "43", "Second", "2026-10-07", 1)
	require.NoError(t, d.store.saveDetail(ctx, a, detail{ID: "42", Tags: []string{"Known", "Found", "Found"}}))
	require.NoError(t, d.store.saveDetail(ctx, b, detail{ID: "43", Tags: []string{"Known"}}))
	require.NoError(t, d.store.saveFeed(ctx, tagPath("Known"), []album{a.Album}, 1))
	tags, err := d.store.tags(ctx)
	require.NoError(t, err)
	require.Equal(t, []cachedTag{{Name: "Known", Path: "/标签/Known", Count: 2}, {Name: "Found", Path: "/标签/Found", Count: 1}, {Name: "Empty", Path: "/标签/Empty", Count: 0}}, tags)
	obj, err := d.Get(ctx, "/标签")
	require.NoError(t, err)
	list, err := d.List(ctx, obj, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	require.Len(t, list, 3)
	require.Equal(t, &model.TagInfo{Name: "Known", Count: 2}, model.GetTagInfo(list[0]))
	var found []model.SearchNode
	require.NoError(t, d.SearchCached(ctx, model.SearchReq{Parent: tagPath("Found"), Keywords: "JM42"}, func(n model.SearchNode) error { found = append(found, n); return nil }))
	require.Len(t, found, 1)
	require.Equal(t, a.Album.datePath(), found[0].Parent)
	require.Equal(t, tagPath("Found")+"/"+a.Album.name(), found[0].AccessPath)
	require.NoError(t, d.store.hide(ctx, a.Album))
	tags, err = d.store.tags(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, tags[0].Count)
	require.NoError(t, d.store.upsert(ctx, []album{a.Album}, ""))
	require.NoError(t, d.Drop(ctx))
	require.NoError(t, d.Init(ctx))
	tags, err = d.store.tags(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, tags[0].Count)
	require.Zero(t, calls.Load())
	require.NotEqual(t, tagPath("A/B"), tagPath("A／B"))
	require.NotEqual(t, tagPath("A\\B"), tagPath("A/B"))
}

func TestTagCatalogOnDemandMergesRequestsAndKeepsOldFeedOnFailure(t *testing.T) {
	var searches atomic.Int32
	var fail atomic.Bool
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/categories":
			replyJSON(w, r, map[string]any{"blocks": []any{map[string]any{"content": []string{"Sample", "Extra", "Sample"}}}})
		case "/search":
			searches.Add(1)
			require.Equal(t, "3", r.URL.Query().Get("main_tag"))
			require.Equal(t, "Sample", r.URL.Query().Get("search_query"))
			if fail.Load() {
				w.WriteHeader(404)
				return
			}
			replyJSON(w, r, map[string]any{"content": []album{{ID: "42", Name: "Example", Date: "2026-10-07"}}})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	d.FeedLimit = 1
	ctx := context.Background()
	require.NoError(t, d.syncTagCatalog(ctx))
	require.Zero(t, searches.Load())
	dir, err := d.Get(ctx, tagPath("Sample"))
	require.NoError(t, err)
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() { list, e := d.List(ctx, dir, model.ListArgs{}); require.NoError(t, e); require.Len(t, list, 1) })
	}
	wg.Wait()
	require.EqualValues(t, 1, searches.Load())
	fail.Store(true)
	require.Error(t, d.ensureTagFeed(ctx, "Sample", true, true))
	list, err := d.List(ctx, dir, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.EqualValues(t, 2, searches.Load())
	list, err = d.ListCached(ctx, dir, model.ListArgs{})
	require.NoError(t, err)
	require.Empty(t, list)
	require.EqualValues(t, 2, searches.Load())
	var tasks int
	require.NoError(t, d.store.db.QueryRow(`SELECT count(*) FROM tasks WHERE kind='tag'`).Scan(&tasks))
	require.Equal(t, 1, tasks)
}
