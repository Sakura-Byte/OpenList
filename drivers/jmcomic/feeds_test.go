package jmcomic

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestBoundedRepeatedFeedStillPopulatesRanksAndCategories(t *testing.T) {
	var repeatedCalls atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/categories":
			replyJSON(w, r, map[string]any{"blocks": []any{}})
		case "/promote":
			replyJSON(w, r, []section{{ID: "9", Title: "Fixed list", Type: "promote", Content: []album{{ID: "42", Name: "Example"}}}})
		case "/promote_list":
			repeatedCalls.Add(1)
			replyJSON(w, r, map[string]any{"content": []album{{ID: "42", Name: "Example", Date: "2026-10-07", Updated: 1}}})
		default:
			replyJSON(w, r, map[string]any{"content": []album{{ID: "42", Name: "Example", Date: "2026-10-07", Updated: 1}, {ID: "43", Name: "Second", Date: "2026-10-07", Updated: 1}}})
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	d.FeedLimit = 2
	require.NoError(t, d.syncFeeds(context.Background()))
	require.EqualValues(t, 2, repeatedCalls.Load())
	for _, p := range []string{"/排行榜/日榜", "/排行榜/周榜", "/分类/同人", "/分类/英文"} {
		dir, err := d.Get(context.Background(), p)
		require.NoError(t, err)
		items, err := d.List(context.Background(), dir, model.ListArgs{})
		require.NoError(t, err)
		require.Len(t, items, 2)
	}
}

func TestFailedRecommendationDoesNotStarveOtherFeeds(t *testing.T) {
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/promote":
			replyJSON(w, r, []section{{ID: "9", Title: "Broken", Type: "promote", Content: []album{{ID: "42", Name: "Example"}}}})
		case "/promote_list":
			w.WriteHeader(http.StatusNotFound)
		default:
			replyJSON(w, r, map[string]any{"content": []album{{ID: "42", Name: "Example", Date: "2026-10-07", Updated: 1}}})
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	d.FeedLimit = 1
	ctx := context.Background()
	require.NoError(t, d.store.saveFeed(ctx, "/推荐/Broken [9]", []album{{ID: "87", Name: "Previously cached"}}, 1))
	require.NoError(t, d.store.saveFeed(ctx, "/推荐/Older snapshot", []album{{ID: "88", Name: "Retain until complete"}}, 1))
	err := d.syncFeeds(ctx)
	require.ErrorContains(t, err, "/推荐/Broken [9]")
	for _, p := range []string{"/排行榜/日榜", "/分类/同人", "/推荐/Broken [9]", "/推荐/Older snapshot"} {
		dir, err := d.Get(ctx, p)
		require.NoError(t, err)
		items, err := d.List(ctx, dir, model.ListArgs{})
		require.NoError(t, err)
		require.Len(t, items, 1)
		if p == "/推荐/Broken [9]" {
			require.Equal(t, "album/87", items[0].GetID())
		}
	}
}

func TestFailedPromoteResponseStillRefreshesCategories(t *testing.T) {
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/promote" {
			w.WriteHeader(http.StatusNotFound)
		} else {
			replyJSON(w, r, map[string]any{"content": []album{{ID: "42", Name: "Example", Date: "2026-10-07", Updated: 1}}})
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	d.FeedLimit = 1
	ctx := context.Background()
	require.NoError(t, d.store.saveFeed(ctx, "/今日推荐", []album{{ID: "87", Name: "Old snapshot"}}, 1))
	require.ErrorContains(t, d.syncFeeds(ctx), "/今日推荐")
	for _, p := range []string{"/今日推荐", "/排行榜/日榜", "/分类/同人"} {
		dir, err := d.Get(ctx, p)
		require.NoError(t, err)
		items, err := d.List(ctx, dir, model.ListArgs{})
		require.NoError(t, err)
		require.Len(t, items, 1)
	}
}

func TestFullCatalogRepeatedPagesRemainIncomplete(t *testing.T) {
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replyJSON(w, r, map[string]any{"content": []album{{ID: "42", Name: "Example", Date: "2026-10-07", Updated: 1}}})
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	ctx := context.Background()
	seedAlbum(t, d, "87", "Must retain", "2026-10-06", 1)
	_, err := d.syncCatalog(ctx, true, 0)
	require.Error(t, err)
	old, err := d.store.album(ctx, "87")
	require.NoError(t, err)
	require.True(t, old.Active)
}
