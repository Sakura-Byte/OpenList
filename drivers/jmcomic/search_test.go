package jmcomic

import (
	"context"
	"net/http"
	"path"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestSearchCachedCanonicalAliasesModesAndNoUpstream(t *testing.T) {
	var calls atomic.Int32
	s := newMockServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	ctx := context.Background()
	a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	seedAlbum(t, d, "43", "Other", "2026-10-07", 1)
	require.NoError(t, d.store.saveFeed(ctx, "/推荐/Group", []album{a.Album}, 1))
	require.NoError(t, d.store.saveFeed(ctx, "/标签/Tag", []album{a.Album}, 1))
	require.NoError(t, d.store.saveDetail(ctx, a, detail{ID: "42", Series: []chapter{{ID: "42", Name: "Chapter"}}}))
	require.NoError(t, d.store.saveTree(ctx, a.Album, chapterTree{ID: "42", AlbumID: "42", Pages: []page{{Name: "0001.png", URL: "https://example.test/1.jpg"}}}))
	query := func(parent, keyword string) []model.SearchNode {
		var nodes []model.SearchNode
		require.NoError(t, d.SearchCached(ctx, model.SearchReq{Parent: parent, Keywords: keyword}, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
		return nodes
	}
	for _, parent := range []string{"/", "/推荐", "/推荐/Group", "/标签/Tag"} {
		nodes := query(parent, "JM42")
		if parent != "/" {
			feed := parent
			if feed == "/推荐" {
				feed = "/推荐/Group"
			}
			require.Equal(t, path.Join(feed, a.Album.name()), nodes[0].AccessPath)
			nodes[0].AccessPath = ""
		}
		require.Equal(t, []model.SearchNode{{Parent: a.Album.datePath(), Name: a.Album.name(), IsDir: true}}, nodes)
	}
	require.Empty(t, query("/推荐/Group", "Other"))
	cp := path.Join(a.Album.canonical(), "001 Chapter [42]")
	require.Len(t, query(path.Join("/推荐/Group", a.Album.name()), "Chapter"), 1)
	require.Empty(t, query("/", "0001"))
	d.Mode = "server"
	require.Equal(t, []model.SearchNode{{Parent: cp, Name: "0001.png"}}, query("/", "0001"))
	require.NoError(t, d.store.hide(ctx, a.Album))
	require.Empty(t, query("/", "JM42"))
	require.NoError(t, d.store.upsert(ctx, []album{a.Album}, ""))
	require.Len(t, query("/", "0001"), 1)
	require.NoError(t, d.Drop(ctx))
	require.NoError(t, d.Init(ctx))
	require.Len(t, query(cp, "0001"), 1)
	var count int
	require.NoError(t, d.store.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='index_jobs'`).Scan(&count))
	require.Zero(t, count)
	require.Zero(t, calls.Load())
}
