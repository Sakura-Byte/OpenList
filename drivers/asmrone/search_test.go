package asmrone

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestSearchCachedFilesScopeRestartAndNoUpstream(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir())
	ctx := context.Background()
	w := seedWork(t, d, 1, "Example")
	require.NoError(t, d.store.saveTracks(ctx, w, []track{{Title: "Folder", Type: "folder", Children: []track{{Title: "声優 100%.MP3", Type: "audio", Size: 123}}}, {Title: "README.md", Type: "text", Size: 77}}, time.Now().Unix()))
	query := func(req model.SearchReq) []model.SearchNode {
		var nodes []model.SearchNode
		require.NoError(t, d.SearchCached(ctx, req, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
		return nodes
	}
	req := model.SearchReq{Parent: w.path(), Keywords: "声優 100%.mp3", Scope: 2}
	require.Equal(t, []model.SearchNode{{Parent: path.Join(w.path(), "Folder"), Name: "声優 100%.MP3", Size: 123}}, query(req))
	req.Scope = 1
	require.Empty(t, query(req))
	req.Scope = 2
	req.Parent = "/Other"
	require.Empty(t, query(req))
	req.Parent = w.path()
	req.Keywords = "README"
	nodes := query(req)
	require.Len(t, nodes, 1)
	require.EqualValues(t, 77, nodes[0].Size)
	require.NoError(t, d.Drop(ctx))
	require.NoError(t, d.Init(ctx))
	require.Len(t, query(req), 1)
	var count int
	require.NoError(t, d.store.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='index_jobs'`).Scan(&count))
	require.Zero(t, count)
	require.Zero(t, calls.Load())
}

func TestCacheSearchBatchUsesPrimaryKeyAndJSONPrefilterIsLiteral(t *testing.T) {
	d := newTestDriver(t, "http://127.0.0.1:1", t.TempDir())
	rows, err := d.store.db.Query("EXPLAIN QUERY PLAN "+searchWorkBatchQuery, int64(1<<63-1))
	require.NoError(t, err)
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan.WriteString(detail)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Contains(t, plan.String(), "INTEGER PRIMARY KEY")
	require.NotContains(t, plan.String(), "TEMP B-TREE")
	w := seedWork(t, d, 1, "Example")
	name := `声優 Ä <"Quoted" & 100%>.mp3`
	require.NoError(t, d.store.saveTracks(context.Background(), w, []track{{Title: name, Type: "audio"}}, 1))
	var nodes []model.SearchNode
	require.NoError(t, d.SearchCached(context.Background(), model.SearchReq{Parent: "/", Keywords: `声優 ä <"quoted" & 100%>.mp3`, Scope: 2}, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
	require.Len(t, nodes, 1)
	require.Equal(t, name, nodes[0].Name)
}
