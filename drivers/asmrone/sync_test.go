package asmrone

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
)

func TestUninitializedManagementReturnsErrors(t *testing.T) {
	d := &ASMRONE{}
	_, err := d.ASMRONEStatus(context.Background())
	require.Error(t, err)
	require.Error(t, d.ASMRONEAction(context.Background(), "pause", ""))
}

func TestRateConfigurationMatchesFrontendFloatType(t *testing.T) {
	info := op.GetDriverInfoMap()["ASMR.ONE"]
	for _, item := range info.Additional {
		if item.Name == "requests_per_second" {
			require.Equal(t, "float", item.Type)
			require.Equal(t, "1", item.Default)
			return
		}
	}
	t.Fatal("missing rate configuration")
}

func TestDirectoryCollisionsPreserveOriginalMetadata(t *testing.T) {
	d := newTestDriver(t, "http://127.0.0.1:1", t.TempDir())
	works := []work{{ID: 1, SourceID: "RJ1", Title: "Same", Created: "2026-10-07"}, {ID: 3, SourceID: "RJ1", Title: "Same [2]", Created: "2026-10-07"}, {ID: 2, SourceID: "RJ1", Title: "Same", Created: "2026-10-07"}}
	require.NoError(t, d.store.upsertPage(context.Background(), works, ""))
	list, err := d.store.works(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 3)
	names := map[string]bool{}
	for _, w := range list {
		require.False(t, names[w.name()])
		names[w.name()] = true
	}
	w, err := d.store.byID(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, "Same", w.Title)
	require.Equal(t, "RJ1 Same [2] (2)", w.name())
	require.NoError(t, d.store.upsertPage(context.Background(), works, ""))
	latest, err := d.store.byID(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, w.name(), latest.name())
}

func TestRenameCycleKeepsFinalIndexEventLast(t *testing.T) {
	d := newTestDriver(t, "http://127.0.0.1:1", t.TempDir())
	ctx := context.Background()
	w := seedWork(t, d, 1, "A")
	_, err := d.store.db.Exec(`DELETE FROM index_jobs`)
	require.NoError(t, err)
	for _, title := range []string{"B", "A", "B"} {
		w.Title = title
		require.NoError(t, d.store.upsertPage(ctx, []work{w.work}, ""))
	}
	rows, err := d.store.db.Query(`SELECT old_path,new_path FROM index_jobs ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var events [][2]string
	for rows.Next() {
		var oldPath, newPath string
		require.NoError(t, rows.Scan(&oldPath, &newPath))
		events = append(events, [2]string{oldPath, newPath})
	}
	require.NoError(t, rows.Err())
	require.Equal(t, [][2]string{{"/RJ000001 B", "/RJ000001 A"}, {"/RJ000001 A", "/RJ000001 B"}}, events)
}

func TestManualRefreshPreemptsCatalogPages(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var order []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tracks/1" {
			mu.Lock()
			order = append(order, "tracks")
			mu.Unlock()
			io.WriteString(w, `[]`)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		mu.Lock()
		order = append(order, fmt.Sprintf("catalog%d", page))
		mu.Unlock()
		if page == 1 {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		response := catalogPage{Works: []work{}}
		response.Pagination.CurrentPage = page
		response.Pagination.PageSize = 100
		response.Pagination.TotalCount = 200
		for i := (page-1)*100 + 1; i <= page*100; i++ {
			response.Works = append(response.Works, work{ID: number(i), SourceID: fmt.Sprintf("RJ%06d", i), Title: fmt.Sprintf("Work%d", i), Created: "2026-10-07"})
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	seedWork(t, d, 1, "Work1")
	done := make(chan error, 1)
	go func() { done <- d.syncCatalog(d.ctx, true) }()
	<-started
	require.NoError(t, d.ASMRONEAction(context.Background(), "refresh", "1"))
	require.Eventually(t, func() bool {
		d.scheduler.mu.Lock()
		defer d.scheduler.mu.Unlock()
		for _, job := range d.scheduler.jobs {
			if job.high {
				return true
			}
		}
		return false
	}, 2*time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-done)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"catalog1", "tracks", "catalog2"}, order)
}

func TestRunningCatalogIntentSurvivesDropAndRestart(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		io.WriteString(w, `{"works":[],"pagination":{"currentPage":1,"pageSize":100,"totalCount":0}}`)
	}))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	d.mu.Lock()
	d.state.FullCompleted = true
	d.state.LastSync = time.Now().Unix()
	d.state.LastFullSync = d.state.LastSync
	require.NoError(t, d.store.saveState(d.ctx, d.state))
	d.mu.Unlock()
	require.NoError(t, d.ASMRONEAction(context.Background(), "full", ""))
	require.NoError(t, d.ASMRONEAction(context.Background(), "resume", ""))
	<-started
	require.NoError(t, d.Drop(context.Background()))
	store, err := openStore(d.store.filename)
	require.NoError(t, err)
	state, err := store.loadState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "full", state.Requested)
	require.NoError(t, store.db.Close())
	require.NoError(t, d.Init(context.Background()))
	require.Eventually(t, func() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.state.Requested == "" && d.state.FullCompleted }, 3*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestCachedUpdateSiteScanDoesNotFetchTracks(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `[]`) }))
	defer server.Close()
	d := newTestDriver(t, server.URL, t.TempDir())
	seedWork(t, d, 3, "Uncached")
	err := op.WalkUpdateSiteStorageChunks(context.Background(), d, "/", -1, model.ListArgs{Refresh: true}, func(chunk driver.UpdateSiteChunk) error { return nil })
	require.NoError(t, err)
	require.Zero(t, calls.Load())
}
