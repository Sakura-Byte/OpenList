package jmcomic

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestSharedSchedulerOneRPSAndCancellation(t *testing.T) {
	a, b := &JMComic{}, &JMComic{}
	s := acquireScheduler(t.Name(), a, 100)
	require.Same(t, s, acquireScheduler(t.Name(), b, 1))
	defer releaseScheduler(t.Name(), a)
	defer releaseScheduler(t.Name(), b)
	var mu sync.Mutex
	var starts []time.Time
	var running, maxRunning atomic.Int32
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			require.NoError(t, s.do(context.Background(), true, func() error {
				n := running.Add(1)
				if n > maxRunning.Load() {
					maxRunning.Store(n)
				}
				mu.Lock()
				starts = append(starts, time.Now())
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				running.Add(-1)
				return nil
			}))
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, maxRunning.Load())
	require.Len(t, starts, 3)
	for i := 1; i < len(starts); i++ {
		require.GreaterOrEqual(t, starts[i].Sub(starts[i-1]), 950*time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.do(ctx, false, func() error { t.Error("cancelled job ran"); return nil }), context.Canceled)
}
func TestRetryAfterAndNoBurst(t *testing.T) {
	var times []time.Time
	var mu sync.Mutex
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		replyJSON(w, r, map[string]any{"content": []album{}})
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	items, e := d.catalogPage(context.Background(), 1, "0", "mr", true)
	require.NoError(t, e)
	require.Empty(t, items)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, times, 2)
	require.GreaterOrEqual(t, times[1].Sub(times[0]), 950*time.Millisecond)
}
func TestIncompleteConfirmationDoesNotHideAndRecoveryReusesTree(t *testing.T) {
	var phase atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/categories/filter":
			n, _ := strconv.Atoi(r.URL.Query().Get("page"))
			items := []album{}
			if n == 1 {
				items = []album{{ID: "8", Name: "Present", Date: "2026-10-07", Updated: 3}}
				if phase.Load() == 2 {
					items = append(items, album{ID: "7", Name: "Restore", Date: "2026-07-21", Updated: 1})
				}
			}
			replyJSON(w, r, map[string]any{"total": 1, "content": items})
		case "/album":
			if r.URL.Query().Get("id") == "9" && phase.Load() == 0 {
				replyJSON(w, r, map[string]any{"broken": true})
				return
			}
			w.WriteHeader(404)
		default:
			t.Errorf("unexpected metadata fetch %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "server")
	a := seedAlbum(t, d, "7", "Removed", "2026-07-21", 1)
	seedAlbum(t, d, "9", "Transient", "2026-07-21", 1)
	require.NoError(t, d.store.saveTree(context.Background(), a.Album, chapterTree{ID: "7", AlbumID: "7", Version: 1, Pages: []page{}}))
	_, e := d.syncCatalog(context.Background(), true, 0)
	require.Error(t, e)
	for _, key := range []string{"7", "9"} {
		a, e := d.store.album(context.Background(), key)
		require.NoError(t, e)
		require.True(t, a.Active)
	}
	phase.Store(1)
	_, e = d.syncCatalog(context.Background(), true, 0)
	require.NoError(t, e)
	a, e = d.store.album(context.Background(), "7")
	require.NoError(t, e)
	require.False(t, a.Active)
	old, e := d.store.tree(context.Background(), "7")
	require.NoError(t, e)
	require.Empty(t, old.Pages)
	phase.Store(2)
	_, e = d.syncCatalog(context.Background(), true, 0)
	require.NoError(t, e)
	a, e = d.store.album(context.Background(), "7")
	require.NoError(t, e)
	require.True(t, a.Active)
	_, e = d.loadTree(context.Background(), a, "7", false, true)
	require.NoError(t, e)
}
func TestFailedUncachedChapterRetryAfterRestartAndPausePersistence(t *testing.T) {
	var bad atomic.Bool
	bad.Store(true)
	var requests atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if bad.Load() {
			w.WriteHeader(404)
			return
		}
		io.WriteString(w, template("42", "https://images.example", []string{"00001.png"}))
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	_, e := d.loadTree(context.Background(), a, "42", false, true)
	require.Error(t, e)
	require.NoError(t, d.Drop(context.Background()))
	require.NoError(t, d.Init(context.Background()))
	status, e := d.JMComicStatus(context.Background())
	require.NoError(t, e)
	require.True(t, status.Paused)
	require.EqualValues(t, 1, status.Failed)
	bad.Store(false)
	require.NoError(t, d.JMComicAction(context.Background(), "retry", ""))
	require.Eventually(t, func() bool { v, e := d.store.tree(context.Background(), "42"); return e == nil && len(v.Pages) == 1 }, 3*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 2, requests.Load())
	status, e = d.JMComicStatus(context.Background())
	require.NoError(t, e)
	require.True(t, status.Paused)
	require.NoError(t, d.JMComicAction(context.Background(), "incremental", ""))
	require.NoError(t, d.JMComicAction(context.Background(), "full", ""))
	d.mu.Lock()
	require.Equal(t, "full", d.state.Requested)
	d.mu.Unlock()
}
func TestManualRefreshFailureKeepsDetailAndNormalRefreshNoRequest(t *testing.T) {
	var requests atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(404) }))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	a := seedAlbum(t, d, "42", "Old", "2026-10-07", 1)
	dt := detail{ID: "42", Name: "Old", Series: []chapter{{ID: "42", Name: "Chapter", Sort: "1"}}}
	require.NoError(t, d.store.saveDetail(context.Background(), a, dt))
	a, e := d.store.album(context.Background(), "42")
	require.NoError(t, e)
	_, e = d.loadDetail(context.Background(), a, true, true)
	require.Error(t, e)
	current, e := d.store.album(context.Background(), "42")
	require.NoError(t, e)
	require.Equal(t, "Old", current.Detail.Name)
	o, e := d.Get(context.Background(), a.Album.canonical())
	require.NoError(t, e)
	_, e = d.List(context.Background(), o, model.ListArgs{Refresh: true})
	require.NoError(t, e)
	require.EqualValues(t, 1, requests.Load())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = d.loadTree(ctx, a, "42", false, true)
	require.True(t, errors.Is(e, context.Canceled))
}
func TestFeedsLimitTagsAndCacheOnlyTraversal(t *testing.T) {
	var requests atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/categories" {
			replyJSON(w, r, map[string]any{"blocks": []any{}})
			return
		}
		if r.URL.Path == "/promote" {
			replyJSON(w, r, []section{{ID: "3", Title: "Recommended", Type: "promote", Content: []album{{ID: "42", Name: "Example"}}}})
			return
		}
		items := []album{{ID: "42", Name: "Example", Date: "2026-10-07", Updated: 1}, {ID: "43", Name: "Second", Date: "2026-10-07", Updated: 1}, {ID: "44", Name: "Third", Date: "2026-10-07", Updated: 1}}
		replyJSON(w, r, map[string]any{"content": items})
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	d.FeedLimit = 2
	d.Tags = "custom"
	require.NoError(t, d.syncFeeds(context.Background()))
	o, e := d.Get(context.Background(), "/标签/custom")
	require.NoError(t, e)
	list, e := d.List(context.Background(), o, model.ListArgs{})
	require.NoError(t, e)
	require.Len(t, list, 2)
	before := requests.Load()
	list, e = d.ListCached(context.Background(), o, model.ListArgs{})
	require.NoError(t, e)
	require.Empty(t, list)
	require.Equal(t, before, requests.Load())
}
func TestAnonymousSessionCookiesAndCoalescing(t *testing.T) {
	var settings, calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/setting" {
			settings.Add(1)
			http.SetCookie(w, &http.Cookie{Name: "AVS", Value: "anonymous-only", Path: "/"})
			replyJSON(w, r, map[string]any{})
			return
		}
		cookie, e := r.Cookie("AVS")
		if e != nil || cookie.Value != "anonymous-only" {
			w.WriteHeader(403)
			return
		}
		calls.Add(1)
		replyJSON(w, r, map[string]any{"content": []album{}})
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() { _, e := d.catalogPage(context.Background(), 1, "0", "mr", true); require.NoError(t, e) })
	}
	wg.Wait()
	require.EqualValues(t, 1, settings.Load())
	require.EqualValues(t, 5, calls.Load())
}
func TestPausedAutomaticRetriesWaitForExplicitRetry(t *testing.T) {
	var calls atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, template("42", "https://images.example", []string{}))
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	require.NoError(t, d.store.queue(context.Background(), "chapter", "42/42"))
	time.Sleep(1200 * time.Millisecond)
	require.EqualValues(t, 0, calls.Load())
	require.NoError(t, d.JMComicAction(context.Background(), "retry", ""))
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 3*time.Second, 20*time.Millisecond)
}
func TestMetadataCommitSearchesCurrentPath(t *testing.T) {
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected upstream request") }))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	old := seedAlbum(t, d, "42", "Old", "2026-07-21", 1)
	next := seedAlbum(t, d, "42", "Renamed", "2026-07-22", 2)
	require.NoError(t, d.store.saveDetail(context.Background(), old, detail{ID: "42", Name: "Old", Series: []chapter{{ID: "42", Name: "Chapter"}}}))
	require.NoError(t, d.store.saveTree(context.Background(), old.Album, chapterTree{ID: "42", AlbumID: "42", Version: 1, Pages: []page{}}))
	var nodes []model.SearchNode
	require.NoError(t, d.SearchCached(context.Background(), model.SearchReq{Parent: "/", Keywords: "Chapter", Scope: 1}, func(n model.SearchNode) error { nodes = append(nodes, n); return nil }))
	require.Equal(t, []model.SearchNode{{Parent: next.Album.canonical(), Name: "001 Chapter [42]", IsDir: true}}, nodes)
}
