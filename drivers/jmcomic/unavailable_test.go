package jmcomic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestEmptyAlbumDetailsKeepDirectoryBrowsable(t *testing.T) {
	for _, mode := range []string{"client", "server"} {
		t.Run(mode, func(t *testing.T) {
			var requests, images atomic.Int32
			var available atomic.Bool
			s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/album" {
					images.Add(1)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				requests.Add(1)
				if !available.Load() {
					io.WriteString(w, `{"code":200,"data":[]}`)
					return
				}
				replyJSON(w, r, detail{ID: "42", Name: "Example", Series: []chapter{{ID: "42", Name: "Chapter", Sort: "1"}}})
			}))
			defer s.Close()
			d := newTestDriver(t, s.URL, t.TempDir(), mode)
			ctx := context.Background()
			a := seedAlbum(t, d, "42", "Example", "", 1)
			album, err := d.Get(ctx, a.Album.canonical())
			require.NoError(t, err)
			files, err := d.List(ctx, album, model.ListArgs{})
			require.NoError(t, err)
			require.Len(t, files, 1)
			require.Equal(t, "README.md", files[0].GetName())
			manifest, err := d.Other(ctx, model.OtherArgs{Obj: album, Method: "jm_reader"})
			require.NoError(t, err)
			b, err := json.Marshal(manifest)
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(b, &fields))
			require.Equal(t, true, fields["unavailable"])
			require.Empty(t, manifest.(*driver.JMReaderManifest).Chapters)
			require.EqualValues(t, 1, requests.Load())
			// An empty success envelope does not confirm a removal or cache an
			// invented empty work. Its negative result survives a restart.
			cached, err := d.store.album(ctx, "42")
			require.NoError(t, err)
			require.True(t, cached.Active)
			require.Nil(t, cached.Detail)
			_, err = d.loadDetail(ctx, cached, false, true)
			require.Error(t, err)
			require.False(t, unavailable(err), "empty success payload must not confirm removal")
			require.NoError(t, d.Drop(ctx))
			require.NoError(t, d.Init(ctx))
			_, err = d.List(ctx, album, model.ListArgs{Refresh: true})
			require.NoError(t, err)
			require.EqualValues(t, 1, requests.Load())
			available.Store(true)
			manifest, err = d.Other(ctx, model.OtherArgs{Obj: album, Method: "jm_reader", Data: map[string]any{"refresh": true}})
			require.NoError(t, err)
			require.Len(t, manifest.(*driver.JMReaderManifest).Chapters, 1)
			require.EqualValues(t, 2, requests.Load())
			require.Zero(t, images.Load())
		})
	}
}

func TestEmptyDetailsPreservePreviouslyCachedWork(t *testing.T) {
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"code":200,"data":[]}`)
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	ctx := context.Background()
	a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	dt := detail{ID: "42", Name: "Saved", Series: []chapter{{ID: "42", Name: "Chapter", Sort: "1"}}}
	require.NoError(t, d.store.saveDetail(ctx, a, dt))
	a.Album.Updated = 2
	require.NoError(t, d.store.upsert(ctx, []album{a.Album}, ""))
	o, err := d.Get(ctx, a.Album.canonical())
	require.NoError(t, err)
	m, err := d.Other(ctx, model.OtherArgs{Obj: o, Method: "jm_reader"})
	require.NoError(t, err)
	require.Len(t, m.(*driver.JMReaderManifest).Chapters, 1)
	m, err = d.Other(ctx, model.OtherArgs{Obj: o, Method: "jm_reader", Data: map[string]any{"refresh": true}})
	require.NoError(t, err)
	require.True(t, m.(*driver.JMReaderManifest).Unavailable)
	require.Len(t, m.(*driver.JMReaderManifest).Chapters, 1)
	cached, err := d.store.album(ctx, "42")
	require.NoError(t, err)
	require.Equal(t, dt, *cached.Detail)
	require.True(t, cached.Active)
}

func TestEmptyDetailsDoNotConfirmRemoval(t *testing.T) {
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/album" {
			io.WriteString(w, `{"code":200,"data":[]}`)
		} else {
			replyJSON(w, r, map[string]any{"content": []album{}})
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	_, err := d.syncCatalog(context.Background(), true, 0)
	require.Error(t, err)
	cached, err := d.store.album(context.Background(), string(a.Album.ID))
	require.NoError(t, err)
	require.True(t, cached.Active)
}

func TestEmptyAlbumEnvelopeShapes(t *testing.T) {
	for _, shape := range []string{"[]", "[ ]", "{}", "null", `""`, "encrypted empty array"} {
		t.Run(shape, func(t *testing.T) {
			var requests atomic.Int32
			s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if shape == "encrypted empty array" {
					replyJSON(w, r, []album{})
				} else {
					io.WriteString(w, `{"code":200,"data":`+shape+`}`)
				}
			}))
			defer s.Close()
			d := newTestDriver(t, s.URL, t.TempDir(), "client")
			_, err := d.fetchDetail(context.Background(), "42", true)
			require.ErrorIs(t, err, errNoAlbumDetail)
			require.False(t, unavailable(err))
			require.EqualValues(t, 1, requests.Load())
			require.False(t, strings.Contains(err.Error(), "unexpected JMComic metadata payload"))
		})
	}
}
