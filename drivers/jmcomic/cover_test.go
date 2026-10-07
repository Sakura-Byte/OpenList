package jmcomic

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
)

func TestReaderCoverUsesCachedUpstreamImageHost(t *testing.T) {
	for _, mode := range []string{"client", "server"} {
		t.Run(mode, func(t *testing.T) {
			var settings, images atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/setting" {
					settings.Add(1)
					replyJSON(w, r, map[string]any{"img_host": "https://images.example/"})
					return
				}
				images.Add(1)
				w.WriteHeader(http.StatusNotFound)
			}))
			defer s.Close()
			d := newTestDriver(t, s.URL, t.TempDir(), mode)
			ctx := context.Background()
			a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
			require.NoError(t, d.store.saveDetail(ctx, a, detail{ID: "42", Name: "Example", Series: []chapter{{ID: "42", Name: "Chapter", Sort: "1"}}}))
			o, err := d.Get(ctx, a.Album.canonical())
			require.NoError(t, err)
			require.Zero(t, settings.Load())
			manifest, err := d.Other(ctx, model.OtherArgs{Obj: o, Method: "jm_reader"})
			require.NoError(t, err)
			cover := "https://images.example/media/albums/42_3x4.jpg"
			require.Equal(t, cover, manifest.(*driver.JMReaderManifest).Cover)
			require.EqualValues(t, 1, settings.Load())
			files, err := d.List(ctx, o, model.ListArgs{Refresh: true})
			require.NoError(t, err)
			var fileCover string
			for _, file := range files {
				if file.GetName() == "cover.jpg" {
					fileCover = file.(*object).page.URL
				}
			}
			if mode == "client" {
				require.Empty(t, fileCover)
			} else {
				require.Equal(t, cover, fileCover)
			}
			require.NoError(t, d.Drop(ctx))
			require.NoError(t, d.Init(ctx))
			manifest, err = d.Other(ctx, model.OtherArgs{Obj: o, Method: "jm_reader"})
			require.NoError(t, err)
			require.Equal(t, cover, manifest.(*driver.JMReaderManifest).Cover)
			require.NoError(t, op.WalkStorageRecursive(ctx, d, "/", -1, model.ListArgs{}, false, nil, func(_ string, _ []model.Obj) error { return nil }))
			require.EqualValues(t, 1, settings.Load())
			require.Zero(t, images.Load())
		})
	}
}

func TestCoverURLPreservesExplicitLinksAndResolvesMediaPaths(t *testing.T) {
	d := &JMComic{state: syncState{ImageHost: "https://images.example"}}
	for _, test := range []struct{ image, want string }{
		{"https://other.example/cover.jpg?token=x", "https://other.example/cover.jpg?token=x"},
		{"/media/library/album/42/thumb/album.jpg", "https://images.example/media/library/album/42/thumb/album.jpg"},
		{"", "https://images.example/media/albums/42_3x4.jpg"},
		{"//other.example/media/cover.jpg", ""},
		{"/media/../../private", ""},
		{"javascript:alert(1)", ""},
	} {
		require.Equal(t, test.want, d.coverURL(album{ID: "42", Image: test.image}))
	}
}
