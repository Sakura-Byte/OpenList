package asmrone

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestModifiedTracksSuccessfulCacheChanges(t *testing.T) {
	d := newTestDriver(t, "http://127.0.0.1:1", t.TempDir())
	ctx := context.Background()
	w := seedWork(t, d, 117765, "Original")
	initial := w.modified()
	require.False(t, initial.IsZero())
	require.Equal(t, w.ModifiedAt, initial.UnixMilli())
	// Re-reading an unchanged page updates sync bookkeeping, not Modified.
	require.NoError(t, d.store.upsertPage(ctx, []work{w.work}, "next-scan"))
	unchanged, err := d.store.byID(ctx, int64(w.ID))
	require.NoError(t, err)
	require.Equal(t, initial, unchanged.modified())
	// A known earlier value makes the metadata-change assertion deterministic.
	_, err = d.store.db.Exec(`UPDATE work_changes SET modified_at=1000 WHERE work_id=?`, int64(w.ID))
	require.NoError(t, err)
	w.Title = "Renamed"
	require.NoError(t, d.store.upsertPage(ctx, []work{w.work}, ""))
	renamed, err := d.store.byID(ctx, int64(w.ID))
	require.NoError(t, err)
	require.Greater(t, renamed.ModifiedAt, int64(1000))
	// Successful file-tree replacement advances the same work's timestamp.
	savedAt := renamed.modified().Unix() + 10
	require.NoError(t, d.store.saveTracks(ctx, renamed, []track{{Type: "audio", Title: "audio.mp3", Stream: "https://media.example/audio"}}, savedAt))
	w, err = d.store.byID(ctx, int64(w.ID))
	require.NoError(t, err)
	expected := time.Unix(savedAt, 0)
	require.Equal(t, expected, w.modified())
	rangeDir, err := d.Get(ctx, "/"+w.rangeName())
	require.NoError(t, err)
	require.Equal(t, expected, rangeDir.ModTime())
	listed, err := d.List(ctx, rangeDir, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	require.Equal(t, expected, listed[0].ModTime())
	dir, err := d.Get(ctx, w.path())
	require.NoError(t, err)
	files, err := d.List(ctx, dir, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	for _, file := range files {
		require.Equal(t, expected, file.ModTime())
	}
	require.NoError(t, d.store.failTracks(ctx, int64(w.ID), "upstream unavailable", savedAt+60))
	failed, err := d.store.byID(ctx, int64(w.ID))
	require.NoError(t, err)
	require.Equal(t, expected, failed.modified())
	// Refresh and a driver reload both retain the successful change time.
	require.NoError(t, d.Drop(ctx))
	require.NoError(t, d.Init(ctx))
	dir, err = d.Get(ctx, w.path())
	require.NoError(t, err)
	require.Equal(t, expected, dir.ModTime())
	files, err = d.List(ctx, dir, model.ListArgs{Refresh: true})
	require.NoError(t, err)
	for _, file := range files {
		require.Equal(t, expected, file.ModTime())
	}
}
