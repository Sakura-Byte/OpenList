package server_test

import (
	"encoding/json"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func TestMetadataSingleSaveVisibleAfterOverlappingRead(t *testing.T) {
	f := newHiddenPathFixture(t)
	meta := model.Meta{Path: "/hidden-path-tests", Header: "old header", Readme: "old readme"}
	f.saveMeta(t, meta)
	old, err := op.GetMetaByPath(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = old.ID
	// Read while the update is about to write, reproducing a folder request
	// overlapping the first save without relying on timing or goroutines.
	const callback = "test:metadata_overlapping_read"
	err = db.GetDb().Callback().Update().Before("gorm:begin_transaction").Register(callback, func(tx *gorm.DB) {
		if _, ok := tx.Statement.Dest.(*model.Meta); ok {
			if _, err := op.GetMetaByPath(meta.Path); err != nil {
				t.Error(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.GetDb().Callback().Update().Remove(callback) })
	meta.Header, meta.Readme = "new header", "new readme"
	f.post(t, "/api/admin/meta/update", hiddenPathAdminToken, meta, 200)
	rec := f.post(t, "/api/fs/list", "", map[string]any{"path": meta.Path}, 200)
	var resp struct {
		Data struct {
			Header string `json:"header"`
			Readme string `json:"readme"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Data.Header != meta.Header || resp.Data.Readme != meta.Readme {
		t.Fatalf("first save returned success but folder still shows header=%q readme=%q", resp.Data.Header, resp.Data.Readme)
	}
}

func TestMetadataReadStartedBeforeSaveCannotRestoreOldCache(t *testing.T) {
	f := newHiddenPathFixture(t)
	meta := model.Meta{Path: "/hidden-path-tests", Readme: "old readme"}
	f.saveMeta(t, meta)
	old, err := db.GetMetaByPath(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	meta.ID = old.ID
	readStarted, releaseRead, readDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var blocked atomic.Bool
	const callback = "test:metadata_delayed_read"
	err = db.GetDb().Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if m, ok := tx.Statement.Dest.(*model.Meta); ok && m.Path == meta.Path && blocked.CompareAndSwap(false, true) {
			close(readStarted)
			<-releaseRead
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.GetDb().Callback().Query().Remove(callback) })
	go func() {
		_, err := op.GetMetaByPath(meta.Path)
		readDone <- err
	}()
	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		close(releaseRead)
		t.Fatal("read did not reach query callback")
	}
	func() {
		defer close(releaseRead)
		meta.Readme = "new readme"
		f.post(t, "/api/admin/meta/update", hiddenPathAdminToken, meta, 200)
		// A read after the save must not join the old singleflight call.
		done := make(chan *model.Meta, 1)
		go func() {
			m, _ := op.GetMetaByPath(meta.Path)
			done <- m
		}()
		select {
		case m := <-done:
			if m == nil || m.Readme != meta.Readme {
				t.Errorf("post-save read did not see new metadata: %+v", m)
			}
		case <-time.After(5 * time.Second):
			t.Error("post-save read joined the stale in-flight read")
		}
	}()
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	got, err := op.GetMetaByPath(meta.Path)
	if err != nil || got.Readme != meta.Readme {
		t.Fatalf("old read restored stale cache: meta=%+v err=%v", got, err)
	}
}

func TestMetadataGridBatch(t *testing.T) {
	f := newHiddenPathFixture(t)
	first := model.Meta{
		Path: "/hidden-path-tests", Password: "old password", PSub: true,
		Write: true, WSub: true, Hide: "private", HSub: true,
		Header: "old header", HeaderSub: true, Readme: "old readme", RSub: true,
		ReadUsers: []uint{3}, ReadUsersSub: true, WriteUsers: []uint{4}, WriteUsersSub: true,
	}
	second := model.Meta{Path: "/hidden-path-tests/hidden", Header: "second header"}
	reset := func() {
		f.saveMeta(t, first)
		f.saveMeta(t, second)
		m, err := op.GetMetaByPath(first.Path)
		if err != nil {
			t.Fatal(err)
		}
		first.ID = m.ID
		m, err = op.GetMetaByPath(second.Path)
		if err != nil {
			t.Fatal(err)
		}
		second.ID = m.ID
	}
	reset()
	endpoint := "/api/admin/meta/update_grid"
	batch := func(metas ...model.MetaGridUpdate) any {
		return map[string]any{"metas": metas}
	}
	assertUnchanged := func() {
		for _, want := range []model.Meta{first, second} {
			got, err := db.GetMetaById(want.ID)
			if err != nil || !reflect.DeepEqual(got, &want) {
				t.Fatalf("failed batch changed record: got=%+v want=%+v err=%v", got, want, err)
			}
			cached, err := op.GetMetaByPath(want.Path)
			if err != nil || !reflect.DeepEqual(cached, &want) {
				t.Fatalf("failed batch changed cached record: %+v, %v", cached, err)
			}
		}
	}
	t.Run("admin only", func(t *testing.T) {
		f.post(t, endpoint, f.tokens["member"], batch(model.MetaGridUpdate{ID: first.ID, Path: first.Path}), 403)
		f.post(t, endpoint, "", batch(model.MetaGridUpdate{ID: first.ID, Path: first.Path}), 403)
		assertUnchanged()
	})
	t.Run("invalid batches are rejected before writes", func(t *testing.T) {
		for _, body := range []any{
			map[string]any{}, batch(), batch([]model.MetaGridUpdate{}...), batch(model.MetaGridUpdate{Path: first.Path}),
			batch(model.MetaGridUpdate{ID: first.ID}),
			batch(model.MetaGridUpdate{ID: first.ID, Path: first.Path}, model.MetaGridUpdate{ID: first.ID, Path: second.Path}),
			batch(model.MetaGridUpdate{ID: first.ID, Path: first.Path}, model.MetaGridUpdate{ID: second.ID, Path: first.Path + "/"}),
			batch(model.MetaGridUpdate{ID: first.ID, Path: first.Path, Readme: "should not save"}, model.MetaGridUpdate{ID: second.ID, Path: second.Path, Hide: "["}),
		} {
			f.post(t, endpoint, hiddenPathAdminToken, body, 400)
			assertUnchanged()
		}
		f.request(t, http.MethodPost, endpoint, hiddenPathAdminToken, "malformed payload", 400)
	})
	t.Run("database errors roll back all writes", func(t *testing.T) {
		for _, bad := range []model.MetaGridUpdate{
			{ID: 999999, Path: "/missing"},
			{ID: second.ID, Path: "/occupied"},
		} {
			if bad.ID == second.ID {
				f.saveMeta(t, model.Meta{Path: "/occupied"})
			}
			f.post(t, endpoint, hiddenPathAdminToken, batch(
				model.MetaGridUpdate{ID: first.ID, Path: first.Path, Readme: "should roll back"}, bad,
			), 500)
			assertUnchanged()
		}
	})
	t.Run("one batch saves all values and preserves access lists", func(t *testing.T) {
		f.post(t, endpoint, hiddenPathAdminToken, batch(
			model.MetaGridUpdate{ID: first.ID, Path: first.Path},
			model.MetaGridUpdate{ID: second.ID, Path: second.Path, Header: "new header", HeaderSub: true, Readme: "new readme", RSub: true},
		), 200)
		got, err := op.GetMetaByPath(first.Path)
		want := model.Meta{ID: first.ID, Path: first.Path, ReadUsers: first.ReadUsers, ReadUsersSub: true, WriteUsers: first.WriteUsers, WriteUsersSub: true}
		if err != nil || !reflect.DeepEqual(got, &want) {
			t.Fatalf("zero values or access lists were lost: got=%+v want=%+v err=%v", got, want, err)
		}
		got, err = op.GetMetaByPath(second.Path)
		if err != nil || got.Header != "new header" || got.Readme != "new readme" || !got.HeaderSub || !got.RSub {
			t.Fatalf("second record not updated: %+v, %v", got, err)
		}
	})
	t.Run("moves invalidate old and negative new path caches", func(t *testing.T) {
		newPath := "/hidden-path-tests/moved"
		if _, err := op.GetMetaByPath(newPath); !errors.Is(err, errs.MetaNotFound) {
			t.Fatalf("expected missing metadata: %v", err)
		}
		f.post(t, endpoint, hiddenPathAdminToken, batch(model.MetaGridUpdate{ID: second.ID, Path: newPath + "/", Readme: "moved readme"}), 200)
		if _, err := op.GetMetaByPath(second.Path); !errors.Is(err, errs.MetaNotFound) {
			t.Fatalf("old path cache not cleared: %v", err)
		}
		got, err := op.GetMetaByPath(newPath)
		if err != nil || got.Readme != "moved readme" || got.Path != newPath {
			t.Fatalf("new path cache not cleared: %+v, %v", got, err)
		}
	})
}
