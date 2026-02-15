package op

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"golang.org/x/time/rate"
)

type fakeListRBatch struct {
	parent  string
	entries []model.Obj
}

type fakeListRStorage struct {
	model.Storage
	addition     driver.RootPath
	listMap      map[string][]model.Obj
	listRErr     error
	listRBatches []fakeListRBatch
	listCalls    int
	listRCalls   int
}

func (f *fakeListRStorage) Config() driver.Config {
	return driver.Config{Name: "FakeListR"}
}

func (f *fakeListRStorage) GetAddition() driver.Additional {
	return &f.addition
}

func (f *fakeListRStorage) Init(ctx context.Context) error {
	return nil
}

func (f *fakeListRStorage) Drop(ctx context.Context) error {
	return nil
}

func (f *fakeListRStorage) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	f.listCalls++
	p := utils.FixAndCleanPath(dir.GetPath())
	src := f.listMap[p]
	out := make([]model.Obj, 0, len(src))
	for i := range src {
		raw := model.UnwrapObj(src[i])
		obj := &model.Object{
			ID:       raw.GetID(),
			Path:     raw.GetPath(),
			Name:     raw.GetName(),
			Size:     raw.GetSize(),
			Modified: raw.ModTime(),
			Ctime:    raw.CreateTime(),
			IsFolder: raw.IsDir(),
			HashInfo: raw.GetHash(),
		}
		out = append(out, obj)
	}
	return out, nil
}

func (f *fakeListRStorage) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	return nil, nil
}

func (f *fakeListRStorage) ListR(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	f.listRCalls++
	if f.listRErr != nil {
		return f.listRErr
	}
	for i := range f.listRBatches {
		if err := callback(f.listRBatches[i].parent, f.listRBatches[i].entries); err != nil {
			return err
		}
	}
	return nil
}

func newFakeObj(name string, isDir bool) model.Obj {
	return &model.Object{Name: name, IsFolder: isDir}
}

func TestWalkStorageRecursive_UsesListRWhenEnabled(t *testing.T) {
	storage := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/fake", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRBatches: []fakeListRBatch{
			{parent: "/", entries: []model.Obj{newFakeObj("a", true), newFakeObj("f", false)}},
			{parent: "/a", entries: []model.Obj{newFakeObj("x", false)}},
		},
	}
	var got int
	err := WalkStorageRecursive(context.Background(), storage, "/", -1, model.ListArgs{}, true, nil,
		func(parent string, objs []model.Obj) error {
			got += len(objs)
			return nil
		})
	if err != nil {
		t.Fatalf("WalkStorageRecursive error: %v", err)
	}
	if storage.listRCalls != 1 {
		t.Fatalf("expected 1 ListR call, got %d", storage.listRCalls)
	}
	if storage.listCalls != 0 {
		t.Fatalf("expected 0 List calls, got %d", storage.listCalls)
	}
	if got != 3 {
		t.Fatalf("expected 3 objs, got %d", got)
	}
}

func TestWalkStorageRecursive_FallbackWhenListRFails(t *testing.T) {
	storage := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/fake", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRErr: errors.New("boom"),
		listMap: map[string][]model.Obj{
			"/":  {newFakeObj("a", true), newFakeObj("f", false)},
			"/a": {newFakeObj("x", false)},
		},
	}
	var got int
	err := WalkStorageRecursive(context.Background(), storage, "/", -1, model.ListArgs{}, true, nil,
		func(parent string, objs []model.Obj) error {
			got += len(objs)
			return nil
		})
	if err != nil {
		t.Fatalf("WalkStorageRecursive error: %v", err)
	}
	if storage.listRCalls != 1 {
		t.Fatalf("expected 1 ListR call, got %d", storage.listRCalls)
	}
	if storage.listCalls < 2 {
		t.Fatalf("expected fallback List calls >= 2, got %d", storage.listCalls)
	}
	if got != 3 {
		t.Fatalf("expected 3 objs after fallback, got %d", got)
	}
}

func TestWalkStorageRecursive_MaxDepthWithListFallback(t *testing.T) {
	storage := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/fake", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listMap: map[string][]model.Obj{
			"/":    {newFakeObj("a", true), newFakeObj("f", false)},
			"/a":   {newFakeObj("x", true), newFakeObj("y", false)},
			"/a/x": {newFakeObj("z", false)},
		},
	}
	var got int
	err := WalkStorageRecursive(context.Background(), storage, "/", 1, model.ListArgs{}, false, nil,
		func(parent string, objs []model.Obj) error {
			got += len(objs)
			return nil
		})
	if err != nil {
		t.Fatalf("WalkStorageRecursive error: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected 2 objs at depth 1, got %d", got)
	}
}

func TestWalkStorageRecursive_ListRLimiterByBatch(t *testing.T) {
	storage := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/fake", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRBatches: []fakeListRBatch{
			{parent: "/", entries: []model.Obj{newFakeObj("a", false)}},
			{parent: "/", entries: []model.Obj{newFakeObj("b", false)}},
		},
	}
	limiter := rate.NewLimiter(50, 1) // 20ms/token
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatalf("failed to consume initial token: %v", err)
	}
	start := time.Now()
	err := WalkStorageRecursive(context.Background(), storage, "/", -1, model.ListArgs{}, true, limiter,
		func(parent string, objs []model.Obj) error { return nil })
	if err != nil {
		t.Fatalf("WalkStorageRecursive error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("expected ListR limiter wait, elapsed=%v", elapsed)
	}
}
