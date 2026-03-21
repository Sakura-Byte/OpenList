package op

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type fakeUpdateSiteStorage struct {
	*fakeListRStorage
	updateSiteChunks []driver.UpdateSiteChunk
	updateSiteCalls  int
}

type fakePlainStorage struct {
	model.Storage
	addition  driver.RootPath
	listMap   map[string][]model.Obj
	listCalls int
}

func (f *fakePlainStorage) Config() driver.Config          { return driver.Config{Name: "FakePlain"} }
func (f *fakePlainStorage) GetAddition() driver.Additional { return &f.addition }
func (f *fakePlainStorage) Init(ctx context.Context) error { return nil }
func (f *fakePlainStorage) Drop(ctx context.Context) error { return nil }
func (f *fakePlainStorage) GetRoot(ctx context.Context) (model.Obj, error) {
	return &model.Object{Name: "root", Path: "/", IsFolder: true}, nil
}
func (f *fakePlainStorage) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	f.listCalls++
	return append([]model.Obj(nil), f.listMap[dir.GetPath()]...), nil
}
func (f *fakePlainStorage) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	return nil, nil
}

func (f *fakeUpdateSiteStorage) ListRForUpdateSite(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	f.updateSiteCalls++
	for _, chunk := range f.updateSiteChunks {
		if err := callback(chunk); err != nil {
			return err
		}
	}
	return nil
}

func TestWalkUpdateSitePublicChunks_UsesChildUpdateSiteScannerUnderLocalRootStorage(t *testing.T) {
	prepareTestStorages(t)

	root := &fakePlainStorage{
		Storage:  model.Storage{MountPath: "/", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listMap: map[string][]model.Obj{
			"/": {newFakeObj("local.txt", false)},
		},
	}
	child3D := &fakeUpdateSiteStorage{
		fakeListRStorage: &fakeListRStorage{
			Storage:  model.Storage{MountPath: "/3D", CacheExpiration: 30},
			addition: driver.RootPath{RootFolderPath: "/"},
		},
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/Derpixon", ParentPath: "/", Name: "Derpixon", IsDir: true}}},
		},
	}
	childASMR := &fakeUpdateSiteStorage{
		fakeListRStorage: &fakeListRStorage{
			Storage:  model.Storage{MountPath: "/ASMR", CacheExpiration: 30},
			addition: driver.RootPath{RootFolderPath: "/"},
		},
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/Whisper", ParentPath: "/", Name: "Whisper", IsDir: true}}},
		},
	}
	registerTestStorage(root)
	registerTestStorage(child3D)
	registerTestStorage(childASMR)

	gotPaths := make([]string, 0, 5)
	if err := WalkUpdateSitePublicChunks(context.Background(), "/", -1, model.ListArgs{Refresh: true}, func(chunk driver.UpdateSiteChunk) error {
		for _, entry := range chunk.Entries {
			gotPaths = append(gotPaths, entry.VisiblePath)
		}
		return nil
	}); err != nil {
		t.Fatalf("WalkUpdateSitePublicChunks error: %v", err)
	}
	if root.listCalls != 1 {
		t.Fatalf("expected local root storage to list once, got %d calls", root.listCalls)
	}
	if child3D.updateSiteCalls != 1 || childASMR.updateSiteCalls != 1 {
		t.Fatalf("expected both child storages to use update-site scanner, 3D=%d ASMR=%d", child3D.updateSiteCalls, childASMR.updateSiteCalls)
	}
	expected := map[string]bool{
		"/local.txt":    true,
		"/3D":           true,
		"/ASMR":         true,
		"/3D/Derpixon":  true,
		"/ASMR/Whisper": true,
	}
	if len(gotPaths) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %#v", len(expected), len(gotPaths), gotPaths)
	}
	for _, got := range gotPaths {
		if !expected[got] {
			t.Fatalf("unexpected visible path: %s in %#v", got, gotPaths)
		}
	}
}

func TestWalkUpdateSitePublicChunks_DebugShowsEnginePerEntry(t *testing.T) {
	prepareTestStorages(t)

	root := &fakePlainStorage{
		Storage:  model.Storage{MountPath: "/", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listMap:  map[string][]model.Obj{"/": {newFakeObj("local.txt", false)}},
	}
	child := &fakeUpdateSiteStorage{
		fakeListRStorage: &fakeListRStorage{
			Storage:  model.Storage{MountPath: "/3D", CacheExpiration: 30},
			addition: driver.RootPath{RootFolderPath: "/"},
		},
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/Derpixon", ParentPath: "/", Name: "Derpixon", IsDir: true, Debug: &driver.UpdateSiteEntryDebug{Engine: "update_site_delta"}}}},
		},
	}
	registerTestStorage(root)
	registerTestStorage(child)

	got := make([]driver.UpdateSiteEntry, 0, 3)
	if err := WalkUpdateSitePublicChunks(context.Background(), "/", -1, model.ListArgs{Refresh: true}, func(chunk driver.UpdateSiteChunk) error {
		got = append(got, chunk.Entries...)
		return nil
	}); err != nil {
		t.Fatalf("WalkUpdateSitePublicChunks error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	seenRootList := false
	seenVirtual := false
	seenChild := false
	for _, entry := range got {
		switch entry.VisiblePath {
		case "/local.txt":
			seenRootList = entry.Debug != nil && entry.Debug.Engine == "list" && entry.Debug.StorageDriver == "FakePlain"
		case "/3D":
			seenVirtual = entry.Debug != nil && entry.Debug.Engine == "virtual"
		case "/3D/Derpixon":
			seenChild = entry.Debug != nil && entry.Debug.Engine == "update_site_delta"
		}
	}
	if !seenRootList || !seenVirtual || !seenChild {
		t.Fatalf("unexpected debug entries: %+v", got)
	}
}

func TestWalkUpdateSiteStorageChunks_FlattensGenericListR(t *testing.T) {
	storage := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/fake", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRBatches: []fakeListRBatch{
			{parent: "/", entries: []model.Obj{newFakeObj("a", false)}},
			{parent: "/", entries: []model.Obj{newFakeObj("b", false)}},
			{parent: "/dir", entries: []model.Obj{newFakeObj("c", false)}},
		},
	}
	got := make([]driver.UpdateSiteEntry, 0, 3)
	if err := WalkUpdateSiteStorageChunks(context.Background(), storage, "/", -1, model.ListArgs{}, func(chunk driver.UpdateSiteChunk) error {
		got = append(got, chunk.Entries...)
		return nil
	}); err != nil {
		t.Fatalf("WalkUpdateSiteStorageChunks error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if got[0].VisiblePath != "/a" || got[1].VisiblePath != "/b" || got[2].VisiblePath != "/dir/c" {
		t.Fatalf("unexpected flattened entries: %+v", got)
	}
}
