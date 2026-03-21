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

func (f *fakePlainStorage) Config() driver.Config {
	return driver.Config{Name: "FakePlain"}
}

func (f *fakePlainStorage) GetAddition() driver.Additional {
	return &f.addition
}

func (f *fakePlainStorage) Init(ctx context.Context) error {
	return nil
}

func (f *fakePlainStorage) Drop(ctx context.Context) error {
	return nil
}

func (f *fakePlainStorage) GetRoot(ctx context.Context) (model.Obj, error) {
	return &model.Object{
		Name:     "root",
		Path:     "/",
		IsFolder: true,
	}, nil
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

func TestWalkUpdateSitePublicChunks_UsesChildUpdateSiteScannerUnderRootStorage(t *testing.T) {
	prepareTestStorages(t)

	root := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listMap:  map[string][]model.Obj{"/": {}},
	}
	child := &fakeUpdateSiteStorage{
		fakeListRStorage: &fakeListRStorage{
			Storage:  model.Storage{MountPath: "/3D", CacheExpiration: 30},
			addition: driver.RootPath{RootFolderPath: "/"},
		},
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Parent: "/", Entries: []model.Obj{newFakeObj("Derpixon", true)}, ParentDone: true},
			{Parent: "/Derpixon", Entries: []model.Obj{newFakeObj("2021", true)}, ParentDone: true},
		},
	}
	registerTestStorage(root)
	registerTestStorage(child)

	gotParents := make([]string, 0, 3)
	if err := WalkUpdateSitePublicChunks(context.Background(), "/", -1, model.ListArgs{Refresh: true}, func(chunk driver.UpdateSiteChunk) error {
		gotParents = append(gotParents, chunk.Parent)
		return nil
	}); err != nil {
		t.Fatalf("WalkUpdateSitePublicChunks error: %v", err)
	}
	if root.listCalls != 1 {
		t.Fatalf("expected root storage to list current level once, got %d calls", root.listCalls)
	}
	if child.updateSiteCalls != 1 {
		t.Fatalf("expected child storage to use update-site scanner, got %d calls", child.updateSiteCalls)
	}
	if len(gotParents) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(gotParents))
	}
	if gotParents[0] != "/" || gotParents[1] != "/3D" || gotParents[2] != "/3D/Derpixon" {
		t.Fatalf("unexpected parent chunks: %#v", gotParents)
	}
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
			{Parent: "/", Entries: []model.Obj{newFakeObj("Derpixon", true)}, ParentDone: true},
		},
	}
	childASMR := &fakeUpdateSiteStorage{
		fakeListRStorage: &fakeListRStorage{
			Storage:  model.Storage{MountPath: "/ASMR", CacheExpiration: 30},
			addition: driver.RootPath{RootFolderPath: "/"},
		},
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Parent: "/", Entries: []model.Obj{newFakeObj("Whisper", true)}, ParentDone: true},
		},
	}
	registerTestStorage(root)
	registerTestStorage(child3D)
	registerTestStorage(childASMR)

	gotParents := make([]string, 0, 3)
	if err := WalkUpdateSitePublicChunks(context.Background(), "/", -1, model.ListArgs{Refresh: true}, func(chunk driver.UpdateSiteChunk) error {
		gotParents = append(gotParents, chunk.Parent)
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
	if len(gotParents) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(gotParents))
	}
	if gotParents[0] != "/" {
		t.Fatalf("expected first chunk at root, got %#v", gotParents)
	}
	if gotParents[1] != "/3D" || gotParents[2] != "/ASMR" {
		t.Fatalf("expected child chunks for /3D and /ASMR, got %#v", gotParents)
	}
}

func TestWalkUpdateSiteStorageChunks_CollapsesGenericListRIntoParentDoneBoundaries(t *testing.T) {
	storage := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/fake", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRBatches: []fakeListRBatch{
			{parent: "/", entries: []model.Obj{newFakeObj("a", false)}},
			{parent: "/", entries: []model.Obj{newFakeObj("b", false)}},
			{parent: "/dir", entries: []model.Obj{newFakeObj("c", false)}},
		},
	}
	got := make([]driver.UpdateSiteChunk, 0, 3)
	if err := WalkUpdateSiteStorageChunks(context.Background(), storage, "/", -1, model.ListArgs{}, func(chunk driver.UpdateSiteChunk) error {
		got = append(got, chunk)
		return nil
	}); err != nil {
		t.Fatalf("WalkUpdateSiteStorageChunks error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(got))
	}
	if got[0].Parent != "/" || got[0].ParentDone {
		t.Fatalf("expected first root chunk with parent_done=false, got %+v", got[0])
	}
	if got[1].Parent != "/" || !got[1].ParentDone {
		t.Fatalf("expected second root chunk with parent_done=true, got %+v", got[1])
	}
	if got[2].Parent != "/dir" || !got[2].ParentDone {
		t.Fatalf("expected child chunk with parent_done=true, got %+v", got[2])
	}
}
