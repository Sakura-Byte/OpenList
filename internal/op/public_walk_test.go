package op

import (
	"context"
	stdpath "path"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestWalkPublicRecursiveWithStats_UsesChildListRUnderRootStorage(t *testing.T) {
	prepareTestStorages(t)

	root := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listMap:  map[string][]model.Obj{"/": {}},
	}
	child := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/3D", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRBatches: []fakeListRBatch{
			{parent: "/", entries: []model.Obj{newFakeObj("Derpixon", true)}},
			{parent: "/Derpixon", entries: []model.Obj{newFakeObj("2021", true)}},
		},
	}
	registerTestStorage(root)
	registerTestStorage(child)

	gotParents := make([]string, 0, 3)
	stats, err := WalkPublicRecursiveWithStats(context.Background(), "/", -1, true, nil, func(parent string, objs []model.Obj) error {
		gotParents = append(gotParents, parent)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkPublicRecursiveWithStats error: %v", err)
	}
	if root.listRCalls != 0 {
		t.Fatalf("expected root storage to avoid ListR when child mounts exist, got %d calls", root.listRCalls)
	}
	if root.listCalls != 1 {
		t.Fatalf("expected root storage to list current level once, got %d calls", root.listCalls)
	}
	if child.listRCalls != 1 {
		t.Fatalf("expected child storage to use ListR, got %d calls", child.listRCalls)
	}
	if !stats.UsedListR {
		t.Fatalf("expected aggregated stats to report UsedListR")
	}
	if len(gotParents) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(gotParents))
	}
	if gotParents[0] != "/" || gotParents[1] != "/3D" || gotParents[2] != "/3D/Derpixon" {
		t.Fatalf("unexpected parent batches: %#v", gotParents)
	}
}

func TestWalkPublicRecursiveWithStats_AllowsVirtualIntermediatePath(t *testing.T) {
	prepareTestStorages(t)

	root := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listMap: map[string][]model.Obj{
			"/": {},
		},
	}
	child := &fakeListRStorage{
		Storage:  model.Storage{MountPath: "/a/b", CacheExpiration: 30},
		addition: driver.RootPath{RootFolderPath: "/"},
		listRBatches: []fakeListRBatch{
			{parent: "/", entries: []model.Obj{newFakeObj("leaf.txt", false)}},
		},
	}
	registerTestStorage(root)
	registerTestStorage(child)

	gotParents := make([]string, 0, 2)
	stats, err := WalkPublicRecursiveWithStats(context.Background(), "/a", -1, true, nil, func(parent string, objs []model.Obj) error {
		gotParents = append(gotParents, parent)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkPublicRecursiveWithStats error: %v", err)
	}
	if child.listRCalls != 1 {
		t.Fatalf("expected mounted child storage to use ListR, got %d calls", child.listRCalls)
	}
	if !stats.UsedListR {
		t.Fatalf("expected aggregated stats to report UsedListR")
	}
	if len(gotParents) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(gotParents))
	}
	if gotParents[0] != "/a" || gotParents[1] != "/a/b" {
		t.Fatalf("unexpected parent batches: %#v", gotParents)
	}
}

func prepareTestStorages(t *testing.T) {
	t.Helper()

	Cache.ClearAll()
	entries := make([]struct {
		key   string
		value driver.Driver
	}, 0)
	storagesMap.Range(func(key string, value driver.Driver) bool {
		entries = append(entries, struct {
			key   string
			value driver.Driver
		}{key: key, value: value})
		return true
	})
	for _, entry := range entries {
		storagesMap.Delete(entry.key)
	}
	t.Cleanup(func() {
		Cache.ClearAll()
		currentKeys := make([]string, 0)
		storagesMap.Range(func(key string, value driver.Driver) bool {
			currentKeys = append(currentKeys, key)
			return true
		})
		for _, key := range currentKeys {
			storagesMap.Delete(key)
		}
		for _, entry := range entries {
			storagesMap.Store(entry.key, entry.value)
		}
	})
}

func registerTestStorage(storage driver.Driver) {
	storage.SetStorage(*storage.GetStorage())
	storage.GetStorage().MountPath = stdpath.Clean(storage.GetStorage().MountPath)
	storagesMap.Store(storage.GetStorage().MountPath, storage)
}
