package alias

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func (d *fakeThumbDriver) ListRForUpdateSite(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	if d.data == nil {
		return nil
	}
	for _, chunk := range d.data.updateSiteChunks {
		if err := callback(chunk); err != nil {
			return err
		}
	}
	return nil
}

func TestAliasUpdateSiteScannerTraversesRepeatedBalancedPath(t *testing.T) {
	ensureAliasTestEnv(t)

	seq := atomic.AddUint64(&fakeStorageSeq, 1)
	visibleMount := fmt.Sprintf("/alias-balance-%d", seq)
	backendMount1 := visibleMount + ".balance1"
	backendMount2 := visibleMount + ".balance2"
	aliasMount := fmt.Sprintf("/alias-update-site-%d", seq)

	registerFakeData(backendMount1, &fakeThumbData{
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/left", ParentPath: "/", Name: "left"}}},
		},
	})
	registerFakeData(backendMount2, &fakeThumbData{
		updateSiteChunks: []driver.UpdateSiteChunk{
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/right", ParentPath: "/", Name: "right"}}},
		},
	})
	t.Cleanup(func() {
		unregisterFakeData(backendMount1)
		unregisterFakeData(backendMount2)
	})

	createStorage := func(storage model.Storage) uint {
		id, err := op.CreateStorage(context.Background(), storage)
		if err != nil {
			t.Fatalf("failed to create storage %s: %v", storage.MountPath, err)
		}
		return id
	}

	id1 := createStorage(model.Storage{Driver: fakeThumbDriverName, MountPath: backendMount1, Addition: `{}`})
	id2 := createStorage(model.Storage{Driver: fakeThumbDriverName, MountPath: backendMount2, Addition: `{}`})

	addition, err := json.Marshal(Addition{
		Paths: fmt.Sprintf("root:%s\nroot:%s", visibleMount, visibleMount),
	})
	if err != nil {
		t.Fatalf("failed to marshal alias addition: %v", err)
	}
	aliasID := createStorage(model.Storage{
		Driver:    "Alias",
		MountPath: aliasMount,
		Addition:  string(addition),
	})
	t.Cleanup(func() {
		for _, id := range []uint{aliasID, id1, id2} {
			if err := op.DeleteStorageById(context.Background(), id); err != nil {
				t.Fatalf("failed to delete storage %d: %v", id, err)
			}
		}
	})

	storage, err := op.GetStorageByMountPath(aliasMount)
	if err != nil {
		t.Fatalf("failed to get alias storage: %v", err)
	}
	aliasDriver := storage.(*Alias)
	root, err := aliasDriver.GetRoot(context.Background())
	if err != nil {
		t.Fatalf("failed to get alias root: %v", err)
	}

	gotNames := make([]string, 0, 2)
	if err := aliasDriver.ListRForUpdateSite(context.Background(), root, model.ListArgs{}, -1, func(chunk driver.UpdateSiteChunk) error {
		for _, entry := range chunk.Entries {
			gotNames = append(gotNames, entry.Name)
		}
		return nil
	}); err != nil {
		t.Fatalf("alias ListRForUpdateSite: %v", err)
	}
	if len(gotNames) != 2 {
		t.Fatalf("expected 2 names, got %d", len(gotNames))
	}
	if gotNames[0] != "left" && gotNames[1] != "left" {
		t.Fatalf("expected left entry, got %#v", gotNames)
	}
	if gotNames[0] != "right" && gotNames[1] != "right" {
		t.Fatalf("expected right entry, got %#v", gotNames)
	}
}
