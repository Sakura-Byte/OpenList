package updatesite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type fakeScanDriver struct {
	storage model.Storage
}

func (f *fakeScanDriver) Config() driver.Config            { return driver.Config{Name: "FakeScan"} }
func (f *fakeScanDriver) GetStorage() *model.Storage       { return &f.storage }
func (f *fakeScanDriver) SetStorage(storage model.Storage) { f.storage = storage }
func (f *fakeScanDriver) GetAddition() driver.Additional   { return &driver.RootPath{} }
func (f *fakeScanDriver) Init(ctx context.Context) error   { return nil }
func (f *fakeScanDriver) Drop(ctx context.Context) error   { return nil }
func (f *fakeScanDriver) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	return nil, nil
}
func (f *fakeScanDriver) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	return nil, nil
}

func TestScanPublicPathPageCursorLifecycle(t *testing.T) {
	withFakeScanDeps(t,
		map[string][]model.Obj{
			"/":     {fakeObj("3D", true), fakeObj("ASMR", true)},
			"/3D":   {fakeObj("Alpha", false)},
			"/ASMR": {},
		},
		map[string][]model.Obj{},
	)

	first, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		PageLimit:    1,
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done {
		t.Fatalf("expected more pages")
	}
	if first.Cursor == "" {
		t.Fatalf("expected cursor")
	}
	if first.Stats.PageCount != 1 || first.Stats.DirCount != 1 || first.Stats.EntryCount != 2 || first.Stats.PendingDirs != 2 {
		t.Fatalf("unexpected first stats: %+v", first.Stats)
	}
	if got := first.Batches[0].ParentPath; got != "/" {
		t.Fatalf("expected root batch, got %s", got)
	}

	second, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		PageLimit:    1,
		Cursor:       first.Cursor,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.Done {
		t.Fatalf("expected third page")
	}
	if second.Stats.PageCount != 2 || second.Stats.DirCount != 2 || second.Stats.EntryCount != 3 || second.Stats.PendingDirs != 1 {
		t.Fatalf("unexpected second stats: %+v", second.Stats)
	}
	if got := second.Batches[0].ParentPath; got != "/3D" {
		t.Fatalf("expected /3D batch, got %s", got)
	}

	third, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		PageLimit:    1,
		Cursor:       second.Cursor,
	})
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	if !third.Done || third.Cursor != "" {
		t.Fatalf("expected done final page, got done=%v cursor=%q", third.Done, third.Cursor)
	}
	if third.Stats.PageCount != 3 || third.Stats.DirCount != 3 || third.Stats.EntryCount != 3 || third.Stats.PendingDirs != 0 {
		t.Fatalf("unexpected third stats: %+v", third.Stats)
	}
	if got := third.Batches[0].ParentPath; got != "/ASMR" {
		t.Fatalf("expected /ASMR batch, got %s", got)
	}
}

func TestScanPublicPathPageInvalidCursor(t *testing.T) {
	withFakeScanDeps(t, map[string][]model.Obj{"/": {}}, nil)
	if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		PageLimit:    1,
		Cursor:       "missing",
	}); !errors.Is(err, ErrScanCursorExpired) {
		t.Fatalf("expected ErrScanCursorExpired, got %v", err)
	}
}

func TestScanPublicPathPageMaxDepthStopsExpansion(t *testing.T) {
	withFakeScanDeps(t,
		map[string][]model.Obj{
			"/":   {fakeObj("3D", true)},
			"/3D": {fakeObj("Nested", true)},
		},
		nil,
	)

	page, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     1,
		IncludeThumb: false,
		PageLimit:    8,
	})
	if err != nil {
		t.Fatalf("scan page: %v", err)
	}
	if !page.Done {
		t.Fatalf("expected done with maxDepth=1")
	}
	if page.Stats.DirCount != 1 || page.Stats.PendingDirs != 0 {
		t.Fatalf("unexpected stats: %+v", page.Stats)
	}
}

func TestScanPublicPathPageMergesStorageAndVirtualChildren(t *testing.T) {
	withFakeScanDeps(t,
		map[string][]model.Obj{
			"/": {fakeObj("3D", true), fakeObj("local.txt", false)},
		},
		map[string][]model.Obj{
			"/": {fakeObj("3D", true), fakeObj("ASMR", true)},
		},
	)

	page, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     1,
		IncludeThumb: false,
		PageLimit:    8,
	})
	if err != nil {
		t.Fatalf("scan page: %v", err)
	}
	if len(page.Batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(page.Batches))
	}
	if got := len(page.Batches[0].Nodes); got != 3 {
		t.Fatalf("expected merged 3 nodes, got %d", got)
	}
}

func TestScanPublicPathPageCursorMismatch(t *testing.T) {
	withFakeScanDeps(t, map[string][]model.Obj{"/": {fakeObj("child", true)}, "/child": {}}, nil)
	first, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		PageLimit:    1,
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/other",
		MaxDepth:     -1,
		IncludeThumb: true,
		PageLimit:    1,
		Cursor:       first.Cursor,
	}); !errors.Is(err, ErrScanCursorMismatch) {
		t.Fatalf("expected ErrScanCursorMismatch, got %v", err)
	}
}

func withFakeScanDeps(t *testing.T, storageListing map[string][]model.Obj, virtualListing map[string][]model.Obj) {
	t.Helper()

	scanSessionCache.Clear()
	prevResolver := resolveStorageAndActualPath
	prevLister := listStorageDirectory
	prevVirtuals := listVirtualChildren

	drivers := map[string]*fakeScanDriver{}
	for path := range storageListing {
		drivers[path] = &fakeScanDriver{storage: model.Storage{MountPath: path, Modified: time.Now()}}
	}

	resolveStorageAndActualPath = func(rawPath string) (driver.Driver, string, error) {
		if d, ok := drivers[rawPath]; ok {
			return d, rawPath, nil
		}
		return nil, "", errs.StorageNotFound
	}
	listStorageDirectory = func(ctx context.Context, storage driver.Driver, actualPath string) ([]model.Obj, error) {
		items, ok := storageListing[actualPath]
		if !ok {
			return nil, errs.ObjectNotFound
		}
		out := make([]model.Obj, len(items))
		copy(out, items)
		return out, nil
	}
	listVirtualChildren = func(prefix string) []model.Obj {
		items := virtualListing[prefix]
		out := make([]model.Obj, len(items))
		copy(out, items)
		return out
	}

	t.Cleanup(func() {
		scanSessionCache.Clear()
		resolveStorageAndActualPath = prevResolver
		listStorageDirectory = prevLister
		listVirtualChildren = prevVirtuals
	})
}

func fakeObj(name string, isDir bool) model.Obj {
	return &model.Object{
		Name:     name,
		Path:     "/" + name,
		IsFolder: isDir,
	}
}
