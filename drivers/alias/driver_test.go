package alias

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const fakeThumbDriverName = "AliasThumbFake"

var (
	aliasTestInitOnce sync.Once
	fakeDataMu        sync.RWMutex
	fakeDataByMount   = map[string]*fakeThumbData{}
	fakeStorageSeq    uint64
	testModTime       = time.Date(2026, time.March, 19, 12, 0, 0, 0, time.UTC)
)

type fakeThumbData struct {
	get              map[string]model.Obj
	list             map[string][]model.Obj
	details          *model.StorageDetails
	updateSiteChunks []driver.UpdateSiteChunk
}

type fakeThumbDriver struct {
	model.Storage
	addition driver.RootPath
	data     *fakeThumbData
}

func (d *fakeThumbDriver) Config() driver.Config {
	return driver.Config{
		Name:             fakeThumbDriverName,
		NoCache:          true,
		LocalSort:        true,
		DefaultRoot:      "/",
		ProxyRangeOption: false,
	}
}

func (d *fakeThumbDriver) GetAddition() driver.Additional {
	return &d.addition
}

func (d *fakeThumbDriver) Init(ctx context.Context) error {
	fakeDataMu.RLock()
	defer fakeDataMu.RUnlock()
	data, ok := fakeDataByMount[d.GetStorage().MountPath]
	if !ok {
		return fmt.Errorf("missing fake fixture for %s", d.GetStorage().MountPath)
	}
	d.data = data
	return nil
}

func (d *fakeThumbDriver) Drop(ctx context.Context) error {
	d.data = nil
	return nil
}

func (d *fakeThumbDriver) GetRoot(ctx context.Context) (model.Obj, error) {
	return &model.Object{
		Name:     "root",
		Path:     "/",
		IsFolder: true,
		Modified: testModTime,
	}, nil
}

func (d *fakeThumbDriver) Get(ctx context.Context, path string) (model.Obj, error) {
	if obj, ok := d.data.get[path]; ok {
		return obj, nil
	}
	return nil, errs.ObjectNotFound
}

func (d *fakeThumbDriver) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if objs, ok := d.data.list[dir.GetPath()]; ok {
		return objs, nil
	}
	return []model.Obj{}, nil
}

func (d *fakeThumbDriver) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	return nil, errs.NotImplement
}

func (d *fakeThumbDriver) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	if d.data == nil || d.data.details == nil {
		return nil, errs.NotImplement
	}
	return d.data.details, nil
}

var _ driver.Driver = (*fakeThumbDriver)(nil)
var _ driver.Getter = (*fakeThumbDriver)(nil)
var _ driver.GetRooter = (*fakeThumbDriver)(nil)
var _ driver.WithDetails = (*fakeThumbDriver)(nil)

type fakeThumbObj struct {
	model.Obj
	thumb string
}

func (o *fakeThumbObj) Thumb() string {
	return o.thumb
}

func (o *fakeThumbObj) Unwrap() model.Obj {
	return o.Obj
}

func initAliasTestEnv() {
	database, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(database)
	op.RegisterDriver(func() driver.Driver {
		return &fakeThumbDriver{}
	})
}

func ensureAliasTestEnv(t *testing.T) {
	t.Helper()
	aliasTestInitOnce.Do(initAliasTestEnv)
}

func registerFakeData(mountPath string, data *fakeThumbData) {
	fakeDataMu.Lock()
	defer fakeDataMu.Unlock()
	fakeDataByMount[mountPath] = data
}

func unregisterFakeData(mountPath string) {
	fakeDataMu.Lock()
	defer fakeDataMu.Unlock()
	delete(fakeDataByMount, mountPath)
}

func newFakeFile(path string, size int64, hash, thumb string, details *model.StorageDetails) model.Obj {
	base := &model.Object{
		Name:     pathBase(path),
		Path:     path,
		Size:     size,
		Modified: testModTime,
		IsFolder: false,
		HashInfo: utils.NewHashInfo(utils.MD5, hash),
	}
	var obj model.Obj = base
	if thumb != "" {
		obj = &fakeThumbObj{Obj: obj, thumb: thumb}
	}
	if details != nil {
		obj = &model.ObjStorageDetails{Obj: obj, StorageDetails: details}
	}
	return obj
}

func newFakeDir(path string, size int64) model.Obj {
	return &model.Object{
		Name:     pathBase(path),
		Path:     path,
		Size:     size,
		Modified: testModTime,
		IsFolder: true,
	}
}

func pathBase(path string) string {
	if path == "/" {
		return "root"
	}
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

type aliasFixture struct {
	aliasMount string
}

func setupAliasFixture(t *testing.T, backend1, backend2 *fakeThumbData, providerPassThrough, detailsPassThrough bool) aliasFixture {
	t.Helper()
	ensureAliasTestEnv(t)

	seq := atomic.AddUint64(&fakeStorageSeq, 1)
	backendMount1 := fmt.Sprintf("/alias-thumb-src-%d-a", seq)
	backendMount2 := fmt.Sprintf("/alias-thumb-src-%d-b", seq)
	aliasMount := fmt.Sprintf("/alias-thumb-%d", seq)

	registerFakeData(backendMount1, backend1)
	registerFakeData(backendMount2, backend2)
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
		Paths:               fmt.Sprintf("root:%s\nroot:%s", backendMount1, backendMount2),
		ProviderPassThrough: providerPassThrough,
		DetailsPassThrough:  detailsPassThrough,
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

	return aliasFixture{aliasMount: aliasMount}
}

func mustGetThumb(t *testing.T, obj model.Obj) string {
	t.Helper()
	thumb, ok := model.GetThumb(obj)
	if !ok {
		t.Fatalf("expected thumb on %s", obj.GetName())
	}
	return thumb
}

func findByName(t *testing.T, objs []model.Obj, name string) model.Obj {
	t.Helper()
	for _, obj := range objs {
		if obj.GetName() == name {
			return obj
		}
	}
	t.Fatalf("object %s not found", name)
	return nil
}

func countByName(objs []model.Obj, name string) int {
	count := 0
	for _, obj := range objs {
		if obj.GetName() == name {
			count++
		}
	}
	return count
}

func TestAliasListKeepsPrimaryThumb(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/cover.jpg", 128, "hash-cover", "thumb-primary", nil)},
			},
			get: map[string]model.Obj{
				"/cover.jpg": newFakeFile("/cover.jpg", 128, "hash-cover", "thumb-primary", nil),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/cover.jpg", 128, "hash-cover", "", nil)},
			},
			get: map[string]model.Obj{
				"/cover.jpg": newFakeFile("/cover.jpg", 128, "hash-cover", "", nil),
			},
		},
		false, false,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount, &fs.ListArgs{})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	obj := findByName(t, objs, "cover.jpg")
	if thumb := mustGetThumb(t, obj); thumb != "thumb-primary" {
		t.Fatalf("expected primary thumb, got %q", thumb)
	}
	if obj.GetPath() != "/cover.jpg" {
		t.Fatalf("expected alias path /cover.jpg, got %s", obj.GetPath())
	}
}

func TestAliasGetKeepsPrimaryThumb(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			get: map[string]model.Obj{
				"/cover.jpg": newFakeFile("/cover.jpg", 128, "hash-cover", "thumb-primary", nil),
			},
			list: map[string][]model.Obj{
				"/": {newFakeFile("/cover.jpg", 128, "hash-cover", "thumb-primary", nil)},
			},
		},
		&fakeThumbData{
			get: map[string]model.Obj{
				"/cover.jpg": newFakeFile("/cover.jpg", 128, "hash-cover", "", nil),
			},
			list: map[string][]model.Obj{
				"/": {newFakeFile("/cover.jpg", 128, "hash-cover", "", nil)},
			},
		},
		false, false,
	)

	obj, err := fs.Get(context.Background(), fixture.aliasMount+"/cover.jpg", &fs.GetArgs{})
	if err != nil {
		t.Fatalf("fs.Get error: %v", err)
	}

	if thumb := mustGetThumb(t, obj); thumb != "thumb-primary" {
		t.Fatalf("expected primary thumb, got %q", thumb)
	}
	if obj.GetPath() != "/cover.jpg" {
		t.Fatalf("expected alias path /cover.jpg, got %s", obj.GetPath())
	}
}

func TestAliasListBorrowsCompatibleThumb(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/poster.jpg", 256, "hash-poster", "", nil)},
			},
			get: map[string]model.Obj{
				"/poster.jpg": newFakeFile("/poster.jpg", 256, "hash-poster", "", nil),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/poster.jpg", 256, "hash-poster", "thumb-fallback", nil)},
			},
			get: map[string]model.Obj{
				"/poster.jpg": newFakeFile("/poster.jpg", 256, "hash-poster", "thumb-fallback", nil),
			},
		},
		false, false,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount, &fs.ListArgs{})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	if thumb := mustGetThumb(t, findByName(t, objs, "poster.jpg")); thumb != "thumb-fallback" {
		t.Fatalf("expected borrowed thumb, got %q", thumb)
	}
}

func TestAliasGetBorrowsCompatibleThumbAndPreservesProvider(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			get: map[string]model.Obj{
				"/video.mp4": newFakeFile("/video.mp4", 512, "hash-video", "", nil),
			},
			list: map[string][]model.Obj{
				"/": {newFakeFile("/video.mp4", 512, "hash-video", "", nil)},
			},
		},
		&fakeThumbData{
			get: map[string]model.Obj{
				"/video.mp4": newFakeFile("/video.mp4", 512, "hash-video", "thumb-video", nil),
			},
			list: map[string][]model.Obj{
				"/": {newFakeFile("/video.mp4", 512, "hash-video", "thumb-video", nil)},
			},
		},
		true, false,
	)

	obj, err := fs.Get(context.Background(), fixture.aliasMount+"/video.mp4", &fs.GetArgs{})
	if err != nil {
		t.Fatalf("fs.Get error: %v", err)
	}

	if thumb := mustGetThumb(t, obj); thumb != "thumb-video" {
		t.Fatalf("expected borrowed thumb, got %q", thumb)
	}
	provider, ok := model.GetProvider(obj)
	if !ok {
		t.Fatalf("expected provider on alias get result")
	}
	if provider != fakeThumbDriverName {
		t.Fatalf("expected provider %s, got %s", fakeThumbDriverName, provider)
	}
}

func TestAliasDoesNotBorrowIncompatibleThumb(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/mismatch.jpg", 1024, "hash-a", "", nil)},
			},
			get: map[string]model.Obj{
				"/mismatch.jpg": newFakeFile("/mismatch.jpg", 1024, "hash-a", "", nil),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/mismatch.jpg", 1024, "hash-b", "wrong-thumb", nil)},
			},
			get: map[string]model.Obj{
				"/mismatch.jpg": newFakeFile("/mismatch.jpg", 1024, "hash-b", "wrong-thumb", nil),
			},
		},
		false, false,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount, &fs.ListArgs{})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	if thumb, ok := model.GetThumb(findByName(t, objs, "mismatch.jpg")); ok && thumb != "" {
		t.Fatalf("expected no borrowed thumb for incompatible replica, got %q", thumb)
	}
}

func TestAliasListPreservesStorageDetails(t *testing.T) {
	details := &model.StorageDetails{
		DiskUsage: model.DiskUsage{
			TotalSpace: 4096,
			UsedSpace:  1024,
		},
	}
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/with-details.jpg", 64, "hash-details", "thumb-details", details)},
			},
			get: map[string]model.Obj{
				"/with-details.jpg": newFakeFile("/with-details.jpg", 64, "hash-details", "thumb-details", details),
			},
			details: details,
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {},
			},
			get:     map[string]model.Obj{},
			details: details,
		},
		false, true,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount, &fs.ListArgs{WithStorageDetails: true})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	gotDetails, ok := model.GetStorageDetails(findByName(t, objs, "with-details.jpg"))
	if !ok || gotDetails == nil {
		t.Fatalf("expected storage details on alias list result")
	}
	if gotDetails.TotalSpace != details.TotalSpace || gotDetails.UsedSpace != details.UsedSpace {
		t.Fatalf("unexpected details: %+v", gotDetails)
	}
}

func TestAliasListSumsReplicaDirectorySizes(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/":     {newFakeDir("/docs", 10)},
				"/docs": {newFakeFile("/docs/a.txt", 1, "hash-a", "", nil)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 10),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/":     {newFakeDir("/docs", 20)},
				"/docs": {newFakeFile("/docs/b.txt", 2, "hash-b", "", nil)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 20),
			},
		},
		false, false,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount, &fs.ListArgs{})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	if count := countByName(objs, "docs"); count != 1 {
		t.Fatalf("expected exactly one docs entry, got %d", count)
	}
	docs := findByName(t, objs, "docs")
	if !docs.IsDir() {
		t.Fatalf("expected docs to be a directory")
	}
	if docs.GetSize() != 30 {
		t.Fatalf("expected merged docs size 30, got %d", docs.GetSize())
	}
}

func TestAliasGetSumsReplicaDirectorySizes(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeDir("/docs", 10)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 10),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeDir("/docs", 20)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 20),
			},
		},
		false, false,
	)

	obj, err := fs.Get(context.Background(), fixture.aliasMount+"/docs", &fs.GetArgs{})
	if err != nil {
		t.Fatalf("fs.Get error: %v", err)
	}

	if !obj.IsDir() {
		t.Fatalf("expected docs to be a directory")
	}
	if obj.GetSize() != 30 {
		t.Fatalf("expected merged docs size 30, got %d", obj.GetSize())
	}
}

func TestAliasListReplicaDirectoryChildrenRemainUnioned(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/":     {newFakeDir("/docs", 10)},
				"/docs": {newFakeFile("/docs/a.txt", 1, "hash-a", "", nil)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 10),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/":     {newFakeDir("/docs", 20)},
				"/docs": {newFakeFile("/docs/b.txt", 2, "hash-b", "", nil)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 20),
			},
		},
		false, false,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount+"/docs", &fs.ListArgs{})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	if countByName(objs, "a.txt") != 1 || countByName(objs, "b.txt") != 1 {
		t.Fatalf("expected children from both replicas, got %+v", objs)
	}
}

func TestAliasDirectorySizeIgnoresMissingReplica(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeDir("/docs", 10)},
			},
			get: map[string]model.Obj{
				"/docs": newFakeDir("/docs", 10),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {},
			},
			get: map[string]model.Obj{},
		},
		false, false,
	)

	obj, err := fs.Get(context.Background(), fixture.aliasMount+"/docs", &fs.GetArgs{})
	if err != nil {
		t.Fatalf("fs.Get error: %v", err)
	}

	if obj.GetSize() != 10 {
		t.Fatalf("expected docs size 10 with one missing replica, got %d", obj.GetSize())
	}
}

func TestAliasReplicaFileSizeKeepsPrimaryValue(t *testing.T) {
	fixture := setupAliasFixture(t,
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/movie.mkv", 100, "hash-movie", "", nil)},
			},
			get: map[string]model.Obj{
				"/movie.mkv": newFakeFile("/movie.mkv", 100, "hash-movie", "", nil),
			},
		},
		&fakeThumbData{
			list: map[string][]model.Obj{
				"/": {newFakeFile("/movie.mkv", 200, "hash-movie-2", "", nil)},
			},
			get: map[string]model.Obj{
				"/movie.mkv": newFakeFile("/movie.mkv", 200, "hash-movie-2", "", nil),
			},
		},
		false, false,
	)

	objs, err := fs.List(context.Background(), fixture.aliasMount, &fs.ListArgs{})
	if err != nil {
		t.Fatalf("fs.List error: %v", err)
	}

	movie := findByName(t, objs, "movie.mkv")
	if movie.GetSize() != 100 {
		t.Fatalf("expected file size to stay on primary replica (100), got %d", movie.GetSize())
	}
}
