package op

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type fakeThumbnailStorage struct {
	model.Storage
	addition  driver.RootPath
	listObjs  []model.Obj
	getObj    model.Obj
	listCalls int
	getCalls  int
}

func (f *fakeThumbnailStorage) Config() driver.Config {
	return driver.Config{Name: "FakeThumbnail"}
}

func (f *fakeThumbnailStorage) GetAddition() driver.Additional {
	return &f.addition
}

func (f *fakeThumbnailStorage) Init(ctx context.Context) error {
	return nil
}

func (f *fakeThumbnailStorage) Drop(ctx context.Context) error {
	return nil
}

func (f *fakeThumbnailStorage) GetRoot(ctx context.Context) (model.Obj, error) {
	return &model.Object{Name: "root", Path: "/", IsFolder: true}, nil
}

func (f *fakeThumbnailStorage) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	f.listCalls++
	out := make([]model.Obj, len(f.listObjs))
	copy(out, f.listObjs)
	return out, nil
}

func (f *fakeThumbnailStorage) Get(ctx context.Context, path string) (model.Obj, error) {
	f.getCalls++
	return f.getObj, nil
}

func (f *fakeThumbnailStorage) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	return nil, nil
}

func newThumbnailObj(name string, expiry *time.Time) model.Obj {
	return &model.ObjThumb{
		Object: model.Object{
			Name:     name,
			Path:     "/" + name,
			Modified: time.Now(),
			IsFolder: false,
		},
		Thumbnail: model.Thumbnail{
			Thumbnail: "thumb://" + name,
			ExpiresAt: expiry,
		},
	}
}

func thumbValue(obj model.Obj) string {
	thumb, _ := model.GetThumb(obj)
	return thumb
}

func TestListClipsDirectoryCacheToThumbnailExpiry(t *testing.T) {
	Cache.ClearAll()
	expiry := time.Now().Add(thumbnailExpirationLead + 200*time.Millisecond)
	storage := &fakeThumbnailStorage{
		Storage: model.Storage{
			MountPath:                   "/fake-thumb-clip",
			CacheExpiration:             30,
			ThumbnailExpirationOverride: true,
		},
		listObjs: []model.Obj{newThumbnailObj("clip.jpg", &expiry)},
	}

	first, err := List(context.Background(), storage, "/", model.ListArgs{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if got := thumbValue(first[0]); got == "" {
		t.Fatal("expected visible thumbnail before expiry")
	}
	if storage.listCalls != 1 {
		t.Fatalf("expected 1 list call after first request, got %d", storage.listCalls)
	}

	time.Sleep(350 * time.Millisecond)

	_, err = List(context.Background(), storage, "/", model.ListArgs{})
	if err != nil {
		t.Fatalf("List returned error on second request: %v", err)
	}
	if storage.listCalls != 2 {
		t.Fatalf("expected cache miss after thumbnail TTL clip, got %d list calls", storage.listCalls)
	}
}

func TestListDoesNotCacheExpiredThumbnailWhenOverrideEnabled(t *testing.T) {
	Cache.ClearAll()
	expiry := time.Now().Add(30 * time.Second)
	storage := &fakeThumbnailStorage{
		Storage: model.Storage{
			MountPath:                   "/fake-thumb-expired",
			CacheExpiration:             30,
			ThumbnailExpirationOverride: true,
		},
		listObjs: []model.Obj{newThumbnailObj("expired.jpg", &expiry)},
	}

	first, err := List(context.Background(), storage, "/", model.ListArgs{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if got := thumbValue(first[0]); got != "" {
		t.Fatalf("expected hidden thumbnail, got %q", got)
	}

	second, err := List(context.Background(), storage, "/", model.ListArgs{})
	if err != nil {
		t.Fatalf("second List returned error: %v", err)
	}
	if got := thumbValue(second[0]); got != "" {
		t.Fatalf("expected hidden thumbnail on second list, got %q", got)
	}
	if storage.listCalls != 2 {
		t.Fatalf("expected no cache for expired thumbnail, got %d list calls", storage.listCalls)
	}
}

func TestListKeepsExistingCacheBehaviorWhenOverrideDisabled(t *testing.T) {
	Cache.ClearAll()
	expiry := time.Now().Add(30 * time.Second)
	storage := &fakeThumbnailStorage{
		Storage: model.Storage{
			MountPath:                   "/fake-thumb-disabled",
			CacheExpiration:             30,
			ThumbnailExpirationOverride: false,
		},
		listObjs: []model.Obj{newThumbnailObj("keep.jpg", &expiry)},
	}

	first, err := List(context.Background(), storage, "/", model.ListArgs{})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if got := thumbValue(first[0]); got == "" {
		t.Fatal("expected thumbnail to remain visible when override is disabled")
	}

	second, err := List(context.Background(), storage, "/", model.ListArgs{})
	if err != nil {
		t.Fatalf("second List returned error: %v", err)
	}
	if got := thumbValue(second[0]); got == "" {
		t.Fatal("expected thumbnail to remain visible when override is disabled")
	}
	if storage.listCalls != 1 {
		t.Fatalf("expected cached response when override is disabled, got %d list calls", storage.listCalls)
	}
}

func TestGetHidesExpiredThumbnailFromGetter(t *testing.T) {
	Cache.ClearAll()
	expiry := time.Now().Add(30 * time.Second)
	storage := &fakeThumbnailStorage{
		Storage: model.Storage{
			MountPath:                   "/fake-thumb-get",
			CacheExpiration:             30,
			ThumbnailExpirationOverride: true,
		},
		getObj: newThumbnailObj("getter.jpg", &expiry),
	}

	obj, err := Get(context.Background(), storage, "/getter.jpg")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got := thumbValue(obj); got != "" {
		t.Fatalf("expected getter thumbnail to be hidden, got %q", got)
	}
}

func TestGetMainItemsIncludesThumbnailExpirationOverrideDefaultTrue(t *testing.T) {
	items := getMainItems(driver.Config{})
	for _, item := range items {
		if item.Name != "thumbnail_expiration_override" {
			continue
		}
		if item.Default != "true" {
			t.Fatalf("expected default true, got %q", item.Default)
		}
		return
	}
	t.Fatal("thumbnail_expiration_override common setting not found")
}
