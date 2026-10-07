package op

import (
	stdpath "path"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/go-cache"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var metaCache = cache.NewMemCache(cache.WithShards[*model.Meta](2))
var metaSnapshotMu sync.Mutex
var metaSnapshot []model.Meta
var metaSnapshotLoaded bool

// Coordinate invalidation with cache fills from reads that started before a save.
var metaCacheMu sync.Mutex
var metaCacheGeneration uint64

func invalidateMetaCache(paths ...string) {
	metaSnapshotMu.Lock()
	metaSnapshot = nil
	metaSnapshotLoaded = false
	metaSnapshotMu.Unlock()
	metaCacheMu.Lock()
	defer metaCacheMu.Unlock()
	metaCacheGeneration++
	metaCache.Del(paths...)
	for _, path := range paths {
		metaG.Forget(path)
	}
}

// Coalesce concurrent reads; invalidation forgets calls started before a save.
var metaG singleflight.Group[*model.Meta]

func GetNearestMeta(path string) (*model.Meta, error) {
	return getNearestMeta(utils.FixAndCleanPath(path))
}
func getNearestMeta(path string) (*model.Meta, error) {
	var metas []model.Meta
	loaded := false
	for {
		meta, err := GetMetaByPath(path)
		if err == nil {
			return meta, nil
		}
		if errors.Cause(err) != errs.MetaNotFound {
			return nil, err
		}
		if !loaded {
			metas, err = getMetaSnapshot()
			if err != nil {
				return nil, err
			}
			loaded = true
		}
		var matched *model.Meta
		for i := range metas {
			if !strings.EqualFold(utils.FixAndCleanPath(metas[i].Path), path) {
				continue
			}
			if matched != nil {
				return nil, errors.Errorf("multiple metas match %q case-insensitively", path)
			}
			matched = &metas[i]
		}
		if matched != nil {
			return matched, nil
		}
		if path == "/" {
			return nil, errs.MetaNotFound
		}
		path = stdpath.Dir(path)
	}
}

func getMetaSnapshot() ([]model.Meta, error) {
	metaSnapshotMu.Lock()
	defer metaSnapshotMu.Unlock()
	if metaSnapshotLoaded {
		return metaSnapshot, nil
	}
	metas, err := db.GetAllMetas()
	if err != nil {
		return nil, err
	}
	metaSnapshot = metas
	metaSnapshotLoaded = true
	return metaSnapshot, nil
}

func GetMetaByPath(path string) (*model.Meta, error) {
	return getMetaByPath(utils.FixAndCleanPath(path))
}
func getMetaByPath(path string) (*model.Meta, error) {
	meta, ok := metaCache.Get(path)
	if ok {
		if meta == nil {
			return meta, errs.MetaNotFound
		}
		return meta, nil
	}
	meta, err, _ := metaG.Do(path, func() (*model.Meta, error) {
		metaCacheMu.Lock()
		generation := metaCacheGeneration
		metaCacheMu.Unlock()
		_meta, err := db.GetMetaByPath(path)
		metaCacheMu.Lock()
		defer metaCacheMu.Unlock()
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if generation == metaCacheGeneration {
					metaCache.Set(path, nil)
				}
				return nil, errs.MetaNotFound
			}
			return nil, err
		}
		if generation == metaCacheGeneration {
			metaCache.Set(path, _meta, cache.WithEx[*model.Meta](time.Hour))
		}
		return _meta, nil
	})
	return meta, err
}

func DeleteMetaById(id uint) error {
	old, err := db.GetMetaById(id)
	if err != nil {
		return err
	}
	if err := db.DeleteMetaById(id); err != nil {
		return err
	}
	invalidateMetaCache(old.Path)
	return nil
}

func UpdateMeta(u *model.Meta) error {
	u.Path = utils.FixAndCleanPath(u.Path)
	old, err := db.GetMetaById(u.ID)
	if err != nil {
		return err
	}
	if err := db.UpdateMeta(u); err != nil {
		return err
	}
	invalidateMetaCache(old.Path, u.Path)
	return nil
}

func CreateMeta(u *model.Meta) error {
	u.Path = utils.FixAndCleanPath(u.Path)
	if err := db.CreateMeta(u); err != nil {
		return err
	}
	invalidateMetaCache(u.Path)
	return nil
}

func UpdateMetasGrid(metas []model.MetaGridUpdate) error {
	for i := range metas {
		metas[i].Path = utils.FixAndCleanPath(metas[i].Path)
	}
	paths, err := db.UpdateMetasGrid(metas)
	if err != nil {
		return err
	}
	invalidateMetaCache(paths...)
	return nil
}

func GetMetaById(id uint) (*model.Meta, error) {
	return db.GetMetaById(id)
}

func GetMetas(pageIndex, pageSize int) (metas []model.Meta, count int64, err error) {
	return db.GetMetas(pageIndex, pageSize)
}
