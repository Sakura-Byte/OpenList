package search

import (
	"context"
	"path"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

var cachedTreeIndexMu sync.Mutex

type cachedTreeReplacer interface {
	ReplaceCachedTree(context.Context, string, []model.SearchNode) error
}

func updateCachedTree(ctx context.Context, storage driver.Driver, oldPath, newPath string) (bool, error) {
	if _, direct := storage.(driver.CachedSearcher); direct {
		return true, nil
	}
	if !cachedTreeIndexMu.TryLock() {
		return false, nil
	}
	defer cachedTreeIndexMu.Unlock()
	if instance == nil || !instance.Config().AutoUpdate || !setting.GetBool(conf.AutoUpdateIndex) || Running() {
		return false, nil
	}
	progress, err := Progress()
	if err != nil {
		return false, err
	}
	if !progress.IsDone {
		return false, nil
	}
	fullPath := func(p string) string { return utils.GetFullPath(storage.GetStorage().MountPath, p) }
	if storage.GetStorage().DisableIndex || isIgnorePath(fullPath("/")) {
		return false, nil
	}
	ctx = driver.WithCachedTraversal(ctx)
	var nodes []model.SearchNode
	if newPath != "" && !isIgnorePath(fullPath(newPath)) {
		obj, err := op.GetUnwrap(ctx, storage, newPath)
		if err != nil && !errs.IsObjectNotFound(err) {
			return false, err
		}
		if err == nil {
			nodes = append(nodes, model.SearchNode{Parent: path.Dir(fullPath(newPath)), Name: obj.GetName(), IsDir: obj.IsDir(), Size: obj.GetSize()})
			err = op.WalkStorageRecursive(ctx, storage, newPath, -1, model.ListArgs{SkipHook: true}, false, nil, func(parent string, objs []model.Obj) error {
				for _, obj := range objs {
					if !isIgnorePath(path.Join(fullPath(parent), obj.GetName())) {
						nodes = append(nodes, model.SearchNode{Parent: fullPath(parent), Name: obj.GetName(), IsDir: obj.IsDir(), Size: obj.GetSize()})
					}
				}
				return nil
			})
			if err != nil {
				return false, err
			}
		}
	}
	removePath := ""
	if oldPath != "" && !isIgnorePath(fullPath(oldPath)) {
		removePath = fullPath(oldPath)
	}
	if replacer, ok := instance.(cachedTreeReplacer); ok {
		err = replacer.ReplaceCachedTree(ctx, removePath, nodes)
	} else {
		if removePath != "" {
			err = Del(ctx, removePath)
		}
		if err == nil && len(nodes) > 0 {
			err = instance.BatchIndex(ctx, nodes)
		}
	}
	return err == nil, err
}
func init() { op.RegisterCachedTreeUpdateHook(updateCachedTree) }
