package op

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

func WalkUpdateSitePublicChunks(ctx context.Context, rawPath string, maxDepth int, args model.ListArgs, onChunk driver.UpdateSiteChunkCallback) error {
	rawPath = utils.FixAndCleanPath(rawPath)
	if maxDepth == 0 {
		return nil
	}

	virtualFiles := GetStorageVirtualFilesByPath(rawPath)
	storage, actualPath, err := GetStorageAndActualPath(rawPath)
	if err == nil {
		if len(virtualFiles) == 0 {
			return WalkUpdateSiteStorageChunks(ctx, storage, actualPath, maxDepth, args, func(chunk driver.UpdateSiteChunk) error {
				if onChunk == nil {
					return nil
				}
				chunk.Parent = utils.GetFullPath(utils.GetActualMountPath(storage.GetStorage().MountPath), chunk.Parent)
				return onChunk(chunk)
			})
		}
		items, listErr := List(ctx, storage, actualPath, args)
		if listErr != nil && len(virtualFiles) == 0 {
			return listErr
		}
		merged := model.NewObjMerge().Merge(items, virtualFiles...)
		if len(merged) == 0 && listErr != nil {
			return listErr
		}
		if onChunk != nil {
			if err := onChunk(driver.UpdateSiteChunk{
				Parent:     rawPath,
				Entries:    merged,
				ParentDone: true,
			}); err != nil {
				return err
			}
		}
		return walkUpdateSiteChildDirs(ctx, rawPath, merged, maxDepth, args, onChunk)
	}
	if !errors.Is(err, errs.StorageNotFound) {
		return err
	}
	if len(virtualFiles) == 0 {
		return errors.WithStack(errs.ObjectNotFound)
	}
	if onChunk != nil {
		if err := onChunk(driver.UpdateSiteChunk{
			Parent:     rawPath,
			Entries:    virtualFiles,
			ParentDone: true,
		}); err != nil {
			return err
		}
	}
	return walkUpdateSiteChildDirs(ctx, rawPath, virtualFiles, maxDepth, args, onChunk)
}

func walkUpdateSiteChildDirs(ctx context.Context, parent string, entries []model.Obj, maxDepth int, args model.ListArgs, onChunk driver.UpdateSiteChunkCallback) error {
	if maxDepth == 1 {
		return nil
	}
	nextDepth := maxDepth - 1
	if maxDepth < 0 {
		nextDepth = -1
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if err := WalkUpdateSitePublicChunks(ctx, stdpath.Join(parent, entry.GetName()), nextDepth, args, onChunk); err != nil {
			return err
		}
	}
	return nil
}

func WalkUpdateSiteStorageChunks(ctx context.Context, storage driver.Driver, actualPath string, maxDepth int, args model.ListArgs, onChunk driver.UpdateSiteChunkCallback) error {
	actualPath = utils.FixAndCleanPath(actualPath)
	if maxDepth == 0 {
		return nil
	}

	dir, err := GetUnwrap(ctx, storage, actualPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get dir for update-site walk [%s]%s", storage.GetStorage().MountPath, actualPath)
	}
	if raw := model.UnwrapObj(dir); raw.GetPath() == "" {
		if setter, ok := raw.(model.SetPath); ok {
			setter.SetPath(actualPath)
		}
	}

	if updater, ok := storage.(driver.UpdateSiteListRer); ok {
		return updater.ListRForUpdateSite(ctx, dir, args, maxDepth, onChunk)
	}
	if listr, ok := storage.(driver.ListRer); ok {
		return walkUpdateSiteStorageByListR(ctx, storage, listr, dir, args, maxDepth, onChunk)
	}
	return walkUpdateSiteStorageByList(ctx, storage, actualPath, args, maxDepth, onChunk)
}

func walkUpdateSiteStorageByListR(ctx context.Context, storage driver.Driver, listr driver.ListRer, dir model.Obj, args model.ListArgs, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
	var pending *driver.UpdateSiteChunk
	flushPending := func(parentDone bool) error {
		if pending == nil || onChunk == nil {
			pending = nil
			return nil
		}
		chunk := *pending
		chunk.ParentDone = parentDone
		pending = nil
		return onChunk(chunk)
	}
	err := listr.ListR(ctx, dir, args, maxDepth, func(parent string, entries []model.Obj) error {
		parent = utils.FixAndCleanPath(parent)
		for i := range entries {
			raw := model.UnwrapObj(entries[i])
			if raw.GetPath() != "" {
				continue
			}
			if setter, ok := raw.(model.SetPath); ok {
				setter.SetPath(stdpath.Join(parent, raw.GetName()))
			}
		}
		model.WrapObjsName(entries)
		if storage.Config().LocalSort {
			model.SortFiles(entries, storage.GetStorage().OrderBy, storage.GetStorage().OrderDirection)
		}
		model.ExtractFolder(entries, storage.GetStorage().ExtractFolder)
		if pending == nil {
			pending = &driver.UpdateSiteChunk{
				Parent:  parent,
				Entries: entries,
			}
			return nil
		}
		if pending.Parent == parent {
			if err := flushPending(false); err != nil {
				return err
			}
			pending = &driver.UpdateSiteChunk{
				Parent:  parent,
				Entries: entries,
			}
			return nil
		}
		if err := flushPending(true); err != nil {
			return err
		}
		pending = &driver.UpdateSiteChunk{
			Parent:  parent,
			Entries: entries,
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flushPending(true)
}

func walkUpdateSiteStorageByList(ctx context.Context, storage driver.Driver, actualPath string, args model.ListArgs, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
	actualPath = utils.FixAndCleanPath(actualPath)
	if maxDepth == 0 {
		return nil
	}
	objs, err := walkStorageListWithRetry(ctx, storage, actualPath, args)
	if err != nil {
		return err
	}
	if onChunk != nil {
		if err := onChunk(driver.UpdateSiteChunk{
			Parent:     actualPath,
			Entries:    objs,
			ParentDone: true,
		}); err != nil {
			return err
		}
	}
	if maxDepth == 1 {
		return nil
	}
	nextDepth := maxDepth - 1
	if maxDepth < 0 {
		nextDepth = -1
	}
	for _, obj := range objs {
		if !obj.IsDir() {
			continue
		}
		nextPath := stdpath.Join(actualPath, obj.GetName())
		if err := walkUpdateSiteStorageByList(ctx, storage, nextPath, args, nextDepth, onChunk); err != nil {
			return err
		}
	}
	return nil
}
