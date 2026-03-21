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
				return onChunk(driver.UpdateSiteChunk{
					Entries: remapUpdateSiteEntriesToPublicMount(utils.GetActualMountPath(storage.GetStorage().MountPath), chunk.Entries),
				})
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
			entries := make([]driver.UpdateSiteEntry, 0, len(merged))
			for _, obj := range merged {
				debug := &driver.UpdateSiteEntryDebug{Engine: "virtual"}
				if containsObjByName(items, obj.GetName()) {
					debug = &driver.UpdateSiteEntryDebug{
						Engine:        "list",
						StorageMount:  storage.GetStorage().MountPath,
						StorageDriver: storage.Config().Name,
					}
				}
				entries = append(entries, makeUpdateSiteEntry(rawPath, obj, debug))
			}
			if err := onChunk(driver.UpdateSiteChunk{Entries: entries}); err != nil {
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
		entries := make([]driver.UpdateSiteEntry, 0, len(virtualFiles))
		for _, obj := range virtualFiles {
			entries = append(entries, makeUpdateSiteEntry(rawPath, obj, &driver.UpdateSiteEntryDebug{Engine: "virtual"}))
		}
		if err := onChunk(driver.UpdateSiteChunk{Entries: entries}); err != nil {
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

	withDebug := func(engine string) driver.UpdateSiteChunkCallback {
		return func(chunk driver.UpdateSiteChunk) error {
			if onChunk == nil {
				return nil
			}
			debug := &driver.UpdateSiteEntryDebug{
				Engine:        engine,
				StorageMount:  storage.GetStorage().MountPath,
				StorageDriver: storage.Config().Name,
			}
			entries := cloneUpdateSiteEntries(chunk.Entries)
			for i := range entries {
				if entries[i].Debug == nil {
					entries[i].Debug = debug
				}
			}
			return onChunk(driver.UpdateSiteChunk{Entries: entries})
		}
	}

	if updater, ok := storage.(driver.UpdateSiteListRer); ok {
		return updater.ListRForUpdateSite(ctx, dir, args, maxDepth, withDebug("update_site_scanner"))
	}
	if listr, ok := storage.(driver.ListRer); ok {
		return walkUpdateSiteStorageByListR(ctx, storage, listr, dir, args, maxDepth, withDebug("listr"))
	}
	return walkUpdateSiteStorageByList(ctx, storage, actualPath, args, maxDepth, withDebug("list"))
}

func walkUpdateSiteStorageByListR(ctx context.Context, storage driver.Driver, listr driver.ListRer, dir model.Obj, args model.ListArgs, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
	return listr.ListR(ctx, dir, args, maxDepth, func(parent string, objs []model.Obj) error {
		parent = utils.FixAndCleanPath(parent)
		for i := range objs {
			raw := model.UnwrapObj(objs[i])
			if raw.GetPath() != "" {
				continue
			}
			if setter, ok := raw.(model.SetPath); ok {
				setter.SetPath(stdpath.Join(parent, raw.GetName()))
			}
		}
		model.WrapObjsName(objs)
		if storage.Config().LocalSort {
			model.SortFiles(objs, storage.GetStorage().OrderBy, storage.GetStorage().OrderDirection)
		}
		model.ExtractFolder(objs, storage.GetStorage().ExtractFolder)
		if onChunk == nil || len(objs) == 0 {
			return nil
		}
		entries := make([]driver.UpdateSiteEntry, 0, len(objs))
		for _, obj := range objs {
			entries = append(entries, makeUpdateSiteEntry(parent, obj, nil))
		}
		return onChunk(driver.UpdateSiteChunk{Entries: entries})
	})
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
	if onChunk != nil && len(objs) > 0 {
		entries := make([]driver.UpdateSiteEntry, 0, len(objs))
		for _, obj := range objs {
			entries = append(entries, makeUpdateSiteEntry(actualPath, obj, nil))
		}
		if err := onChunk(driver.UpdateSiteChunk{Entries: entries}); err != nil {
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

func makeUpdateSiteEntry(parentPath string, obj model.Obj, debug *driver.UpdateSiteEntryDebug) driver.UpdateSiteEntry {
	thumb, _ := model.GetThumb(obj)
	return driver.UpdateSiteEntry{
		VisiblePath: stdpath.Join(parentPath, obj.GetName()),
		ParentPath:  parentPath,
		Name:        obj.GetName(),
		IsDir:       obj.IsDir(),
		Size:        obj.GetSize(),
		Modified:    obj.ModTime(),
		Thumb:       thumb,
		Debug:       debug,
	}
}

func remapUpdateSiteEntriesToPublicMount(mountPath string, entries []driver.UpdateSiteEntry) []driver.UpdateSiteEntry {
	if len(entries) == 0 {
		return nil
	}
	remapped := make([]driver.UpdateSiteEntry, len(entries))
	for i := range entries {
		remapped[i] = entries[i]
		remapped[i].VisiblePath = utils.GetFullPath(mountPath, entries[i].VisiblePath)
		remapped[i].ParentPath = utils.GetFullPath(mountPath, entries[i].ParentPath)
	}
	return remapped
}

func cloneUpdateSiteEntries(entries []driver.UpdateSiteEntry) []driver.UpdateSiteEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]driver.UpdateSiteEntry, len(entries))
	copy(cloned, entries)
	return cloned
}

func containsObjByName(objs []model.Obj, name string) bool {
	for _, obj := range objs {
		if obj.GetName() == name {
			return true
		}
	}
	return false
}
