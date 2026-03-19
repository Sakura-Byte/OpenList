package op

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

// WalkPublicRecursive traverses a public path tree, including virtual mount directories.
// It emits batches grouped by the public parent path.
func WalkPublicRecursive(ctx context.Context, rawPath string, maxDepth int, useListR bool, limiter *rate.Limiter, onBatch WalkStorageBatchFunc) error {
	_, err := WalkPublicRecursiveWithStats(ctx, rawPath, maxDepth, useListR, limiter, onBatch)
	return err
}

func WalkPublicRecursiveWithStats(ctx context.Context, rawPath string, maxDepth int, useListR bool, limiter *rate.Limiter, onBatch WalkStorageBatchFunc) (WalkStorageStats, error) {
	stats := WalkStorageStats{}
	rawPath = utils.FixAndCleanPath(rawPath)
	if maxDepth == 0 {
		return stats, nil
	}

	storage, actualPath, err := GetStorageAndActualPath(rawPath)
	if err == nil {
		return WalkStorageRecursiveWithStats(ctx, storage, actualPath, maxDepth, model.ListArgs{Refresh: true}, useListR, limiter, func(parent string, objs []model.Obj) error {
			if onBatch == nil {
				return nil
			}
			return onBatch(utils.GetFullPath(storage.GetStorage().MountPath, parent), objs)
		})
	}
	if !errors.Is(err, errs.StorageNotFound) {
		return stats, err
	}

	virtualFiles := GetStorageVirtualFilesByPath(rawPath)
	if len(virtualFiles) == 0 {
		return stats, errors.WithStack(errs.ObjectNotFound)
	}
	if onBatch != nil {
		stats.BatchCount++
		stats.EntryCount += len(virtualFiles)
		if err := onBatch(rawPath, virtualFiles); err != nil {
			return stats, err
		}
	}
	if maxDepth == 1 {
		return stats, nil
	}

	nextDepth := maxDepth - 1
	if maxDepth < 0 {
		nextDepth = -1
	}
	for _, obj := range virtualFiles {
		if !obj.IsDir() {
			continue
		}
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return stats, err
			}
		}
		childStats, childErr := WalkPublicRecursiveWithStats(ctx, stdpath.Join(rawPath, obj.GetName()), nextDepth, useListR, limiter, onBatch)
		stats.BatchCount += childStats.BatchCount
		stats.EntryCount += childStats.EntryCount
		if childStats.UsedListR {
			stats.UsedListR = true
		}
		if childStats.FellBack {
			stats.FellBack = true
		}
		if childErr != nil {
			return stats, childErr
		}
	}
	return stats, nil
}
