package op

import (
	"context"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type WalkStorageBatchFunc func(parent string, objs []model.Obj) error

type WalkStorageStats struct {
	UsedListR  bool `json:"used_listr"`
	FellBack   bool `json:"fell_back"`
	BatchCount int  `json:"batch_count"`
	EntryCount int  `json:"entry_count"`
}

// WalkStorageRecursive recursively traverses storage from actualPath.
// When useListR is true and the driver supports ListR, it will use ListR first
// and transparently fall back to recursive List on errors.
func WalkStorageRecursive(ctx context.Context, storage driver.Driver, actualPath string, maxDepth int,
	args model.ListArgs, useListR bool, limiter *rate.Limiter, onBatch WalkStorageBatchFunc) error {
	_, err := WalkStorageRecursiveWithStats(ctx, storage, actualPath, maxDepth, args, useListR, limiter, onBatch)
	return err
}

func WalkStorageRecursiveWithStats(ctx context.Context, storage driver.Driver, actualPath string, maxDepth int,
	args model.ListArgs, useListR bool, limiter *rate.Limiter, onBatch WalkStorageBatchFunc) (WalkStorageStats, error) {
	stats := WalkStorageStats{}
	actualPath = utils.FixAndCleanPath(actualPath)
	if maxDepth == 0 {
		return stats, nil
	}

	if !useListR {
		return walkStorageRecursiveByListWithStats(ctx, storage, actualPath, maxDepth, args, limiter, onBatch)
	}

	listr, ok := storage.(driver.ListRer)
	if !ok {
		return walkStorageRecursiveByListWithStats(ctx, storage, actualPath, maxDepth, args, limiter, onBatch)
	}

	dir, err := GetUnwrap(ctx, storage, actualPath)
	if err != nil {
		return stats, errors.WithMessagef(err, "failed get dir for ListR [%s]%s", storage.GetStorage().MountPath, actualPath)
	}
	if raw := model.UnwrapObj(dir); raw.GetPath() == "" {
		if setter, ok := raw.(model.SetPath); ok {
			setter.SetPath(actualPath)
		}
	}

	startTime := time.Now()
	stats.UsedListR = true
	err = listr.ListR(ctx, dir, args, maxDepth, func(parent string, entries []model.Obj) error {
		if len(entries) == 0 {
			return nil
		}
		stats.BatchCount++
		stats.EntryCount += len(entries)
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return err
			}
		}
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
		if onBatch != nil {
			return onBatch(parent, entries)
		}
		return nil
	})
	elapsed := time.Since(startTime)
	if err == nil {
		log.Debugf("ListR walk done, storage=[%s]%s, batches=%d, elapsed=%s",
			storage.GetStorage().MountPath, actualPath, stats.BatchCount, elapsed)
		return stats, nil
	}
	if errors.Is(err, context.Canceled) || utils.IsCanceled(ctx) {
		log.Debugf("ListR walk canceled, storage=[%s]%s, batches=%d, elapsed=%s",
			storage.GetStorage().MountPath, actualPath, stats.BatchCount, elapsed)
		return stats, err
	}

	log.Debugf("ListR walk failed, storage=[%s]%s, batches=%d, elapsed=%s",
		storage.GetStorage().MountPath, actualPath, stats.BatchCount, elapsed)
	log.Warnf("ListR fallback to recursive List, storage=[%s]%s, err=%+v", storage.GetStorage().MountPath, actualPath, err)
	fallbackStats, fallbackErr := walkStorageRecursiveByListWithStats(ctx, storage, actualPath, maxDepth, args, limiter, onBatch)
	stats.FellBack = true
	stats.BatchCount += fallbackStats.BatchCount
	stats.EntryCount += fallbackStats.EntryCount
	return stats, fallbackErr
}

func walkStorageRecursiveByList(ctx context.Context, storage driver.Driver, actualPath string, maxDepth int,
	args model.ListArgs, limiter *rate.Limiter, onBatch WalkStorageBatchFunc) error {
	_, err := walkStorageRecursiveByListWithStats(ctx, storage, actualPath, maxDepth, args, limiter, onBatch)
	return err
}

func walkStorageRecursiveByListWithStats(ctx context.Context, storage driver.Driver, actualPath string, maxDepth int,
	args model.ListArgs, limiter *rate.Limiter, onBatch WalkStorageBatchFunc) (WalkStorageStats, error) {
	stats := WalkStorageStats{}
	if maxDepth == 0 {
		return stats, nil
	}
	objs, err := walkStorageListWithRetry(ctx, storage, actualPath, args)
	if err != nil {
		return stats, err
	}
	if onBatch != nil && len(objs) > 0 {
		stats.BatchCount++
		stats.EntryCount += len(objs)
		if err = onBatch(actualPath, objs); err != nil {
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
	for _, obj := range objs {
		if !obj.IsDir() {
			continue
		}
		if limiter != nil {
			if err = limiter.Wait(ctx); err != nil {
				return stats, err
			}
		}
		nextPath := stdpath.Join(actualPath, obj.GetName())
		childStats, childErr := walkStorageRecursiveByListWithStats(ctx, storage, nextPath, nextDepth, args, limiter, onBatch)
		stats.BatchCount += childStats.BatchCount
		stats.EntryCount += childStats.EntryCount
		if childErr != nil {
			return stats, childErr
		}
	}
	return stats, nil
}

func walkStorageListWithRetry(ctx context.Context, storage driver.Driver, actualPath string, args model.ListArgs) ([]model.Obj, error) {
	maxAttempts := 1
	retryDelay := 200 * time.Millisecond
	maxBackoff := 5 * time.Second
	if conf.Conf != nil {
		if conf.Conf.IndexWalkRetry.MaxAttempts > 0 {
			maxAttempts = conf.Conf.IndexWalkRetry.MaxAttempts
		}
		if conf.Conf.IndexWalkRetry.DelayMs > 0 {
			retryDelay = time.Duration(conf.Conf.IndexWalkRetry.DelayMs) * time.Millisecond
		}
		if conf.Conf.IndexWalkRetry.MaxBackoffMs > 0 {
			maxBackoff = time.Duration(conf.Conf.IndexWalkRetry.MaxBackoffMs) * time.Millisecond
		}
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		objs, err := List(ctx, storage, actualPath, args)
		if err == nil {
			return objs, nil
		}
		if errors.Is(err, context.Canceled) || utils.IsCanceled(ctx) {
			return nil, err
		}
		lastErr = err
		log.Warnf("walk storage list (%s)[%s] failed on attempt %d/%d: %+v",
			storage.GetStorage().MountPath, actualPath, attempt, maxAttempts, err)
		if attempt == maxAttempts {
			break
		}
		backoff := time.Duration(1<<(attempt-1)) * retryDelay
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, lastErr
}
