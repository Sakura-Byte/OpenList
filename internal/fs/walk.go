package fs

import (
	"context"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	log "github.com/sirupsen/logrus"
)

const (
	walkListRetryAttempts = 10
	walkListRetryDelay    = 200 * time.Millisecond
	walkListMaxBackoff    = 5 * time.Second
)

// WalkFS traverses filesystem fs starting at name up to depth levels.
//
// WalkFS will stop when current depth > `depth`. For each visited node,
// WalkFS calls walkFn. If a visited file system node is a directory and
// walkFn returns path.SkipDir, walkFS will skip traversal of this node.
func WalkFS(ctx context.Context, depth int, name string, info model.Obj, walkFn func(reqPath string, info model.Obj) error) error {
	// This implementation is based on Walk's code in the standard path/path package.
	walkFnErr := walkFn(name, info)
	if walkFnErr != nil {
		if info.IsDir() && walkFnErr == filepath.SkipDir {
			return nil
		}
		return walkFnErr
	}
	if !info.IsDir() || depth == 0 {
		return nil
	}
	meta, _ := op.GetNearestMeta(name)
	// Read directory names.
	objs, err := walkListWithRetry(context.WithValue(ctx, conf.MetaKey, meta), name, &ListArgs{})
	if err != nil {
		return walkFnErr
	}
	for _, fileInfo := range objs {
		filename := path.Join(name, fileInfo.GetName())
		if err := WalkFS(ctx, depth-1, filename, fileInfo, walkFn); err != nil {
			if err == filepath.SkipDir {
				break
			}
			return err
		}
	}
	return nil
}

// WalkFSParallel is like WalkFS but processes directories in parallel.
// It uses bounded parallelism controlled by the concurrency parameter.
// The walkFn must be safe for concurrent use.
func WalkFSParallel(ctx context.Context, depth int, name string, info model.Obj, walkFn func(reqPath string, info model.Obj) error, concurrency int) error {
	walkFnErr := walkFn(name, info)
	if walkFnErr != nil {
		if info.IsDir() && walkFnErr == filepath.SkipDir {
			return nil
		}
		return walkFnErr
	}
	if !info.IsDir() || depth == 0 {
		return nil
	}
	meta, _ := op.GetNearestMeta(name)
	objs, err := walkListWithRetry(context.WithValue(ctx, conf.MetaKey, meta), name, &ListArgs{})
	if err != nil {
		return walkFnErr
	}

	// Use a semaphore channel to limit concurrency
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	done := make(chan struct{})

	for _, fileInfo := range objs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			return err
		default:
		}

		filename := path.Join(name, fileInfo.GetName())

		// Acquire semaphore
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(fi model.Obj, fname string) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := WalkFSParallel(ctx, depth-1, fname, fi, walkFn, concurrency); err != nil {
				if err != filepath.SkipDir {
					select {
					case errChan <- err:
					default:
					}
				}
			}
		}(fileInfo, filename)
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func walkListWithRetry(ctx context.Context, name string, args *ListArgs) ([]model.Obj, error) {
	var lastErr error
	for attempt := 1; attempt <= walkListRetryAttempts; attempt++ {
		objs, err := List(ctx, name, args)
		if err == nil {
			return objs, nil
		}
		lastErr = err
		log.Warnf("walk list %s failed on attempt %d/%d: %+v", name, attempt, walkListRetryAttempts, err)
		if attempt == walkListRetryAttempts {
			break
		}
		backoff := time.Duration(1<<(attempt-1)) * walkListRetryDelay
		if backoff > walkListMaxBackoff {
			backoff = walkListMaxBackoff
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, lastErr
}
