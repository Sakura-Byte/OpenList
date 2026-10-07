package op

import (
	"context"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

// Return false while the index is unavailable or busy; persistent driver jobs
// will retry, including after a restart or an interrupted index build.
type CachedTreeUpdateHook func(context.Context, driver.Driver, string, string) (bool, error)

var cachedTreeUpdateHooks []CachedTreeUpdateHook

func RegisterCachedTreeUpdateHook(hook CachedTreeUpdateHook) {
	cachedTreeUpdateHooks = append(cachedTreeUpdateHooks, hook)
}

func HandleCachedTreeUpdate(ctx context.Context, storage driver.Driver, oldPath, newPath string) (bool, error) {
	if len(cachedTreeUpdateHooks) == 0 {
		return false, nil
	}
	for _, hook := range cachedTreeUpdateHooks {
		ok, err := hook(ctx, storage, oldPath, newPath)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}
