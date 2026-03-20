package op

import (
	"context"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/updatesite"
)

func init() {
	updatesite.SetStorageProvider(GetAllStorages)
	updatesite.SetPublicScanDeps(
		GetStorageAndActualPath,
		func(ctx context.Context, storage driver.Driver, actualPath string) ([]model.Obj, error) {
			return List(ctx, storage, actualPath, model.ListArgs{Refresh: true})
		},
		GetStorageVirtualFilesByPath,
	)
}
