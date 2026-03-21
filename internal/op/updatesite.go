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
		func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
			return WalkUpdateSitePublicChunks(ctx, rawPath, maxDepth, model.ListArgs{Refresh: true}, onChunk)
		},
	)
}
