package v4_1_10

import (
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func EnableThumbnailExpirationOverride() {
	if !strings.HasPrefix(conf.Version, "v") {
		return
	}
	storages, _, err := db.GetStorages(1, -1)
	if err != nil {
		utils.Log.Errorf("[EnableThumbnailExpirationOverride] failed to get storages: %s", err.Error())
		return
	}
	for _, s := range storages {
		if s.ThumbnailExpirationOverride {
			continue
		}
		s.ThumbnailExpirationOverride = true
		err = db.UpdateStorage(&s)
		if err != nil {
			utils.Log.Errorf("[EnableThumbnailExpirationOverride] failed to update storage [%d]%s: %s", s.ID, s.MountPath, err.Error())
		}
	}
}
