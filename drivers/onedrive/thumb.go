package onedrive

import (
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

const updateSiteThumbnailExpirationLead = time.Minute

func updateSiteThumbnailValue(obj model.Obj, override bool, now time.Time) string {
	thumb, _ := model.GetThumb(obj)
	if !override || strings.TrimSpace(thumb) == "" {
		return thumb
	}
	expiry, ok := model.GetThumbExpiration(obj)
	if !ok || expiry == nil || expiry.After(now.Add(updateSiteThumbnailExpirationLead)) {
		return thumb
	}
	return ""
}
