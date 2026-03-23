package op

import (
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/bmatcuk/doublestar/v4"
	log "github.com/sirupsen/logrus"
)

const thumbnailExpirationLead = time.Minute

func shouldOverrideThumbnailExpiration(storage driver.Driver) bool {
	return storage != nil && storage.GetStorage() != nil && storage.GetStorage().ThumbnailExpirationOverride
}

func normalizeThumbnailObject(obj model.Obj, override bool, now time.Time) (model.Obj, *time.Time, bool) {
	if !override {
		return obj, nil, false
	}
	thumb, ok := model.GetThumb(obj)
	if !ok || strings.TrimSpace(thumb) == "" {
		return obj, nil, false
	}
	expiry, ok := model.GetThumbExpiration(obj)
	if !ok || expiry == nil {
		return obj, nil, false
	}
	if !expiry.After(now.Add(thumbnailExpirationLead)) {
		return &model.ObjThumbOverlay{Obj: obj, Thumbnail: model.Thumbnail{}}, nil, true
	}
	return obj, expiry, false
}

func normalizeThumbnailObjects(objs []model.Obj, override bool, now time.Time) ([]model.Obj, *time.Duration, bool) {
	if len(objs) == 0 || !override {
		return objs, nil, true
	}
	var (
		cloned    []model.Obj
		ttlCap    *time.Duration
		cacheable = true
	)
	for i, obj := range objs {
		normalized, expiry, invalid := normalizeThumbnailObject(obj, override, now)
		if normalized != obj {
			if cloned == nil {
				cloned = make([]model.Obj, len(objs))
				copy(cloned, objs)
			}
			cloned[i] = normalized
		}
		if invalid {
			cacheable = false
			continue
		}
		if expiry == nil {
			continue
		}
		candidate := expiry.Sub(now) - thumbnailExpirationLead
		if ttlCap == nil || candidate < *ttlCap {
			candidateCopy := candidate
			ttlCap = &candidateCopy
		}
	}
	if cloned == nil {
		cloned = objs
	}
	return cloned, ttlCap, cacheable
}

func normalizeThumbnailForResponse(storage driver.Driver, obj model.Obj) model.Obj {
	now := time.Now()
	normalized, _, _ := normalizeThumbnailObject(obj, shouldOverrideThumbnailExpiration(storage), now)
	return normalized
}

func thumbnailValueForObject(storage driver.Driver, obj model.Obj) string {
	normalized := normalizeThumbnailForResponse(storage, obj)
	thumb, _ := model.GetThumb(normalized)
	return thumb
}

func resolveDirectoryCacheTTL(storage driver.Driver, path string) time.Duration {
	ttl := storage.GetStorage().CacheExpiration
	customCachePolicies := storage.GetStorage().CustomCachePolicies
	if len(customCachePolicies) > 0 {
		configPolicies := strings.Split(customCachePolicies, "\n")
		for _, configPolicy := range configPolicies {
			pattern, ttlStr, ok := strings.Cut(strings.TrimSpace(configPolicy), ":")
			if !ok {
				log.Warnf("Malformed custom cache policy entry: %s in storage %s for path %s. Expected format: pattern:ttl", configPolicy, storage.GetStorage().MountPath, path)
				continue
			}
			match, err := doublestar.Match(pattern, path)
			if err != nil {
				log.Warnf("Invalid glob pattern in custom cache policy: %s, error: %v", pattern, err)
				continue
			}
			if !match {
				continue
			}
			configTTL, err := strconv.ParseInt(ttlStr, 10, 64)
			if err == nil {
				ttl = int(configTTL)
				break
			}
		}
	}
	return time.Minute * time.Duration(ttl)
}
