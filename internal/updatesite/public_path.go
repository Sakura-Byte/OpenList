package updatesite

import (
	"crypto/sha1"
	"encoding/hex"
	"path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

var storageProvider func() []driver.Driver

func SetStorageProvider(provider func() []driver.Driver) {
	storageProvider = provider
}

func normalizeMountedPath(rawPath string) string {
	rawPath = utils.FixAndCleanPath(rawPath)
	type candidate struct {
		from string
		to   string
	}
	var best candidate
	if storageProvider == nil {
		return rawPath
	}
	for _, storage := range storageProvider() {
		mountPath := utils.FixAndCleanPath(storage.GetStorage().MountPath)
		if !utils.PathEqual(rawPath, mountPath) && !strings.HasPrefix(rawPath, utils.PathAddSeparatorSuffix(mountPath)) {
			continue
		}
		rel := strings.TrimPrefix(rawPath, mountPath)
		normalized := utils.FixAndCleanPath(path.Join(utils.GetActualMountPath(mountPath), rel))
		if len(mountPath) > len(best.from) {
			best = candidate{from: mountPath, to: normalized}
		}
	}
	if best.to != "" {
		return best.to
	}
	return rawPath
}

func resolvePublicPath(rawPath string) (string, bool) {
	rawPath = normalizeMountedPath(rawPath)
	candidates := make([]string, 0, 2)
	if storageProvider != nil {
		for _, storage := range storageProvider() {
		resolver, ok := storage.(driver.PublicPathResolver)
		if !ok {
			continue
		}
		if publicPath, ok := resolver.ResolvePublicPath(rawPath); ok {
			publicPath = normalizeMountedPath(publicPath)
			if publicPath != "" {
				candidates = append(candidates, publicPath)
			}
		}
	}
	}
	if len(candidates) > 0 {
		best := candidates[0]
		for _, candidate := range candidates[1:] {
			if len(candidate) < len(best) || (len(candidate) == len(best) && candidate < best) {
				best = candidate
			}
		}
		return best, true
	}
	if rawPath == "/union" || strings.HasPrefix(rawPath, "/union/") {
		return "", false
	}
	return rawPath, true
}

func visibleRootForPath(publicPath string) string {
	publicPath = utils.FixAndCleanPath(publicPath)
	if publicPath == "/" {
		return "/"
	}
	trimmed := strings.TrimPrefix(publicPath, "/")
	root, _, _ := strings.Cut(trimmed, "/")
	if root == "" {
		return "/"
	}
	return "/" + root
}

func defaultBucketPath(publicPath string, isDir bool) string {
	publicPath = utils.FixAndCleanPath(publicPath)
	if isDir {
		return publicPath
	}
	parent := path.Dir(publicPath)
	if parent == "." {
		return "/"
	}
	return utils.FixAndCleanPath(parent)
}

func dedupeKey(parts ...string) string {
	sum := sha1.Sum([]byte(strings.Join(parts, "\n")))
	return "sha1:" + hex.EncodeToString(sum[:])
}

func modifiedAt(obj model.Obj) *time.Time {
	modified := obj.ModTime()
	if modified.IsZero() {
		return nil
	}
	return &modified
}
