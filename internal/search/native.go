package search

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/search/searcher"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func nativeMount(d driver.Driver) string { return utils.GetActualMountPath(d.GetStorage().MountPath) }
func overlaps(a, b string) bool          { return utils.IsSubPath(a, b) || utils.IsSubPath(b, a) }

// Available includes local driver searches even when no index engine is enabled.
func Available(parent string) bool {
	if instance != nil {
		return true
	}
	for _, d := range op.GetAllStorages() {
		if _, ok := d.(driver.CachedSearcher); ok && d.GetStorage().Status == op.WORK && !d.GetStorage().DisableIndex && overlaps(parent, nativeMount(d)) && !isIgnorePath(nativeMount(d)) {
			return true
		}
	}
	return false
}

func SearchFiltered(ctx context.Context, req model.SearchReq, filter searcher.Filter) ([]model.SearchNode, int64, error) {
	return searchWithStorages(ctx, req, filter, op.GetAllStorages())
}

func searchWithStorages(ctx context.Context, req model.SearchReq, filter searcher.Filter, storages []driver.Driver) ([]model.SearchNode, int64, error) {
	if err := req.Validate(); err != nil {
		return nil, 0, err
	}
	req.Parent = utils.FixAndCleanPath(req.Parent)
	sort.Slice(storages, func(i, j int) bool { return nativeMount(storages[i]) < nativeMount(storages[j]) })
	var sources []driver.Driver
	for _, d := range storages {
		if _, ok := d.(driver.CachedSearcher); ok && d.GetStorage().Status == op.WORK && !d.GetStorage().DisableIndex && overlaps(req.Parent, nativeMount(d)) && !isIgnorePath(nativeMount(d)) {
			sources = append(sources, d)
		}
	}
	// Resolve nested mounts using the same longest-path rule as filesystem reads.
	owner := func(p string) driver.Driver {
		var found driver.Driver
		length := -1
		for _, d := range storages {
			mount := nativeMount(d)
			if utils.IsSubPath(mount, p) && len(mount) > length {
				found, length = d, len(mount)
			}
		}
		return found
	}
	if len(sources) == 0 {
		if instance == nil {
			return nil, 0, errs.SearchNotAvailable
		}
		return searchIndexedFiltered(ctx, req, func(n model.SearchNode) bool {
			if d := owner(path.Join(n.Parent, n.Name)); d != nil {
				if _, direct := d.(driver.CachedSearcher); direct {
					return false
				}
			}
			return filter == nil || filter(n)
		})
	}
	from := int64(req.Page-1) * int64(req.PerPage)
	var total int64
	out := []model.SearchNode{}
	seen := map[string]bool{}
	accept := func(n model.SearchNode) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := path.Join(n.Parent, n.Name)
		if seen[p] || isIgnorePath(p) || filter != nil && !filter(n) {
			return nil
		}
		seen[p] = true
		if total >= from && int64(len(out)) < int64(req.PerPage) {
			out = append(out, n)
		}
		total++
		return nil
	}
	// Keep the configured index for other providers; suppress any old copies of
	// local-cache results before permissions, totals and pagination are computed.
	if instance != nil {
		batch := req
		batch.Page, batch.PerPage = 1, searchBatchSize
		processed := int64(0)
		for {
			nodes, count, err := instance.Search(ctx, batch)
			if err != nil {
				return nil, 0, err
			}
			for _, n := range nodes {
				if d := owner(path.Join(n.Parent, n.Name)); d != nil {
					if _, ok := d.(driver.CachedSearcher); ok {
						continue
					}
				}
				if err := accept(n); err != nil {
					return nil, 0, err
				}
			}
			processed += int64(len(nodes))
			if len(nodes) == 0 || processed >= count {
				break
			}
			batch.Page++
		}
	}
	for _, d := range sources {
		mount := nativeMount(d)
		local := req
		local.Parent = "/"
		if utils.IsSubPath(mount, req.Parent) {
			local.Parent = "/" + strings.TrimPrefix(strings.TrimPrefix(req.Parent, mount), "/")
		}
		err := d.(driver.CachedSearcher).SearchCached(ctx, local, func(n model.SearchNode) error {
			n.Parent = utils.GetFullPath(mount, n.Parent)
			if n.AccessPath != "" {
				n.AccessPath = utils.GetFullPath(mount, n.AccessPath)
			}
			if owner(path.Join(n.Parent, n.Name)) != d {
				return nil
			}
			return accept(n)
		})
		if err != nil {
			return nil, 0, fmt.Errorf("search %s cache: %w", mount, err)
		}
	}
	return out, total, nil
}
