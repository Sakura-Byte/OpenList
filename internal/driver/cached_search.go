package driver

import (
	"context"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

// CachedSearcher searches the driver's existing local data without upstream I/O
// or a separate search index. Paths are relative to the storage root. The caller
// applies permissions and pagination to the streamed results.
type CachedSearcher interface {
	SearchCached(context.Context, model.SearchReq, func(model.SearchNode) error) error
}

// CachedSearchMatch uses literal, case-insensitive AND matching on file names.
// A result must be below Parent; Parent itself is not a search result.
func CachedSearchMatch(req model.SearchReq, node model.SearchNode) bool {
	p := path.Clean("/" + req.Parent)
	if node.Parent != p && !strings.HasPrefix(node.Parent, strings.TrimRight(p, "/")+"/") {
		return false
	}
	if req.Scope == 1 && !node.IsDir || req.Scope == 2 && node.IsDir {
		return false
	}
	name := strings.ToLower(node.Name)
	for _, word := range strings.Fields(strings.ToLower(req.Keywords)) {
		if !strings.Contains(name, word) {
			return false
		}
	}
	return true
}
