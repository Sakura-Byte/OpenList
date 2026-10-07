package driver

import (
	"context"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

// CachedTree exposes a local snapshot without fetching missing remote directories.
// Automated traversals must not turn an incomplete snapshot into a remote crawl.
type CachedTree interface {
	ListCached(context.Context, model.Obj, model.ListArgs) ([]model.Obj, error)
}

type cachedTraversalKey struct{}

func WithCachedTraversal(ctx context.Context) context.Context {
	return context.WithValue(ctx, cachedTraversalKey{}, true)
}

func IsCachedTraversal(ctx context.Context) bool {
	v, _ := ctx.Value(cachedTraversalKey{}).(bool)
	return v
}

type ASMRONEStatus struct {
	Phase        string `json:"phase"`
	Paused       bool   `json:"paused"`
	Works        int64  `json:"works"`
	Cached       int64  `json:"cached"`
	Pending      int64  `json:"pending"`
	Failed       int64  `json:"failed"`
	CurrentTask  string `json:"current_task"`
	Pages        int    `json:"pages"`
	TotalPages   int    `json:"total_pages"`
	LastSync     int64  `json:"last_sync"`
	LastFullSync int64  `json:"last_full_sync"`
	NextSync     int64  `json:"next_sync"`
	NextFullSync int64  `json:"next_full_sync"`
	CacheBytes   int64  `json:"cache_bytes"`
	LastError    string `json:"last_error"`
}

type ASMRONEManager interface {
	ASMRONEStatus(context.Context) (ASMRONEStatus, error)
	ASMRONEAction(context.Context, string, string) error
}
