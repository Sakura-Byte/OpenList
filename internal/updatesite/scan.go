package updatesite

import (
	"context"
	"errors"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cache"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/google/uuid"
)

const (
	defaultScanPageLimit = 32
	maxScanPageLimit     = 256
	scanCursorTTL        = 10 * time.Minute
)

var (
	resolveStorageAndActualPath func(rawPath string) (driver.Driver, string, error)
	listStorageDirectory        func(ctx context.Context, storage driver.Driver, actualPath string) ([]model.Obj, error)
	listVirtualChildren         func(prefix string) []model.Obj

	ErrScanCursorExpired  = errors.New("update-site scan cursor expired")
	ErrScanCursorMismatch = errors.New("update-site scan cursor does not match request")
	ErrScanNotConfigured  = errors.New("update-site scan dependencies are not configured")

	scanSessionCache = cache.NewKeyedCache[*scanSession](scanCursorTTL)
)

type ScanPageRequest struct {
	Path         string
	MaxDepth     int
	IncludeThumb bool
	PageLimit    int
	Cursor       string
}

type ScanPageBatch struct {
	ParentPath string
	Nodes      []model.Obj
}

type ScanPageStats struct {
	PageCount   int `json:"page_count"`
	DirCount    int `json:"dir_count"`
	EntryCount  int `json:"entry_count"`
	PendingDirs int `json:"pending_dirs"`
}

type ScanPageMeta struct {
	IncludeThumb bool `json:"include_thumb"`
	PageLimit    int  `json:"page_limit"`
}

type ScanPageResponse struct {
	Batches []ScanPageBatch `json:"batches"`
	Cursor  string          `json:"cursor,omitempty"`
	Done    bool            `json:"done"`
	Stats   ScanPageStats   `json:"stats"`
	Meta    ScanPageMeta    `json:"meta"`
}

type scanTask struct {
	Path  string
	Depth int
}

type scanSession struct {
	Path         string
	MaxDepth     int
	IncludeThumb bool
	Queue        []scanTask
	Seen         map[string]struct{}
	Stats        ScanPageStats
}

func SetPublicScanDeps(
	resolve func(rawPath string) (driver.Driver, string, error),
	list func(ctx context.Context, storage driver.Driver, actualPath string) ([]model.Obj, error),
	virtuals func(prefix string) []model.Obj,
) {
	resolveStorageAndActualPath = resolve
	listStorageDirectory = list
	listVirtualChildren = virtuals
}

func ScanPublicPathPage(ctx context.Context, req ScanPageRequest) (ScanPageResponse, error) {
	if resolveStorageAndActualPath == nil || listStorageDirectory == nil || listVirtualChildren == nil {
		return ScanPageResponse{}, ErrScanNotConfigured
	}

	req.Path = utils.FixAndCleanPath(req.Path)
	if req.Path == "" {
		req.Path = "/"
	}
	if req.MaxDepth == 0 {
		req.MaxDepth = -1
	}
	req.PageLimit = normalizeScanPageLimit(req.PageLimit)

	cursor := req.Cursor
	session, err := loadOrCreateScanSession(req)
	if err != nil {
		return ScanPageResponse{}, err
	}
	if cursor == "" {
		cursor = uuid.NewString()
	}

	nextSession := cloneScanSession(session)
	resp := ScanPageResponse{
		Batches: make([]ScanPageBatch, 0, req.PageLimit),
		Cursor:  cursor,
		Done:    false,
		Meta: ScanPageMeta{
			IncludeThumb: nextSession.IncludeThumb,
			PageLimit:    req.PageLimit,
		},
	}

	for len(resp.Batches) < req.PageLimit && len(nextSession.Queue) > 0 {
		task := nextSession.Queue[0]
		nextSession.Queue = nextSession.Queue[1:]
		task.Path = utils.FixAndCleanPath(task.Path)
		if _, seen := nextSession.Seen[task.Path]; seen {
			continue
		}
		nextSession.Seen[task.Path] = struct{}{}

		nodes, err := listPublicDirectorySnapshot(ctx, task.Path)
		if err != nil {
			return ScanPageResponse{}, err
		}
		resp.Batches = append(resp.Batches, ScanPageBatch{
			ParentPath: task.Path,
			Nodes:      nodes,
		})
		nextSession.Stats.DirCount++
		nextSession.Stats.EntryCount += len(nodes)

		if nextSession.MaxDepth >= 0 && task.Depth+1 >= nextSession.MaxDepth {
			continue
		}
		for _, node := range nodes {
			if !node.IsDir() {
				continue
			}
			nextSession.Queue = append(nextSession.Queue, scanTask{
				Path:  stdpath.Join(task.Path, node.GetName()),
				Depth: task.Depth + 1,
			})
		}
	}

	nextSession.Stats.PageCount++
	nextSession.Stats.PendingDirs = len(nextSession.Queue)
	resp.Stats = nextSession.Stats

	if len(nextSession.Queue) == 0 {
		resp.Done = true
		resp.Cursor = ""
		scanSessionCache.Delete(cursor)
		return resp, nil
	}

	scanSessionCache.SetWithTTL(cursor, nextSession, scanCursorTTL)
	return resp, nil
}

func normalizeScanPageLimit(limit int) int {
	switch {
	case limit <= 0:
		return defaultScanPageLimit
	case limit > maxScanPageLimit:
		return maxScanPageLimit
	default:
		return limit
	}
}

func loadOrCreateScanSession(req ScanPageRequest) (*scanSession, error) {
	if req.Cursor == "" {
		return &scanSession{
			Path:         req.Path,
			MaxDepth:     req.MaxDepth,
			IncludeThumb: req.IncludeThumb,
			Queue:        []scanTask{{Path: req.Path, Depth: 0}},
			Seen:         map[string]struct{}{},
			Stats:        ScanPageStats{},
		}, nil
	}

	session, ok := scanSessionCache.Get(req.Cursor)
	if !ok {
		return nil, ErrScanCursorExpired
	}
	if session.Path != req.Path || session.MaxDepth != req.MaxDepth || session.IncludeThumb != req.IncludeThumb {
		return nil, ErrScanCursorMismatch
	}
	return session, nil
}

func cloneScanSession(session *scanSession) *scanSession {
	queue := make([]scanTask, len(session.Queue))
	copy(queue, session.Queue)
	seen := make(map[string]struct{}, len(session.Seen))
	for key := range session.Seen {
		seen[key] = struct{}{}
	}
	return &scanSession{
		Path:         session.Path,
		MaxDepth:     session.MaxDepth,
		IncludeThumb: session.IncludeThumb,
		Queue:        queue,
		Seen:         seen,
		Stats:        session.Stats,
	}
}

func listPublicDirectorySnapshot(ctx context.Context, rawPath string) ([]model.Obj, error) {
	rawPath = utils.FixAndCleanPath(rawPath)
	virtualFiles := listVirtualChildren(rawPath)

	storage, actualPath, err := resolveStorageAndActualPath(rawPath)
	if err == nil {
		items, listErr := listStorageDirectory(ctx, storage, actualPath)
		if listErr != nil && len(virtualFiles) == 0 {
			return nil, listErr
		}
		if len(virtualFiles) == 0 {
			if items == nil {
				return []model.Obj{}, nil
			}
			return items, nil
		}
		return model.NewObjMerge().Merge(items, virtualFiles...), nil
	}
	if !errors.Is(err, errs.StorageNotFound) {
		return nil, err
	}
	if len(virtualFiles) == 0 {
		return nil, errs.ObjectNotFound
	}
	return virtualFiles, nil
}
