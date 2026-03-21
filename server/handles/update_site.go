package handles

import (
	"errors"
	"net/http"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/updatesite"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type UpdateSiteRecursiveListReq struct {
	Path         string `json:"path" form:"path"`
	MaxDepth     int    `json:"max_depth" form:"max_depth"`
	IncludeThumb *bool  `json:"include_thumb" form:"include_thumb"`
	ChunkLimit   int    `json:"chunk_limit" form:"chunk_limit"`
	Cursor       string `json:"cursor" form:"cursor"`
}

type UpdateSiteRecursiveEntryDebugResp struct {
	Engine        string `json:"engine"`
	StorageMount  string `json:"storage_mount,omitempty"`
	StorageDriver string `json:"storage_driver,omitempty"`
}

type UpdateSiteRecursiveEntryResp struct {
	VisiblePath string                             `json:"visible_path"`
	ParentPath  string                             `json:"parent_path"`
	Name        string                             `json:"name"`
	Size        int64                              `json:"size"`
	IsDir       bool                               `json:"is_dir"`
	Modified    time.Time                          `json:"modified"`
	Thumb       string                             `json:"thumb"`
	Debug       *UpdateSiteRecursiveEntryDebugResp `json:"debug,omitempty"`
}

type UpdateSiteRecursiveListResp struct {
	Entries []UpdateSiteRecursiveEntryResp `json:"entries"`
	Cursor  string                         `json:"cursor,omitempty"`
	Done    bool                           `json:"done"`
	Stats   updatesite.ScanPageStats       `json:"stats"`
	Meta    struct {
		IncludeThumb bool `json:"include_thumb"`
		ChunkLimit   int  `json:"chunk_limit"`
	} `json:"meta"`
}

func UpdateSiteListRecursive(c *gin.Context) {
	var req UpdateSiteRecursiveListReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if req.Path == "" {
		req.Path = "/"
	}
	if req.MaxDepth == 0 {
		req.MaxDepth = -1
	}
	includeThumb := true
	if req.IncludeThumb != nil {
		includeThumb = *req.IncludeThumb
	}

	page, err := updatesite.ScanPublicPathPage(c.Request.Context(), updatesite.ScanPageRequest{
		Path:         req.Path,
		MaxDepth:     req.MaxDepth,
		IncludeThumb: includeThumb,
		ChunkLimit:   req.ChunkLimit,
		Cursor:       req.Cursor,
	})
	if err != nil {
		switch {
		case errors.Is(err, updatesite.ErrScanCursorExpired):
			c.JSON(http.StatusGone, common.Resp[interface{}]{
				Code:    http.StatusGone,
				Message: err.Error(),
				Data:    nil,
			})
		case errors.Is(err, updatesite.ErrScanCursorMismatch):
			common.ErrorResp(c, err, http.StatusBadRequest)
		default:
			common.ErrorResp(c, err, http.StatusInternalServerError)
		}
		return
	}

	resp := UpdateSiteRecursiveListResp{
		Entries: make([]UpdateSiteRecursiveEntryResp, 0, len(page.Entries)),
		Cursor:  page.Cursor,
		Done:    page.Done,
		Stats:   page.Stats,
	}
	resp.Meta.IncludeThumb = page.Meta.IncludeThumb
	resp.Meta.ChunkLimit = page.Meta.ChunkLimit
	for _, entry := range page.Entries {
		item := UpdateSiteRecursiveEntryResp{
			VisiblePath: entry.VisiblePath,
			ParentPath:  entry.ParentPath,
			Name:        entry.Name,
			Size:        entry.Size,
			IsDir:       entry.IsDir,
			Modified:    entry.Modified,
		}
		if includeThumb {
			item.Thumb = entry.Thumb
		}
		if entry.Debug != nil {
			item.Debug = &UpdateSiteRecursiveEntryDebugResp{
				Engine:        entry.Debug.Engine,
				StorageMount:  entry.Debug.StorageMount,
				StorageDriver: entry.Debug.StorageDriver,
			}
		}
		resp.Entries = append(resp.Entries, item)
	}
	common.SuccessResp(c, resp)
}
