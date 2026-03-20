package handles

import (
	"errors"
	"net/http"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/updatesite"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type UpdateSiteRecursiveListReq struct {
	Path         string `json:"path" form:"path"`
	MaxDepth     int    `json:"max_depth" form:"max_depth"`
	IncludeThumb *bool  `json:"include_thumb" form:"include_thumb"`
	PageLimit    int    `json:"page_limit" form:"page_limit"`
	Cursor       string `json:"cursor" form:"cursor"`
}

type UpdateSiteRecursiveEntryResp struct {
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	IsDir    bool      `json:"is_dir"`
	Modified time.Time `json:"modified"`
	Thumb    string    `json:"thumb"`
}

type UpdateSiteRecursiveBatchResp struct {
	Parent  string                         `json:"parent"`
	Content []UpdateSiteRecursiveEntryResp `json:"content"`
}

type UpdateSiteRecursiveListResp struct {
	Batches []UpdateSiteRecursiveBatchResp `json:"batches"`
	Cursor  string                         `json:"cursor,omitempty"`
	Done    bool                           `json:"done"`
	Stats   updatesite.ScanPageStats       `json:"stats"`
	Meta    struct {
		IncludeThumb bool `json:"include_thumb"`
		PageLimit    int  `json:"page_limit"`
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
		PageLimit:    req.PageLimit,
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
		Batches: make([]UpdateSiteRecursiveBatchResp, 0, len(page.Batches)),
		Cursor:  page.Cursor,
		Done:    page.Done,
		Stats:   page.Stats,
	}
	resp.Meta.IncludeThumb = page.Meta.IncludeThumb
	resp.Meta.PageLimit = page.Meta.PageLimit
	for _, pageBatch := range page.Batches {
		batch := UpdateSiteRecursiveBatchResp{
			Parent:  pageBatch.ParentPath,
			Content: make([]UpdateSiteRecursiveEntryResp, 0, len(pageBatch.Nodes)),
		}
		for _, obj := range pageBatch.Nodes {
			thumb := ""
			if includeThumb {
				thumb, _ = model.GetThumb(obj)
			}
			batch.Content = append(batch.Content, UpdateSiteRecursiveEntryResp{
				Name:     obj.GetName(),
				Size:     obj.GetSize(),
				IsDir:    obj.IsDir(),
				Modified: obj.ModTime(),
				Thumb:    thumb,
			})
		}
		resp.Batches = append(resp.Batches, batch)
	}
	common.SuccessResp(c, resp)
}
