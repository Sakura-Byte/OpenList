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
	ChunkLimit   int    `json:"chunk_limit" form:"chunk_limit"`
	Cursor       string `json:"cursor" form:"cursor"`
}

type UpdateSiteRecursiveEntryResp struct {
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	IsDir    bool      `json:"is_dir"`
	Modified time.Time `json:"modified"`
	Thumb    string    `json:"thumb"`
}

type UpdateSiteRecursiveChunkResp struct {
	Parent  string                         `json:"parent"`
	Content []UpdateSiteRecursiveEntryResp `json:"content"`
	Done    bool                           `json:"parent_done"`
}

type UpdateSiteRecursiveListResp struct {
	Chunks []UpdateSiteRecursiveChunkResp `json:"chunks"`
	Cursor string                         `json:"cursor,omitempty"`
	Done   bool                           `json:"done"`
	Stats  updatesite.ScanPageStats       `json:"stats"`
	Meta   struct {
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
		Chunks: make([]UpdateSiteRecursiveChunkResp, 0, len(page.Chunks)),
		Cursor: page.Cursor,
		Done:   page.Done,
		Stats:  page.Stats,
	}
	resp.Meta.IncludeThumb = page.Meta.IncludeThumb
	resp.Meta.ChunkLimit = page.Meta.ChunkLimit
	for _, pageChunk := range page.Chunks {
		chunk := UpdateSiteRecursiveChunkResp{
			Parent:  pageChunk.ParentPath,
			Content: make([]UpdateSiteRecursiveEntryResp, 0, len(pageChunk.Nodes)),
			Done:    pageChunk.ParentDone,
		}
		for _, obj := range pageChunk.Nodes {
			thumb := ""
			if includeThumb {
				thumb, _ = model.GetThumb(obj)
			}
			chunk.Content = append(chunk.Content, UpdateSiteRecursiveEntryResp{
				Name:     obj.GetName(),
				Size:     obj.GetSize(),
				IsDir:    obj.IsDir(),
				Modified: obj.ModTime(),
				Thumb:    thumb,
			})
		}
		resp.Chunks = append(resp.Chunks, chunk)
	}
	common.SuccessResp(c, resp)
}
