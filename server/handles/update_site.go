package handles

import (
	"errors"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type UpdateSiteRecursiveListReq struct {
	Path         string `json:"path" form:"path"`
	MaxDepth     int    `json:"max_depth" form:"max_depth"`
	IncludeThumb *bool  `json:"include_thumb" form:"include_thumb"`
	BatchLimit   int    `json:"batch_limit" form:"batch_limit"`
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
	Stats   op.WalkStorageStats           `json:"stats"`
	Meta    struct {
		Truncated    bool `json:"truncated"`
		IncludeThumb bool `json:"include_thumb"`
		BatchLimit   int  `json:"batch_limit"`
	} `json:"meta"`
}

var errUpdateSiteBatchLimit = errors.New("update_site batch limit reached")

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

	resp := UpdateSiteRecursiveListResp{
		Batches: make([]UpdateSiteRecursiveBatchResp, 0, 64),
	}
	resp.Meta.IncludeThumb = includeThumb
	resp.Meta.BatchLimit = req.BatchLimit
	stats, err := op.WalkPublicRecursiveWithStats(c.Request.Context(), req.Path, req.MaxDepth, true, nil, func(parent string, objs []model.Obj) error {
		if req.BatchLimit > 0 && len(resp.Batches) >= req.BatchLimit {
			resp.Meta.Truncated = true
			return errUpdateSiteBatchLimit
		}
		batch := UpdateSiteRecursiveBatchResp{
			Parent:  parent,
			Content: make([]UpdateSiteRecursiveEntryResp, 0, len(objs)),
		}
		for _, obj := range objs {
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
		return nil
	})
	resp.Stats = stats
	if err != nil && !errors.Is(err, errUpdateSiteBatchLimit) {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, resp)
}
