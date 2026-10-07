package handles

import (
	"path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/search"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

type SearchReq struct {
	model.SearchReq
	Password string `json:"password"`
}

type SearchResp struct {
	model.SearchNode
	Type int `json:"type"`
}

func Search(c *gin.Context) {
	var (
		req SearchReq
		err error
	)
	if err = c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	req.Parent, err = user.JoinPath(req.Parent)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := req.Validate(); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	meta, err := op.GetNearestMeta(req.Parent)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		common.ErrorResp(c, err, 500)
		return
	}
	if !canBrowse(c, user, meta, req.Parent, req.Password) {
		return
	}
	if !search.Available(req.Parent) {
		common.ErrorResp(c, errs.SearchNotAvailable, 404)
		return
	}
	nodes, total, err := search.SearchFiltered(c, req.SearchReq, func(node model.SearchNode) bool {
		if !utils.IsSubPath(user.BasePath, node.Parent) {
			return false
		}
		if node.AccessPath != "" {
			aliasMeta, err := op.GetNearestMeta(path.Dir(node.AccessPath))
			if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
				return false
			}
			if !utils.IsSubPath(user.BasePath, node.AccessPath) || !common.CanAccess(user, aliasMeta, node.AccessPath, req.Password) {
				return false
			}
		}
		meta, err := op.GetNearestMeta(node.Parent)
		if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
			return false
		}
		return common.CanAccess(user, meta, path.Join(node.Parent, node.Name), req.Password)
	})
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{
		Content: utils.MustSliceConvert(nodes, nodeToSearchResp),
		Total:   total,
	})
}

func nodeToSearchResp(node model.SearchNode) SearchResp {
	return SearchResp{
		SearchNode: node,
		Type:       utils.GetObjType(node.Name, node.IsDir),
	}
}
