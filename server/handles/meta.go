package handles

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/dlclark/regexp2"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func ListMetas(c *gin.Context) {
	var req model.PageReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	log.Debugf("%+v", req)
	metas, total, err := op.GetMetas(req.Page, req.PerPage)
	if err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c, common.PageResp{
		Content: metas,
		Total:   total,
	})
}

func CreateMeta(c *gin.Context) {
	var req model.Meta
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	r, err := validHide(req.Hide)
	if err != nil {
		common.ErrorStrResp(c, fmt.Sprintf("%s is illegal: %s", r, err.Error()), 400)
		return
	}
	if err := op.CreateMeta(&req); err != nil {
		common.ErrorResp(c, err, 500, true)
	} else {
		common.SuccessResp(c)
	}
}

func UpdateMeta(c *gin.Context) {
	var req model.Meta
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	r, err := validHide(req.Hide)
	if err != nil {
		common.ErrorStrResp(c, fmt.Sprintf("%s is illegal: %s", r, err.Error()), 400)
		return
	}
	if err := op.UpdateMeta(&req); err != nil {
		common.ErrorResp(c, err, 500, true)
	} else {
		common.SuccessResp(c)
	}
}

func validHide(hide string) (string, error) {
	rs := strings.SplitSeq(hide, "\n")
	for r := range rs {
		_, err := regexp2.Compile(r, regexp2.None)
		if err != nil {
			return r, err
		}
	}
	return "", nil
}

func UpdateMetasGrid(c *gin.Context) {
	var req struct {
		Metas []model.MetaGridUpdate `json:"metas" binding:"required,min=1,dive"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	ids := make(map[uint]bool, len(req.Metas))
	paths := make(map[string]bool, len(req.Metas))
	for _, meta := range req.Metas {
		path := utils.FixAndCleanPath(meta.Path)
		if ids[meta.ID] || paths[path] {
			common.ErrorStrResp(c, "duplicate metadata id or path", 400)
			return
		}
		ids[meta.ID], paths[path] = true, true
		if r, err := validHide(meta.Hide); err != nil {
			common.ErrorStrResp(c, fmt.Sprintf("%s is illegal: %s", r, err.Error()), 400)
			return
		}
	}
	if err := op.UpdateMetasGrid(req.Metas); err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c)
}

func DeleteMeta(c *gin.Context) {
	idStr := c.Query("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := op.DeleteMetaById(uint(id)); err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c)
}

func GetMeta(c *gin.Context) {
	idStr := c.Query("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	meta, err := op.GetMetaById(uint(id))
	if err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c, meta)
}
