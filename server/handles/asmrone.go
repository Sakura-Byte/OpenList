package handles

import (
	"errors"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

func asmroneManager(id uint) (driver.ASMRONEManager, error) {
	storage, err := db.GetStorageById(id)
	if err != nil {
		return nil, err
	}
	if storage.Driver != "ASMR.ONE" {
		return nil, errors.New("storage is not ASMR.ONE")
	}
	if storage.Disabled {
		return nil, errors.New("ASMR.ONE storage is disabled")
	}
	d, err := op.GetStorageByMountPath(storage.MountPath)
	if err != nil {
		return nil, err
	}
	if d.GetStorage().Status != op.WORK {
		return nil, errors.New("ASMR.ONE storage is not initialized")
	}
	manager, ok := d.(driver.ASMRONEManager)
	if !ok {
		return nil, errors.New("ASMR.ONE storage is unavailable")
	}
	return manager, nil
}
func ASMRONEStatus(c *gin.Context) {
	id, err := strconv.ParseUint(c.Query("id"), 10, 32)
	if err != nil || id == 0 {
		common.ErrorResp(c, errors.New("invalid storage id"), 400)
		return
	}
	manager, err := asmroneManager(uint(id))
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	status, err := manager.ASMRONEStatus(c.Request.Context())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, status)
}
func ASMRONEAction(c *gin.Context) {
	var req struct {
		ID     uint   `json:"id"`
		Action string `json:"action"`
		WorkID string `json:"work_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.ID == 0 {
		common.ErrorResp(c, errors.New("invalid ASMR.ONE action request"), 400)
		return
	}
	manager, err := asmroneManager(req.ID)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if err := manager.ASMRONEAction(c.Request.Context(), req.Action, req.WorkID); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, nil)
}
