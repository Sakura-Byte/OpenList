package handles

import (
	"net/http"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type fairQueueAcquireReq struct {
	Path     string `json:"path"`
	Sign     string `json:"sign"`
	Username string `json:"username"`
	IP       string `json:"ip"`
}

type fairQueuePollReq struct {
	WaitToken string `json:"waitToken" binding:"required"`
}

type fairQueueAbandonReq struct {
	WaitToken string `json:"waitToken" binding:"required"`
}

type fairQueueActivateReq struct {
	SlotToken string `json:"slotToken" binding:"required"`
}

type fairQueueReleaseReq struct {
	SlotToken       string `json:"slotToken" binding:"required"`
	HitUpstreamAtMs int64  `json:"hitUpstreamAtMs"`
	Reason          string `json:"reason" binding:"required"`
}

func resolveFairQueueUser(path, ip string, req fairQueueAcquireReq) (*model.User, error) {
	if req.Sign != "" {
		user, err := sign.VerifyDownload(path, ip, strings.TrimSuffix(req.Sign, "/"))
		if err != nil {
			return nil, err
		}
		if user != nil {
			return user, nil
		}
		return op.GetGuest()
	}
	if req.Username != "" {
		return op.GetUserByName(req.Username)
	}
	return nil, nil
}

func isValidFairQueueReleaseReason(reason ratelimit.FairQueueReleaseReason) bool {
	switch reason {
	case ratelimit.ReleaseReasonStreamEnd,
		ratelimit.ReleaseReasonClientAbort,
		ratelimit.ReleaseReasonUpstreamErr,
		ratelimit.ReleaseReasonWorkerClean:
		return true
	default:
		return false
	}
}

func FairQueueAcquire(c *gin.Context) {
	var req fairQueueAcquireReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	ip := req.IP
	if ip == "" {
		ip = c.ClientIP()
	}
	user, err := resolveFairQueueUser(req.Path, ip, req)
	if err != nil {
		if req.Sign != "" {
			common.ErrorResp(c, err, http.StatusUnauthorized)
		} else {
			common.ErrorResp(c, err, http.StatusBadRequest)
		}
		return
	}
	if user == nil {
		common.ErrorStrResp(c, "user required (sign or username)", http.StatusBadRequest)
		return
	}

	res, err := ratelimit.FairQueueAcquire(user, ip, req.Path)
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, res)
}

func FairQueuePoll(c *gin.Context) {
	var req fairQueuePollReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}

	res, err := ratelimit.FairQueuePoll(req.WaitToken)
	if err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	common.SuccessResp(c, res)
}

func FairQueueAbandon(c *gin.Context) {
	var req fairQueueAbandonReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if ratelimit.FairQueueAbandon(req.WaitToken) {
		common.SuccessResp(c, gin.H{"result": "abandoned"})
		return
	}
	common.SuccessResp(c, gin.H{"result": "gone"})
}

func FairQueueActivate(c *gin.Context) {
	var req fairQueueActivateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if err := ratelimit.FairQueueActivate(req.SlotToken); err != nil {
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"result": "activated"})
}

func FairQueueRelease(c *gin.Context) {
	var req fairQueueReleaseReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	hitAt := time.Now()
	if req.HitUpstreamAtMs > 0 {
		hitAt = time.UnixMilli(req.HitUpstreamAtMs)
	}
	reason := ratelimit.FairQueueReleaseReason(req.Reason)
	if !isValidFairQueueReleaseReason(reason) {
		common.ErrorStrResp(c, "invalid fairqueue release reason", http.StatusBadRequest)
		return
	}
	if err := ratelimit.FairQueueRelease(req.SlotToken, hitAt, reason); err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, gin.H{"result": "ok"})
}
