package handles

import (
	"net/http"
	"strings"
	"time"

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
	QueryToken string `json:"queryToken" binding:"required"`
}

type fairQueueCancelReq struct {
	QueryToken string `json:"queryToken"`
}

type fairQueueReleaseReq struct {
	SlotToken       string `json:"slotToken" binding:"required"`
	HitUpstreamAtMs int64  `json:"hitUpstreamAtMs"`
}

// resolveFairQueueUserFromSign extracts username and isGuest from a signed token.
// It avoids DB queries by using the signed payload directly.
func resolveFairQueueUserFromSign(path, ip, signToken string) (username string, isGuest bool, err error) {
	if signToken == "" {
		return "", false, nil
	}
	username, err = sign.ExtractUsernameFromDownloadSign(path, ip, strings.TrimSuffix(signToken, "/"))
	if err != nil {
		return "", false, err
	}
	// Empty username in a valid sign means guest
	if username == "" {
		return "", true, nil
	}
	return username, false, nil
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

	var username string
	var isGuest bool
	var err error

	if req.Sign != "" {
		username, isGuest, err = resolveFairQueueUserFromSign(req.Path, ip, req.Sign)
		if err != nil {
			common.ErrorResp(c, err, http.StatusUnauthorized)
			return
		}
	} else if req.Username != "" {
		// Direct username provided (trusted caller)
		username = req.Username
		isGuest = false
	} else {
		common.ErrorStrResp(c, "sign or username required", http.StatusBadRequest)
		return
	}

	res, err := ratelimit.FairQueueAcquire(username, isGuest, ip)
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

	res, err := ratelimit.FairQueuePoll(req.QueryToken)
	if err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	common.SuccessResp(c, res)
}

func FairQueueCancel(c *gin.Context) {
	var req fairQueueCancelReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if req.QueryToken == "" {
		common.SuccessResp(c, gin.H{"result": "noop"})
		return
	}
	if ratelimit.FairQueueCancel(req.QueryToken) {
		common.SuccessResp(c, gin.H{"result": "canceled"})
		return
	}
	common.SuccessResp(c, gin.H{"result": "gone"})
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
	if err := ratelimit.FairQueueRelease(req.SlotToken, hitAt); err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, gin.H{"result": "ok"})
}
