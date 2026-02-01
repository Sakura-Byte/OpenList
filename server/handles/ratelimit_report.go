package handles

import (
	"context"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type RateLimitReportReq struct {
	Kind     string `json:"kind" binding:"required"`
	Path     string `json:"path"`
	Sign     string `json:"sign"`
	Username string `json:"username"`
	IP       string `json:"ip"`
	Mode     string `json:"mode"`
	LeaseID  string `json:"lease_id"`
}

func RateLimitReport(c *gin.Context) {
	var req RateLimitReportReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}

	var kind ratelimit.RequestKind
	switch req.Kind {
	case string(ratelimit.RequestKindDownload):
		kind = ratelimit.RequestKindDownload
	case string(ratelimit.RequestKindList):
		kind = ratelimit.RequestKindList
	case string(ratelimit.RequestKindSearch):
		kind = ratelimit.RequestKindSearch
	default:
		common.ErrorStrResp(c, "invalid kind", http.StatusBadRequest)
		return
	}

	ip := req.IP
	if ip == "" {
		ip = c.ClientIP()
	}
	// Do not tie the lease to the request context, otherwise it gets released
	// as soon as this handler returns. We want the lease to live until the
	// worker explicitly releases it (or the TTL expires).
	ctx := context.WithValue(context.Background(), conf.ClientIPKey, ip)

	mode := strings.ToLower(req.Mode)
	if mode == "" {
		mode = "rps"
	}
	if mode != "rps" {
		common.ErrorStrResp(c, "only rps mode is supported", http.StatusBadRequest)
		return
	}

	var user *model.User
	var err error

	if req.Sign != "" {
		user, err = sign.VerifyDownload(req.Path, ip, req.Sign)
		if err != nil {
			common.ErrorResp(c, err, http.StatusUnauthorized)
			return
		}
	} else if req.Username != "" {
		user, err = op.GetUserByName(req.Username)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
	}

	if err := ratelimit.Allow(ctx, user, kind); err != nil {
		common.ErrorResp(c, err, http.StatusTooManyRequests)
		return
	}
	common.SuccessResp(c)
}
