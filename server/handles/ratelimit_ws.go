package handles

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type rateLimitWSMessage struct {
	Mode     string `json:"mode"` // renew (default) | release
	Kind     string `json:"kind"`
	Path     string `json:"path"`
	Sign     string `json:"sign"`
	Username string `json:"username"`
	IP       string `json:"ip"`
	LeaseID  string `json:"lease_id"`
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func resolveRateLimitUser(path, ip string, msg rateLimitWSMessage) (*model.User, error) {
	if msg.Sign != "" {
		return sign.VerifyDownload(path, ip, msg.Sign)
	}
	if msg.Username != "" {
		return op.GetUserByName(msg.Username)
	}
	return nil, nil
}

// RateLimitWS handles WebSocket heartbeats for download concurrency leases.
func RateLimitWS(c *gin.Context) {
	token := c.GetHeader("Authorization")
	if token == "" {
		token = c.Query("token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(setting.GetStr(conf.Token))) != 1 {
		c.Status(http.StatusUnauthorized)
		return
	}

	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	writeResp := func(code int, msg string) {
		_ = conn.WriteJSON(common.Resp[any]{Code: code, Message: msg})
	}

	var (
		user    *model.User
		ip      string
		leaseID string
	)
	release := func() {
		if leaseID == "" || user == nil {
			return
		}
		_ = ratelimit.ReleaseDownload(leaseID, user, ip)
	}
	defer release()

	conn.SetReadLimit(1024)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
			return
		}
		var msg rateLimitWSMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}

		if msg.Kind == "" {
			msg.Kind = string(ratelimit.RequestKindDownload)
		}
		if msg.Kind != string(ratelimit.RequestKindDownload) {
			writeResp(http.StatusBadRequest, "only download kind is supported for websocket heartbeats")
			continue
		}

		ip = msg.IP
		if ip == "" {
			ip = c.ClientIP()
		}
		resolvedUser, err := resolveRateLimitUser(msg.Path, ip, msg)
		if err != nil {
			writeResp(http.StatusUnauthorized, err.Error())
			continue
		}
		if resolvedUser == nil {
			writeResp(http.StatusBadRequest, "user required (sign or username)")
			continue
		}
		user = resolvedUser

		if msg.LeaseID == "" {
			writeResp(http.StatusBadRequest, "lease_id required")
			continue
		}
		leaseID = msg.LeaseID

		switch strings.ToLower(msg.Mode) {
		case "", "renew":
			if err := ratelimit.RenewDownload(user, ip, leaseID); err != nil {
				writeResp(http.StatusBadRequest, err.Error())
				return
			}
			writeResp(http.StatusOK, "renewed")
		case "release":
			release()
			writeResp(http.StatusOK, "released")
			return
		default:
			writeResp(http.StatusBadRequest, "invalid mode")
		}
	}
}
