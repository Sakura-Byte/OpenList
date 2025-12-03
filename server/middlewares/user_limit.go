package middlewares

import (
	"net/http"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

// UserRateLimit limits requests per user for the given kind.
func UserRateLimit(kind ratelimit.RequestKind) gin.HandlerFunc {
	return func(c *gin.Context) {
		user, ok := c.Request.Context().Value(conf.UserKey).(*model.User)
		if !ok || user == nil {
			c.Next()
			return
		}
		if err := ratelimit.Allow(c.Request.Context(), user, kind); err != nil {
			common.ErrorResp(c, err, http.StatusTooManyRequests, true)
			c.Abort()
			return
		}
		c.Next()
	}
}
