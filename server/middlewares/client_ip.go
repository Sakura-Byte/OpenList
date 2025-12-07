package middlewares

import (
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

// ClientIPContext stores the best-effort client IP into the request context.
func ClientIPContext(c *gin.Context) {
	ip := utils.ClientIP(c.Request)
	common.GinWithValue(c, conf.ClientIPKey, ip)
	c.Next()
}
