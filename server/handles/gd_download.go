package handles

import (
	"net/http"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/gdlimit"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

func positiveQueryInt(c *gin.Context, name string, fallback, limit int) int {
	value, err := strconv.Atoi(c.Query(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return min(value, limit)
}

// GDDownloadTop reports the most downloaded authors of an area.
func GDDownloadTop(c *gin.Context) {
	area := c.DefaultQuery("prefix", gdlimit.StatsArea)
	if area != gdlimit.StatsArea {
		common.ErrorStrResp(c, "only "+gdlimit.StatsArea+" is tracked", http.StatusBadRequest)
		return
	}
	rows, err := gdlimit.TopAuthors(area, positiveQueryInt(c, "days", 7, 365), positiveQueryInt(c, "limit", 50, 1000))
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, rows)
}
