package middlewares

import (
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type filter struct {
	CIDR   *netip.Prefix `json:"cidr,omitempty"`
	Path   *string       `json:"path,omitempty"`
	Method *string       `json:"method,omitempty"`
}

var filterList []*filter

func initFilterList() {
	for _, s := range conf.Conf.Log.Filter.Filters {
		f := new(filter)

		if s.CIDR != "" {
			cidr, err := netip.ParsePrefix(s.CIDR)
			if err != nil {
				log.Errorf("failed to parse CIDR %s: %v", s.CIDR, err)
				continue
			}
			f.CIDR = &cidr
		}

		if s.Path != "" {
			f.Path = &s.Path
		}

		if s.Method != "" {
			f.Method = &s.Method
		}

		if f.CIDR == nil && f.Path == nil && f.Method == nil {
			log.Warnf("filter %s is empty, skipping", s)
			continue
		}

		filterList = append(filterList, f)
		log.Debugf("added filter: %+v", f)
	}

	log.Infof("Loaded %d log filters.", len(filterList))
}

func skiperDecider(c *gin.Context) bool {
	// every filter need metch all condithon as filter match
	// so if any condithon not metch, skip this filter
	// all filters misatch, log this request

	for _, f := range filterList {
		if f.CIDR != nil {
			cip := netip.MustParseAddr(c.ClientIP())
			if !f.CIDR.Contains(cip) {
				continue
			}
		}

		if f.Path != nil {
			if (*f.Path)[0] == '/' {
				// match path as prefix/exact path
				if !strings.HasPrefix(c.Request.URL.Path, *f.Path) {
					continue
				}
			} else {
				// match path as relative path
				if !strings.Contains(c.Request.URL.Path, "/"+*f.Path) {
					continue
				}
			}
		}

		if f.Method != nil {
			if *f.Method != c.Request.Method {
				continue
			}
		}

		return true
	}

	return false
}

func FilteredLogger() gin.HandlerFunc {
	initFilterList()

	return gin.LoggerWithConfig(gin.LoggerConfig{
		Output: log.StandardLogger().Out,
		Skip:   skiperDecider,
		Formatter: func(param gin.LogFormatterParams) string {
			var statusColor, methodColor, resetColor string
			if param.IsOutputColor() {
				statusColor = param.StatusCodeColor()
				methodColor = param.MethodColor()
				resetColor = param.ResetColor()
			}

			if param.Latency > time.Minute {
				param.Latency = param.Latency.Truncate(time.Second)
			}

			var username string
			if v := param.Request.Context().Value(conf.UserKey); v != nil {
				if u, ok := v.(*model.User); ok {
					username = u.Username
				}
			}

			if username == "" {
				username = "guest"
			}

			return fmt.Sprintf("[GIN] %v |%s %3d %s| %13v | %15s | %s |%s %-7s %s %#v\n%s",
				param.TimeStamp.Format("2006/01/02 - 15:04:05"),
				statusColor, param.StatusCode, resetColor,
				param.Latency,
				param.ClientIP,
				username,
				methodColor, param.Method, resetColor,
				param.Path,
				param.ErrorMessage,
			)
		},
	})
}
