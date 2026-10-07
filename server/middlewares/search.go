package middlewares

import "github.com/gin-gonic/gin"

// Search availability depends on the requested path and local driver support,
// not only on the configured index engine. The handler checks after binding.
func SearchIndex(c *gin.Context) { c.Next() }
