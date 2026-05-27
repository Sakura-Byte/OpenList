package middlewares

import (
	"io"

	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/gin-gonic/gin"
)

func MaxAllowed(n int) gin.HandlerFunc {
	sem := make(chan struct{}, n)
	acquire := func() { sem <- struct{}{} }
	release := func() { <-sem }
	return func(c *gin.Context) {
		acquire()
		defer release()
		c.Next()
	}
}

func UploadRateLimiter(limiter stream.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = &stream.RateLimitReader{
			Reader:  c.Request.Body,
			Limiter: limiter,
			Ctx:     c,
		}
		c.Next()
	}
}

type ResponseWriterWrapper struct {
	gin.ResponseWriter
	WrapWriter io.Writer
}

func (w *ResponseWriterWrapper) Write(p []byte) (n int, err error) {
	return w.WrapWriter.Write(p)
}

func DownloadRateLimiter(limiter stream.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		writer := io.Writer(c.Writer)
		if limiter != nil {
			writer = &stream.RateLimitWriter{
				Writer:  writer,
				Limiter: limiter,
				Ctx:     c,
			}
		}
		if stream.LocalProxySingleThreadDownloadLimit != nil {
			if localProxyLimiter := stream.LocalProxySingleThreadDownloadLimit(); localProxyLimiter != nil {
				writer = &stream.RateLimitWriter{
					Writer:  writer,
					Limiter: localProxyLimiter,
					Ctx:     c,
				}
			}
		}
		c.Writer = &ResponseWriterWrapper{
			ResponseWriter: c.Writer,
			WrapWriter:     writer,
		}
		c.Next()
	}
}
