package middlewares

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type countingLimiter struct {
	*rate.Limiter
	calls atomic.Int64
	bytes atomic.Int64
}

func newCountingLimiter() *countingLimiter {
	return &countingLimiter{Limiter: rate.NewLimiter(rate.Inf, 0)}
}

func (l *countingLimiter) WaitN(ctx context.Context, n int) error {
	l.calls.Add(1)
	l.bytes.Add(int64(n))
	return nil
}

func TestDownloadRateLimiterUsesGlobalAndPerRequestLimiters(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldFactory := stream.LocalProxySingleThreadDownloadLimit
	t.Cleanup(func() {
		stream.LocalProxySingleThreadDownloadLimit = oldFactory
	})

	globalLimiter := newCountingLimiter()
	var created atomic.Int64
	var mu sync.Mutex
	localLimiters := make([]*countingLimiter, 0, 2)
	stream.LocalProxySingleThreadDownloadLimit = func() stream.Limiter {
		created.Add(1)
		limiter := newCountingLimiter()
		mu.Lock()
		localLimiters = append(localLimiters, limiter)
		mu.Unlock()
		return limiter
	}

	router := gin.New()
	router.Use(DownloadRateLimiter(globalLimiter))
	router.GET("/file", func(c *gin.Context) {
		_, _ = c.Writer.Write([]byte("abc"))
	})

	for i := 0; i < 2; i++ {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/file", nil)
		router.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("request %d expected status 200, got %d", i, recorder.Code)
		}
		if body := recorder.Body.String(); body != "abc" {
			t.Fatalf("request %d expected body abc, got %q", i, body)
		}
	}

	if got := created.Load(); got != 2 {
		t.Fatalf("expected two per-request limiters, got %d", got)
	}
	if len(localLimiters) != 2 {
		t.Fatalf("expected two captured per-request limiters, got %d", len(localLimiters))
	}
	if localLimiters[0] == localLimiters[1] {
		t.Fatalf("expected independent per-request limiter instances")
	}
	if got := globalLimiter.calls.Load(); got != 2 {
		t.Fatalf("expected global limiter to be called twice, got %d", got)
	}
	if got := globalLimiter.bytes.Load(); got != 6 {
		t.Fatalf("expected global limiter to account for 6 bytes, got %d", got)
	}
	for i, limiter := range localLimiters {
		if got := limiter.calls.Load(); got != 1 {
			t.Fatalf("expected local limiter %d to be called once, got %d", i, got)
		}
		if got := limiter.bytes.Load(); got != 3 {
			t.Fatalf("expected local limiter %d to account for 3 bytes, got %d", i, got)
		}
	}
}

var _ stream.Limiter = (*countingLimiter)(nil)
