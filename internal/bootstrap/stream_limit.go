package bootstrap

import (
	"context"
	"sync/atomic"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"golang.org/x/time/rate"
)

type blockBurstLimiter struct {
	*rate.Limiter
}

func (l blockBurstLimiter) WaitN(ctx context.Context, total int) error {
	for total > 0 {
		n := l.Burst()
		if l.Limiter.Limit() == rate.Inf || n > total {
			n = total
		}
		err := l.Limiter.WaitN(ctx, n)
		if err != nil {
			return err
		}
		total -= n
	}
	return nil
}

func streamFilterNegative(limit int) (rate.Limit, int) {
	if limit < 0 {
		return rate.Inf, 0
	}
	return rate.Limit(limit) * 1024.0, limit * 1024
}

func initLimiter(limiter *stream.Limiter, s string) {
	clientDownLimit, burst := streamFilterNegative(setting.GetInt(s, -1))
	*limiter = blockBurstLimiter{Limiter: rate.NewLimiter(clientDownLimit, burst)}
	op.RegisterSettingChangingCallback(func() {
		newLimit, newBurst := streamFilterNegative(setting.GetInt(s, -1))
		(*limiter).SetLimit(newLimit)
		(*limiter).SetBurst(newBurst)
	})
}

func initLocalProxySingleThreadDownloadLimit() {
	var limitKB atomic.Int64
	limitKB.Store(int64(setting.GetInt(conf.LocalProxyMaxSingleThreadDownloadSpeed, -1)))

	stream.LocalProxySingleThreadDownloadLimit = func() stream.Limiter {
		limit := limitKB.Load()
		if limit <= 0 {
			return nil
		}
		burst := int(limit) * 1024
		return blockBurstLimiter{
			Limiter: rate.NewLimiter(rate.Limit(burst), burst),
		}
	}

	op.RegisterSettingChangingCallback(func() {
		limitKB.Store(int64(setting.GetInt(conf.LocalProxyMaxSingleThreadDownloadSpeed, -1)))
	})
}

func InitStreamLimit() {
	initLimiter(&stream.ClientDownloadLimit, conf.StreamMaxClientDownloadSpeed)
	initLimiter(&stream.ClientUploadLimit, conf.StreamMaxClientUploadSpeed)
	initLimiter(&stream.ServerDownloadLimit, conf.StreamMaxServerDownloadSpeed)
	initLimiter(&stream.ServerUploadLimit, conf.StreamMaxServerUploadSpeed)
	initLocalProxySingleThreadDownloadLimit()
}
