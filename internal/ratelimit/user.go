package ratelimit

import (
	"context"
	"math"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"golang.org/x/time/rate"
)

type RequestKind string

const (
	RequestKindDownload RequestKind = "download"
	RequestKindList     RequestKind = "list"
)

const (
	defaultDownloadRPS = 3.0
	defaultListRPS     = 5.0
)

type userLimiter struct {
	limitValue float64
	limiter    *rate.Limiter
}

type Manager struct {
	mu       sync.RWMutex
	download map[uint]*userLimiter
	list     map[uint]*userLimiter
}

var manager = newManager()

func init() {
	op.RegisterSettingChangingCallback(manager.ClearAll)
}

func newManager() *Manager {
	return &Manager{
		download: make(map[uint]*userLimiter),
		list:     make(map[uint]*userLimiter),
	}
}

func limitValueFor(user *model.User, kind RequestKind) float64 {
	var override *float64
	switch kind {
	case RequestKindDownload:
		override = user.DownloadRPS
	case RequestKindList:
		override = user.ListRPS
	}
	if override != nil {
		return *override
	}
	if user.IsGuest() {
		if kind == RequestKindDownload {
			return setting.GetFloat(conf.GuestDownloadRPS, 0)
		}
		return setting.GetFloat(conf.GuestListRPS, 0)
	}
	if kind == RequestKindDownload {
		return setting.GetFloat(conf.UserDefaultDownloadRPS, defaultDownloadRPS)
	}
	return setting.GetFloat(conf.UserDefaultListRPS, defaultListRPS)
}

// LimitValue returns the effective configured RPS for the user and kind.
// 0 or a negative value means unlimited.
func LimitValue(user *model.User, kind RequestKind) float64 {
	return limitValueFor(user, kind)
}

func (m *Manager) store(kind RequestKind) map[uint]*userLimiter {
	if kind == RequestKindList {
		return m.list
	}
	return m.download
}

func (m *Manager) limiter(user *model.User, kind RequestKind) *rate.Limiter {
	value := limitValueFor(user, kind)
	if value <= 0 {
		return nil
	}
	store := m.store(kind)

	m.mu.RLock()
	ul := store[user.ID]
	m.mu.RUnlock()
	if ul != nil && ul.limitValue == value {
		return ul.limiter
	}

	burst := int(math.Ceil(value))
	if burst < 1 {
		burst = 1
	}

	l := rate.NewLimiter(rate.Limit(value), burst)
	m.mu.Lock()
	store[user.ID] = &userLimiter{limitValue: value, limiter: l}
	m.mu.Unlock()
	return l
}

func (m *Manager) ClearAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.download = make(map[uint]*userLimiter)
	m.list = make(map[uint]*userLimiter)
}

// Allow checks if the user is allowed to proceed for a given request kind.
// Returns errs.ExceedUserRateLimit when the limiter rejects the request.
func Allow(ctx context.Context, user *model.User, kind RequestKind) error {
	_ = ctx
	if user == nil {
		return nil
	}
	l := manager.limiter(user, kind)
	if l == nil {
		return nil
	}
	if !l.Allow() {
		return errs.ExceedUserRateLimit
	}
	return nil
}
