package ratelimit

import (
	"math"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"golang.org/x/time/rate"
)

const (
	defaultIPDownloadRPS = 3.0
	defaultIPListRPS     = 10.0
	defaultIPSearchRPS   = 2.0
)

type ipLimiter struct {
	limitValue float64
	limiter    *rate.Limiter
}

type ipManager struct {
	mu       sync.RWMutex
	download map[string]*ipLimiter
	list     map[string]*ipLimiter
	search   map[string]*ipLimiter
}

var ipMgr = newIPManager()

func init() {
	op.RegisterSettingChangingCallback(ipMgr.ClearAll)
}

func newIPManager() *ipManager {
	return &ipManager{
		download: make(map[string]*ipLimiter),
		list:     make(map[string]*ipLimiter),
		search:   make(map[string]*ipLimiter),
	}
}

func ipLimitValue(kind RequestKind) float64 {
	switch kind {
	case RequestKindDownload:
		return setting.GetFloat(conf.IPDownloadRPS, defaultIPDownloadRPS)
	case RequestKindList:
		return setting.GetFloat(conf.IPListRPS, defaultIPListRPS)
	case RequestKindSearch:
		return setting.GetFloat(conf.IPSearchRPS, defaultIPSearchRPS)
	default:
		return 0
	}
}

func (m *ipManager) store(kind RequestKind) map[string]*ipLimiter {
	switch kind {
	case RequestKindList:
		return m.list
	case RequestKindSearch:
		return m.search
	default:
		return m.download
	}
}

func (m *ipManager) limiter(ip string, kind RequestKind) *rate.Limiter {
	value := ipLimitValue(kind)
	if value <= 0 {
		return nil
	}
	store := m.store(kind)

	m.mu.RLock()
	il := store[ip]
	m.mu.RUnlock()
	if il != nil && il.limitValue == value {
		return il.limiter
	}

	burst := int(math.Ceil(value))
	if burst < 1 {
		burst = 1
	}

	l := rate.NewLimiter(rate.Limit(value), burst)
	m.mu.Lock()
	store[ip] = &ipLimiter{limitValue: value, limiter: l}
	m.mu.Unlock()
	return l
}

func (m *ipManager) ClearAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.download = make(map[string]*ipLimiter)
	m.list = make(map[string]*ipLimiter)
	m.search = make(map[string]*ipLimiter)
}
