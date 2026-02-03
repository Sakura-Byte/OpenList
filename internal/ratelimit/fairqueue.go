package ratelimit

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils/random"
)

const (
	defaultOverloadedRetryAfter     = 30 * time.Second
	defaultUserDownloadConcurrency  = 3
	defaultGuestDownloadConcurrency = 3
	defaultIPDownloadConcurrency    = 3
)

type fairQueueResult struct {
	Result     string `json:"result"`
	QueryToken string `json:"queryToken,omitempty"`
	SlotToken  string `json:"slotToken,omitempty"`
	RetryAfter int    `json:"retryAfter,omitempty"` // milliseconds
	Reason     string `json:"reason,omitempty"`
}

type fairQueueSessionState string

const (
	sessionPending fairQueueSessionState = "PENDING"
	sessionGranted fairQueueSessionState = "GRANTED"
)

type fairQueueSession struct {
	Token            string
	Host             string
	IP               string
	CreatedAt        time.Time
	LastSeenAt       time.Time
	State            fairQueueSessionState
	SlotToken        string
	MaxSlotsHost     int
	MaxSlotsIP       int
	WaiterTracked    bool
	CleanupScheduled bool
}

type fairQueueSlot struct {
	Token        string
	Host         string
	IP           string
	AcquiredAt   time.Time
	MaxSlotsHost int
	Releasing    bool
}

type smoothHostReleaser struct {
	mu            sync.Mutex
	lastReleaseAt time.Time
}

func (sr *smoothHostReleaser) nextReleaseAfter(base time.Time, interval time.Duration) time.Time {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if interval <= 0 {
		sr.lastReleaseAt = base
		return base
	}

	if sr.lastReleaseAt.IsZero() || !sr.lastReleaseAt.After(base) {
		sr.lastReleaseAt = base
		return base
	}

	next := sr.lastReleaseAt.Add(interval)
	sr.lastReleaseAt = next
	return next
}

type fairQueueManager struct {
	mu             sync.Mutex
	sessions       map[string]*fairQueueSession
	hostQueues     map[string][]string
	ipPending      map[string]int
	activeSlots    map[string]*fairQueueSlot
	hostActive     map[string]int
	ipActive       map[string]int
	slotToSession  map[string]string
	smoothReleaser map[string]*smoothHostReleaser
	globalWaiters  int
	gcOnce         sync.Once
}

var fairQueue = newFairQueueManager()

func newFairQueueManager() *fairQueueManager {
	return &fairQueueManager{
		sessions:       make(map[string]*fairQueueSession),
		hostQueues:     make(map[string][]string),
		ipPending:      make(map[string]int),
		activeSlots:    make(map[string]*fairQueueSlot),
		hostActive:     make(map[string]int),
		ipActive:       make(map[string]int),
		slotToSession:  make(map[string]string),
		smoothReleaser: make(map[string]*smoothHostReleaser),
	}
}

func fairQueueConfig() conf.FairQueue {
	if conf.Conf == nil {
		return conf.FairQueue{}
	}
	return conf.Conf.FairQueue
}

func fqMaxWait(cfg conf.FairQueue) time.Duration {
	if cfg.MaxWaitMs <= 0 {
		return 20 * time.Second
	}
	return time.Duration(cfg.MaxWaitMs) * time.Millisecond
}

func fqPollInterval(cfg conf.FairQueue) time.Duration {
	if cfg.PollIntervalMs <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(cfg.PollIntervalMs) * time.Millisecond
}

func fqSessionIdle(cfg conf.FairQueue) time.Duration {
	if cfg.SessionIdleSeconds <= 0 {
		return 90 * time.Second
	}
	return time.Duration(cfg.SessionIdleSeconds) * time.Second
}

func fqZombieTimeout(cfg conf.FairQueue) time.Duration {
	if cfg.ZombieTimeoutSeconds <= 0 {
		return 30 * time.Second
	}
	return time.Duration(cfg.ZombieTimeoutSeconds) * time.Second
}

func fqGlobalMaxWaiters(cfg conf.FairQueue) int {
	if cfg.GlobalMaxWaiters <= 0 {
		return 500
	}
	return cfg.GlobalMaxWaiters
}

func fqMaxWaitersPerHost(cfg conf.FairQueue) int {
	if cfg.MaxWaitersPerHost <= 0 {
		return 50
	}
	return cfg.MaxWaitersPerHost
}

func fqGrantedCleanupDelay(cfg conf.FairQueue) time.Duration {
	// 这个 delay 现在只用于“清理已结束 slot 对应的 session”，不会再把活跃 slot 的 session 提前删掉
	if cfg.DefaultGrantedCleanupDelay <= 0 {
		return 5 * time.Second
	}
	return time.Duration(cfg.DefaultGrantedCleanupDelay) * time.Second
}

func fairQueueHostKey(user *model.User) string {
	if user == nil {
		return ""
	}
	return fmt.Sprintf("u:%d", user.ID)
}

func fairQueueIPKey(ip string) string {
	return ip
}

func (m *fairQueueManager) ensureGC() {
	m.gcOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				cfg := fairQueueConfig()
				m.gc(cfg)
			}
		}()
	})
}

func (m *fairQueueManager) gc(cfg conf.FairQueue) {
	now := time.Now()
	maxWait := fqMaxWait(cfg)
	idle := fqSessionIdle(cfg)
	zombie := fqZombieTimeout(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()

	// 清理 pending 的 session（排队者）
	for _, sess := range m.sessions {
		if sess == nil || sess.State != sessionPending {
			continue
		}
		if maxWait > 0 && now.Sub(sess.CreatedAt) >= maxWait {
			m.removeSessionLocked(sess)
			continue
		}
		if idle > 0 && now.Sub(sess.LastSeenAt) >= idle {
			m.removeSessionLocked(sess)
			continue
		}
	}

	// zombie slot 超时回收（兜底释放 active）
	if zombie > 0 {
		for token, slot := range m.activeSlots {
			if slot == nil {
				delete(m.activeSlots, token)
				continue
			}
			if now.Sub(slot.AcquiredAt) >= zombie {
				m.releaseSlotLocked(slot)
			}
		}
	}

	// 顺带清理 granted session：如果 slot 已经不在了，session 也该删
	//（注意：这一步不是必须，但能加速清理 sessions map）
	for _, sess := range m.sessions {
		if sess == nil || sess.State != sessionGranted {
			continue
		}
		if sess.SlotToken == "" {
			m.removeSessionLocked(sess)
			continue
		}
		if m.activeSlots[sess.SlotToken] == nil {
			m.removeSessionLocked(sess)
			continue
		}
	}
}

func (m *fairQueueManager) trackWaiter(sess *fairQueueSession) {
	if sess == nil || sess.WaiterTracked {
		return
	}
	queue := m.hostQueues[sess.Host]
	queue = append(queue, sess.Token)
	m.hostQueues[sess.Host] = queue
	m.globalWaiters++
	if sess.IP != "" {
		key := fairQueueIPKey(sess.IP)
		m.ipPending[key]++
	}
	sess.WaiterTracked = true
}

func (m *fairQueueManager) untrackWaiter(sess *fairQueueSession) {
	if sess == nil || !sess.WaiterTracked {
		return
	}
	queue := m.hostQueues[sess.Host]
	if len(queue) > 0 {
		out := queue[:0]
		for _, token := range queue {
			if token != sess.Token {
				out = append(out, token)
			}
		}
		if len(out) == 0 {
			delete(m.hostQueues, sess.Host)
		} else {
			m.hostQueues[sess.Host] = out
		}
	}
	if m.globalWaiters > 0 {
		m.globalWaiters--
	}
	if sess.IP != "" {
		key := fairQueueIPKey(sess.IP)
		if v := m.ipPending[key]; v > 1 {
			m.ipPending[key] = v - 1
		} else {
			delete(m.ipPending, key)
		}
	}
	sess.WaiterTracked = false
}

// removeSessionLocked：修复关键点：只在 slot 已不活跃时才删 slotToSession，避免 orphan slot 失去关联
func (m *fairQueueManager) removeSessionLocked(sess *fairQueueSession) {
	if sess == nil {
		return
	}
	if sess.WaiterTracked {
		m.untrackWaiter(sess)
	}

	// 如果 session 持有 slotToken：
	// - slot 仍活跃：不要 delete(slotToSession)，避免提前断开 slot->session 的关系
	// - slot 已不活跃：可以删映射
	if sess.SlotToken != "" {
		if m.activeSlots[sess.SlotToken] == nil {
			delete(m.slotToSession, sess.SlotToken)
		}
	}

	delete(m.sessions, sess.Token)
}

func (m *fairQueueManager) acquire(user *model.User, ip string) (fairQueueResult, error) {
	if user == nil {
		return fairQueueResult{}, errors.New("user required")
	}

	maxSlotsHost := userDownloadConcurrency(user)
	maxSlotsIP := 0
	if user.IsGuest() && ip != "" {
		maxSlotsIP = ipDownloadConcurrency()
	}
	if maxSlotsHost <= 0 && maxSlotsIP <= 0 {
		return fairQueueResult{Result: "granted"}, nil
	}

	cfg := fairQueueConfig()
	m.ensureGC()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.globalWaiters >= fqGlobalMaxWaiters(cfg) {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(defaultOverloadedRetryAfter / time.Millisecond),
			Reason:     "global_waiters",
		}, nil
	}

	host := fairQueueHostKey(user)
	if host == "" {
		return fairQueueResult{}, errors.New("host required")
	}
	if max := fqMaxWaitersPerHost(cfg); max > 0 && len(m.hostQueues[host]) >= max {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
			Reason:     "host_waiters",
		}, nil
	}
	if maxSlotsIP > 0 && ip != "" {
		key := fairQueueIPKey(ip)
		if max := cfg.MaxWaitersPerIP; max > 0 && m.ipPending[key] >= max {
			return fairQueueResult{
				Result:     "overloaded",
				RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
				Reason:     "ip_waiters",
			}, nil
		}
	}

	token := random.String(16)
	now := time.Now()
	sess := &fairQueueSession{
		Token:        token,
		Host:         host,
		IP:           ip,
		CreatedAt:    now,
		LastSeenAt:   now,
		State:        sessionPending,
		MaxSlotsHost: maxSlotsHost,
		MaxSlotsIP:   maxSlotsIP,
	}
	m.sessions[token] = sess
	m.trackWaiter(sess)

	return fairQueueResult{
		Result:     "pending",
		QueryToken: token,
		RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
	}, nil
}

func (m *fairQueueManager) poll(queryToken string) (fairQueueResult, error) {
	if queryToken == "" {
		return fairQueueResult{}, errors.New("queryToken required")
	}

	cfg := fairQueueConfig()
	m.ensureGC()

	m.mu.Lock()
	defer m.mu.Unlock()

	sess := m.sessions[queryToken]
	if sess == nil {
		return fairQueueResult{Result: "timeout"}, nil
	}

	now := time.Now()
	maxWait := fqMaxWait(cfg)
	if maxWait > 0 && now.Sub(sess.CreatedAt) >= maxWait {
		m.removeSessionLocked(sess)
		return fairQueueResult{Result: "timeout"}, nil
	}
	idle := fqSessionIdle(cfg)
	if idle > 0 && now.Sub(sess.LastSeenAt) >= idle {
		m.removeSessionLocked(sess)
		return fairQueueResult{Result: "timeout"}, nil
	}
	sess.LastSeenAt = now

	switch sess.State {
	case sessionGranted:
		return fairQueueResult{
			Result:     "granted",
			QueryToken: sess.Token,
			SlotToken:  sess.SlotToken,
		}, nil
	}

	queue := m.hostQueues[sess.Host]
	if len(queue) == 0 || queue[0] != sess.Token {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	if !m.canAcquireLocked(sess) {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// 发放 slot
	slotToken := random.String(16)
	slot := &fairQueueSlot{
		Token:        slotToken,
		Host:         sess.Host,
		IP:           sess.IP,
		AcquiredAt:   now,
		MaxSlotsHost: sess.MaxSlotsHost,
	}
	m.activeSlots[slotToken] = slot
	m.slotToSession[slotToken] = sess.Token

	if sess.MaxSlotsHost > 0 {
		m.hostActive[sess.Host]++
	}
	if sess.MaxSlotsIP > 0 && sess.IP != "" {
		key := fairQueueIPKey(sess.IP)
		m.ipActive[key]++
	}

	sess.State = sessionGranted
	sess.SlotToken = slotToken
	m.untrackWaiter(sess)

	// 修复点：granted cleanup 不再“强删 granted session”，只会在 slot 已经不存在时才清 session
	m.scheduleGrantedCleanupLocked(sess.Token, fqGrantedCleanupDelay(cfg))

	return fairQueueResult{
		Result:     "granted",
		QueryToken: sess.Token,
		SlotToken:  slotToken,
	}, nil
}

// 修复点：cleanup 只在 slot 已经不活跃（已 release / 已 zombie）时才 remove session
func (m *fairQueueManager) scheduleGrantedCleanupLocked(token string, delay time.Duration) {
	sess := m.sessions[token]
	if sess == nil || sess.CleanupScheduled {
		return
	}
	sess.CleanupScheduled = true
	if delay <= 0 {
		// 立即清理也要遵循“slot 不活跃才清”的规则
		if sess.State != sessionGranted || sess.SlotToken == "" || m.activeSlots[sess.SlotToken] == nil {
			m.removeSessionLocked(sess)
		}
		return
	}

	go func() {
		time.Sleep(delay)
		m.mu.Lock()
		defer m.mu.Unlock()

		sess := m.sessions[token]
		if sess == nil || sess.State != sessionGranted {
			return
		}

		// 只有当 slot 已经不在 activeSlots，才清 session
		if sess.SlotToken == "" || m.activeSlots[sess.SlotToken] == nil {
			m.removeSessionLocked(sess)
		}
	}()
}

func (m *fairQueueManager) canAcquireLocked(sess *fairQueueSession) bool {
	if sess == nil {
		return false
	}
	if sess.MaxSlotsHost > 0 {
		if m.hostActive[sess.Host] >= sess.MaxSlotsHost {
			return false
		}
	}
	if sess.MaxSlotsIP > 0 && sess.IP != "" {
		key := fairQueueIPKey(sess.IP)
		if m.ipActive[key] >= sess.MaxSlotsIP {
			return false
		}
	}
	return true
}

func (m *fairQueueManager) cancel(queryToken string) bool {
	if queryToken == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	sess := m.sessions[queryToken]
	if sess == nil {
		return false
	}
	if sess.State == sessionGranted && sess.SlotToken != "" {
		slot := m.activeSlots[sess.SlotToken]
		if slot != nil {
			m.releaseSlotLocked(slot)
		}
	}
	m.removeSessionLocked(sess)
	return true
}

func (m *fairQueueManager) release(slotToken string, hitAt time.Time) error {
	if slotToken == "" {
		return nil
	}
	cfg := fairQueueConfig()
	m.ensureGC()

	m.mu.Lock()
	slot := m.activeSlots[slotToken]
	if slot == nil {
		m.mu.Unlock()
		return nil
	}
	if slot.Releasing {
		m.mu.Unlock()
		return nil
	}
	slot.Releasing = true
	m.mu.Unlock()

	minHoldMs := cfg.MinSlotHoldMs
	if minHoldMs < 0 {
		minHoldMs = 0
	}

	if hitAt.IsZero() {
		hitAt = time.Now()
	}
	target := hitAt.Add(time.Duration(minHoldMs) * time.Millisecond)
	now := time.Now()
	if target.Before(now) {
		target = now
	}

	interval := time.Duration(0)
	if cfg.SmoothReleaseIntervalMs != nil {
		if *cfg.SmoothReleaseIntervalMs > 0 {
			interval = time.Duration(*cfg.SmoothReleaseIntervalMs) * time.Millisecond
		}
	} else if minHoldMs > 0 && slot.MaxSlotsHost > 0 {
		interval = time.Duration(minHoldMs/int64(slot.MaxSlotsHost)) * time.Millisecond
	}

	if interval > 0 {
		m.mu.Lock()
		releaser := m.smoothReleaser[slot.Host]
		if releaser == nil {
			releaser = &smoothHostReleaser{}
			m.smoothReleaser[slot.Host] = releaser
		}
		target = releaser.nextReleaseAfter(target, interval)
		m.mu.Unlock()
	}

	delay := time.Until(target)
	if delay < 0 {
		delay = 0
	}

	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		m.mu.Lock()
		defer m.mu.Unlock()

		slot := m.activeSlots[slotToken]
		if slot == nil {
			return
		}
		m.releaseSlotLocked(slot)
	}()

	return nil
}

func (m *fairQueueManager) releaseSlotLocked(slot *fairQueueSlot) {
	if slot == nil {
		return
	}

	// active 计数回收
	if slot.Host != "" {
		if v := m.hostActive[slot.Host]; v > 1 {
			m.hostActive[slot.Host] = v - 1
		} else {
			delete(m.hostActive, slot.Host)
		}
	}
	if slot.IP != "" {
		key := fairQueueIPKey(slot.IP)
		if v := m.ipActive[key]; v > 1 {
			m.ipActive[key] = v - 1
		} else {
			delete(m.ipActive, key)
		}
	}

	// 删除 slot 本体
	delete(m.activeSlots, slot.Token)

	// 如果能找到 session，则顺带清理 session（现在不会因为 cleanup 提前删掉 slotToSession 而失联）
	if sessToken := m.slotToSession[slot.Token]; sessToken != "" {
		if sess := m.sessions[sessToken]; sess != nil {
			m.removeSessionLocked(sess)
		} else {
			// session 已不在（可能手动删/其他原因），把映射清掉避免泄漏
			delete(m.slotToSession, slot.Token)
		}
	}
}

func (m *fairQueueManager) fastAcquire(user *model.User, ip string) (string, time.Time, error) {
	if user == nil {
		return "", time.Time{}, nil
	}
	maxSlotsHost := userDownloadConcurrency(user)
	maxSlotsIP := 0
	if user.IsGuest() && ip != "" {
		maxSlotsIP = ipDownloadConcurrency()
	}
	if maxSlotsHost <= 0 && maxSlotsIP <= 0 {
		return "", time.Time{}, nil
	}

	m.ensureGC()

	m.mu.Lock()
	defer m.mu.Unlock()

	host := fairQueueHostKey(user)
	if host == "" {
		return "", time.Time{}, errors.New("host required")
	}
	if len(m.hostQueues[host]) > 0 {
		return "", time.Time{}, errs.ExceedUserRateLimit
	}
	if maxSlotsIP > 0 && ip != "" {
		key := fairQueueIPKey(ip)
		if m.ipPending[key] > 0 {
			return "", time.Time{}, errs.ExceedIPRateLimit
		}
	}

	if maxSlotsHost > 0 && m.hostActive[host] >= maxSlotsHost {
		return "", time.Time{}, errs.ExceedUserRateLimit
	}
	if maxSlotsIP > 0 && ip != "" {
		key := fairQueueIPKey(ip)
		if m.ipActive[key] >= maxSlotsIP {
			return "", time.Time{}, errs.ExceedIPRateLimit
		}
	}

	token := random.String(16)
	now := time.Now()
	slot := &fairQueueSlot{
		Token:        token,
		Host:         host,
		IP:           ip,
		AcquiredAt:   now,
		MaxSlotsHost: maxSlotsHost,
	}
	m.activeSlots[token] = slot
	if maxSlotsHost > 0 {
		m.hostActive[host]++
	}
	if maxSlotsIP > 0 && ip != "" {
		key := fairQueueIPKey(ip)
		m.ipActive[key]++
	}

	return token, now, nil
}

func FairQueueAcquire(user *model.User, ip string) (fairQueueResult, error) {
	return fairQueue.acquire(user, ip)
}

func FairQueuePoll(queryToken string) (fairQueueResult, error) {
	return fairQueue.poll(queryToken)
}

func FairQueueCancel(queryToken string) bool {
	return fairQueue.cancel(queryToken)
}

func FairQueueRelease(slotToken string, hitAt time.Time) error {
	return fairQueue.release(slotToken, hitAt)
}

func FairQueueFastAcquire(user *model.User, ip string) (string, time.Time, error) {
	return fairQueue.fastAcquire(user, ip)
}

func userDownloadConcurrency(user *model.User) int {
	if user == nil {
		return 0
	}
	if user.DownloadConcurrency != nil {
		return *user.DownloadConcurrency
	}
	if user.IsGuest() {
		return setting.GetInt(conf.GuestDownloadConcurrency, defaultGuestDownloadConcurrency)
	}
	return setting.GetInt(conf.UserDefaultDownloadConcurrency, defaultUserDownloadConcurrency)
}

func ipDownloadConcurrency() int {
	return setting.GetInt(conf.IPDownloadConcurrency, defaultIPDownloadConcurrency)
}
