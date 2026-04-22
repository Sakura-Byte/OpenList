/*
Package ratelimit implements a fair queue system for download concurrency control.

# Design Overview

## Guest Users (Dual Queue)
Guest users are subject to TWO limits, each with its own FIFO queue:

1. IP Limit (ipQueues[ip], ipActive[ip])
  - Each IP has its own queue
  - Limit: IPDownloadConcurrency per IP

2. Global Guest Limit (guestGlobalQueue, guestTotalActive)
  - All guests share one global queue
  - Limit: GuestDownloadConcurrency total

A guest request can only be granted when:
- It's at the front of its IP queue AND ipActive < ipLimit
- It's at the front of the global queue AND guestTotalActive < guestLimit

## Registered Users (Single Queue)
Each user has their own independent queue:
- userQueues[userId], userActive[userId]
- Limit: UserDownloadConcurrency per user
*/
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
	Result                string `json:"result"`
	WaitToken             string `json:"waitToken,omitempty"`
	SlotToken             string `json:"slotToken,omitempty"`
	ProvisionalDeadlineMs int64  `json:"provisionalDeadlineMs,omitempty"`
	RetryAfter            int    `json:"retryAfter,omitempty"` // milliseconds
	Reason                string `json:"reason,omitempty"`
}

type fairQueueWaiterState string

const (
	waiterStateWaiting            fairQueueWaiterState = "WAITING"
	waiterStateGrantedProvisional fairQueueWaiterState = "GRANTED_PROVISIONAL"
)

type fairQueueSlotState string

const (
	slotStateProvisional fairQueueSlotState = "PROVISIONAL"
	slotStateActive      fairQueueSlotState = "ACTIVE"
)

type FairQueueReleaseReason string

const (
	ReleaseReasonStreamEnd   FairQueueReleaseReason = "stream_end"
	ReleaseReasonClientAbort FairQueueReleaseReason = "client_abort"
	ReleaseReasonUpstreamErr FairQueueReleaseReason = "upstream_error"
	ReleaseReasonWorkerClean FairQueueReleaseReason = "worker_cleanup"
)

const (
	ReleaseReasonUpstreamError FairQueueReleaseReason = ReleaseReasonUpstreamErr
	ReleaseReasonWorkerCleanup FairQueueReleaseReason = ReleaseReasonWorkerClean
)

type fairQueueWaiter struct {
	Token               string
	IP                  string
	UserKey             string
	IsGuest             bool
	CreatedAt           time.Time
	LastSeenAt          time.Time
	State               fairQueueWaiterState
	SlotToken           string
	ProvisionalDeadline time.Time

	MaxSlotsIP   int
	MaxSlotsUser int

	InIPQueue     bool
	InGlobalQueue bool
	InUserQueue   bool
}

type fairQueueSlot struct {
	Token       string
	IP          string
	UserKey     string
	IsGuest     bool
	State       fairQueueSlotState
	GrantedAt   time.Time
	ActivatedAt time.Time
	Releasing   bool
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

	if sr.lastReleaseAt.IsZero() || sr.lastReleaseAt.Before(base) {
		sr.lastReleaseAt = base
		return base
	}

	next := sr.lastReleaseAt.Add(interval)
	sr.lastReleaseAt = next
	return next
}

type fairQueueManager struct {
	mu sync.Mutex

	cfgMu sync.RWMutex
	cfg   conf.FairQueue

	waiters      map[string]*fairQueueWaiter
	slotToWaiter map[string]string
	slots        map[string]*fairQueueSlot

	ipQueues         map[string][]string
	ipActive         map[string]int
	guestGlobalQueue []string
	guestTotalActive int

	userQueues map[string][]string
	userActive map[string]int

	smoothReleaser map[string]*smoothHostReleaser
	globalWaiters  int
	gcOnce         sync.Once
	stopOnce       sync.Once
	stopCh         chan struct{}
	bgWG           sync.WaitGroup
}

var fairQueue = newFairQueueManager()

func newFairQueueManager() *fairQueueManager {
	return &fairQueueManager{
		cfg:            fairQueueConfig(),
		waiters:        make(map[string]*fairQueueWaiter),
		slotToWaiter:   make(map[string]string),
		slots:          make(map[string]*fairQueueSlot),
		ipQueues:       make(map[string][]string),
		ipActive:       make(map[string]int),
		userQueues:     make(map[string][]string),
		userActive:     make(map[string]int),
		smoothReleaser: make(map[string]*smoothHostReleaser),
		stopCh:         make(chan struct{}),
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
	if cfg.DefaultGrantedCleanupDelay <= 0 {
		return 5 * time.Second
	}
	return time.Duration(cfg.DefaultGrantedCleanupDelay) * time.Second
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

func guestDownloadConcurrency() int {
	return setting.GetInt(conf.GuestDownloadConcurrency, defaultGuestDownloadConcurrency)
}

func (m *fairQueueManager) setConfig(cfg conf.FairQueue) {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	m.cfg = cfg
}

func (m *fairQueueManager) currentConfig() conf.FairQueue {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return m.cfg
}

func (m *fairQueueManager) syncConfigFromGlobal() conf.FairQueue {
	cfg := fairQueueConfig()
	m.setConfig(cfg)
	return cfg
}

func (m *fairQueueManager) close() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.bgWG.Wait()
	})
}

func replaceFairQueueManager(mgr *fairQueueManager) {
	if fairQueue != nil {
		fairQueue.close()
	}
	fairQueue = mgr
}

func (m *fairQueueManager) ensureGC() {
	m.gcOnce.Do(func() {
		m.bgWG.Add(1)
		go func() {
			defer m.bgWG.Done()
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					m.gc(m.currentConfig())
				case <-m.stopCh:
					return
				}
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

	for _, waiter := range m.waiters {
		if waiter == nil {
			continue
		}
		switch waiter.State {
		case waiterStateWaiting:
			if maxWait > 0 && now.Sub(waiter.CreatedAt) >= maxWait {
				m.removeWaiterLocked(waiter)
				continue
			}
			if idle > 0 && now.Sub(waiter.LastSeenAt) >= idle {
				m.removeWaiterLocked(waiter)
			}
		case waiterStateGrantedProvisional:
			if !waiter.ProvisionalDeadline.IsZero() && !waiter.ProvisionalDeadline.After(now) {
				if slot := m.slots[waiter.SlotToken]; slot != nil && slot.State == slotStateProvisional {
					m.releaseSlotLocked(slot)
				}
				m.removeWaiterLocked(waiter)
			}
		}
	}

	for token, slot := range m.slots {
		if slot == nil {
			delete(m.slots, token)
			delete(m.slotToWaiter, token)
			continue
		}

		if slot.State == slotStateProvisional {
			waitToken := m.slotToWaiter[token]
			if waitToken == "" || m.waiters[waitToken] == nil {
				m.releaseSlotLocked(slot)
			}
			continue
		}

		if zombie <= 0 {
			continue
		}
		since := slot.ActivatedAt
		if since.IsZero() {
			since = slot.GrantedAt
		}
		if now.Sub(since) >= zombie {
			m.releaseSlotLocked(slot)
		}
	}
}

func (m *fairQueueManager) addToIPQueue(waiter *fairQueueWaiter) {
	if waiter.InIPQueue || waiter.IP == "" {
		return
	}
	m.ipQueues[waiter.IP] = append(m.ipQueues[waiter.IP], waiter.Token)
	waiter.InIPQueue = true
}

func (m *fairQueueManager) removeFromIPQueue(waiter *fairQueueWaiter) {
	if !waiter.InIPQueue || waiter.IP == "" {
		return
	}
	queue := m.ipQueues[waiter.IP]
	out := queue[:0]
	for _, token := range queue {
		if token != waiter.Token {
			out = append(out, token)
		}
	}
	if len(out) == 0 {
		delete(m.ipQueues, waiter.IP)
	} else {
		m.ipQueues[waiter.IP] = out
	}
	waiter.InIPQueue = false
}

func (m *fairQueueManager) addToGlobalQueue(waiter *fairQueueWaiter) {
	if waiter.InGlobalQueue {
		return
	}
	m.guestGlobalQueue = append(m.guestGlobalQueue, waiter.Token)
	waiter.InGlobalQueue = true
	m.globalWaiters++
}

func (m *fairQueueManager) removeFromGlobalQueue(waiter *fairQueueWaiter) {
	if !waiter.InGlobalQueue {
		return
	}
	out := m.guestGlobalQueue[:0]
	for _, token := range m.guestGlobalQueue {
		if token != waiter.Token {
			out = append(out, token)
		}
	}
	m.guestGlobalQueue = out
	waiter.InGlobalQueue = false
	if m.globalWaiters > 0 {
		m.globalWaiters--
	}
}

func (m *fairQueueManager) addToUserQueue(waiter *fairQueueWaiter) {
	if waiter.InUserQueue || waiter.UserKey == "" {
		return
	}
	m.userQueues[waiter.UserKey] = append(m.userQueues[waiter.UserKey], waiter.Token)
	waiter.InUserQueue = true
	m.globalWaiters++
}

func (m *fairQueueManager) removeFromUserQueue(waiter *fairQueueWaiter) {
	if !waiter.InUserQueue || waiter.UserKey == "" {
		return
	}
	queue := m.userQueues[waiter.UserKey]
	out := queue[:0]
	for _, token := range queue {
		if token != waiter.Token {
			out = append(out, token)
		}
	}
	if len(out) == 0 {
		delete(m.userQueues, waiter.UserKey)
	} else {
		m.userQueues[waiter.UserKey] = out
	}
	waiter.InUserQueue = false
	if m.globalWaiters > 0 {
		m.globalWaiters--
	}
}

func (m *fairQueueManager) removeWaiterLocked(waiter *fairQueueWaiter) {
	if waiter == nil {
		return
	}
	if waiter.IsGuest {
		m.removeFromIPQueue(waiter)
		m.removeFromGlobalQueue(waiter)
	} else {
		m.removeFromUserQueue(waiter)
	}
	if waiter.SlotToken != "" && m.slotToWaiter[waiter.SlotToken] == waiter.Token {
		delete(m.slotToWaiter, waiter.SlotToken)
	}
	delete(m.waiters, waiter.Token)
}

func (m *fairQueueManager) acquire(user *model.User, ip, path string) (fairQueueResult, error) {
	if user == nil {
		return fairQueueResult{}, errors.New("user required")
	}

	cfg := m.syncConfigFromGlobal()
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

	if user.IsGuest() {
		return m.acquireGuestLocked(ip, path, cfg)
	}
	return m.acquireUserLocked(user, path, cfg)
}

func (m *fairQueueManager) acquireGuestLocked(ip, path string, cfg conf.FairQueue) (fairQueueResult, error) {
	if ip == "" {
		return fairQueueResult{}, errors.New("guest requires IP")
	}

	ipLimit := ipDownloadConcurrency()
	guestLimit := guestDownloadConcurrency()

	if max := cfg.MaxWaitersPerIP; max > 0 && len(m.ipQueues[ip]) >= max {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
			Reason:     "ip_waiters",
		}, nil
	}

	if len(m.ipQueues[ip]) == 0 {
		ipOK := ipLimit <= 0 || m.ipActive[ip] < ipLimit
		guestOK := guestLimit <= 0 || m.guestTotalActive < guestLimit
		if ipOK && guestOK {
			return m.grantGuestProvisionalLocked(ip, ipLimit, cfg)
		}
	}

	waitToken := random.String(16)
	now := time.Now()
	waiter := &fairQueueWaiter{
		Token:      waitToken,
		IP:         ip,
		IsGuest:    true,
		CreatedAt:  now,
		LastSeenAt: now,
		State:      waiterStateWaiting,
		MaxSlotsIP: ipLimit,
	}
	m.waiters[waitToken] = waiter
	m.addToIPQueue(waiter)
	m.addToGlobalQueue(waiter)

	return fairQueueResult{
		Result:     "pending",
		WaitToken:  waitToken,
		RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
	}, nil
}

func (m *fairQueueManager) acquireUserLocked(user *model.User, path string, cfg conf.FairQueue) (fairQueueResult, error) {
	userKey := fmt.Sprintf("u:%d", user.ID)
	userLimit := userDownloadConcurrency(user)

	if max := fqMaxWaitersPerHost(cfg); max > 0 && len(m.userQueues[userKey]) >= max {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
			Reason:     "user_waiters",
		}, nil
	}

	if len(m.userQueues[userKey]) == 0 && (userLimit <= 0 || m.userActive[userKey] < userLimit) {
		return m.grantUserProvisionalLocked(userKey, userLimit, cfg)
	}

	waitToken := random.String(16)
	now := time.Now()
	waiter := &fairQueueWaiter{
		Token:        waitToken,
		UserKey:      userKey,
		IsGuest:      false,
		CreatedAt:    now,
		LastSeenAt:   now,
		State:        waiterStateWaiting,
		MaxSlotsUser: userLimit,
	}
	m.waiters[waitToken] = waiter
	m.addToUserQueue(waiter)

	return fairQueueResult{
		Result:     "pending",
		WaitToken:  waitToken,
		RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
	}, nil
}

func (m *fairQueueManager) grantGuestProvisionalLocked(ip string, ipLimit int, cfg conf.FairQueue) (fairQueueResult, error) {
	waitToken := random.String(16)
	now := time.Now()
	deadline := now.Add(fqGrantedCleanupDelay(cfg))
	slotToken := random.String(16)

	waiter := &fairQueueWaiter{
		Token:               waitToken,
		IP:                  ip,
		IsGuest:             true,
		CreatedAt:           now,
		LastSeenAt:          now,
		State:               waiterStateGrantedProvisional,
		SlotToken:           slotToken,
		ProvisionalDeadline: deadline,
		MaxSlotsIP:          ipLimit,
	}
	slot := &fairQueueSlot{
		Token:     slotToken,
		IP:        ip,
		IsGuest:   true,
		State:     slotStateProvisional,
		GrantedAt: now,
	}

	m.waiters[waitToken] = waiter
	m.slotToWaiter[slotToken] = waitToken
	m.slots[slotToken] = slot
	if ipLimit > 0 {
		m.ipActive[ip]++
	}
	m.guestTotalActive++

	return fairQueueResult{
		Result:                "granted",
		WaitToken:             waitToken,
		SlotToken:             slotToken,
		ProvisionalDeadlineMs: deadline.UnixMilli(),
	}, nil
}

func (m *fairQueueManager) grantUserProvisionalLocked(userKey string, userLimit int, cfg conf.FairQueue) (fairQueueResult, error) {
	waitToken := random.String(16)
	now := time.Now()
	deadline := now.Add(fqGrantedCleanupDelay(cfg))
	slotToken := random.String(16)

	waiter := &fairQueueWaiter{
		Token:               waitToken,
		UserKey:             userKey,
		IsGuest:             false,
		CreatedAt:           now,
		LastSeenAt:          now,
		State:               waiterStateGrantedProvisional,
		SlotToken:           slotToken,
		ProvisionalDeadline: deadline,
		MaxSlotsUser:        userLimit,
	}
	slot := &fairQueueSlot{
		Token:     slotToken,
		UserKey:   userKey,
		IsGuest:   false,
		State:     slotStateProvisional,
		GrantedAt: now,
	}

	m.waiters[waitToken] = waiter
	m.slotToWaiter[slotToken] = waitToken
	m.slots[slotToken] = slot
	if userLimit > 0 {
		m.userActive[userKey]++
	}

	return fairQueueResult{
		Result:                "granted",
		WaitToken:             waitToken,
		SlotToken:             slotToken,
		ProvisionalDeadlineMs: deadline.UnixMilli(),
	}, nil
}

func (m *fairQueueManager) poll(waitToken string) (fairQueueResult, error) {
	if waitToken == "" {
		return fairQueueResult{}, errors.New("waitToken required")
	}

	cfg := m.syncConfigFromGlobal()
	m.ensureGC()

	m.mu.Lock()
	defer m.mu.Unlock()

	waiter := m.waiters[waitToken]
	if waiter == nil {
		return fairQueueResult{Result: "timeout"}, nil
	}

	now := time.Now()
	maxWait := fqMaxWait(cfg)
	idle := fqSessionIdle(cfg)

	if waiter.State == waiterStateWaiting {
		if maxWait > 0 && now.Sub(waiter.CreatedAt) >= maxWait {
			m.removeWaiterLocked(waiter)
			return fairQueueResult{Result: "timeout"}, nil
		}
		if idle > 0 && now.Sub(waiter.LastSeenAt) >= idle {
			m.removeWaiterLocked(waiter)
			return fairQueueResult{Result: "timeout"}, nil
		}
	}

	waiter.LastSeenAt = now

	if waiter.State == waiterStateGrantedProvisional {
		if !waiter.ProvisionalDeadline.IsZero() && !waiter.ProvisionalDeadline.After(now) {
			if slot := m.slots[waiter.SlotToken]; slot != nil && slot.State == slotStateProvisional {
				m.releaseSlotLocked(slot)
			}
			m.removeWaiterLocked(waiter)
			return fairQueueResult{Result: "timeout"}, nil
		}
		return fairQueueResult{
			Result:                "granted",
			WaitToken:             waiter.Token,
			SlotToken:             waiter.SlotToken,
			ProvisionalDeadlineMs: waiter.ProvisionalDeadline.UnixMilli(),
		}, nil
	}

	if waiter.IsGuest {
		return m.pollGuestLocked(waiter, cfg)
	}
	return m.pollUserLocked(waiter, cfg)
}

func (m *fairQueueManager) pollGuestLocked(waiter *fairQueueWaiter, cfg conf.FairQueue) (fairQueueResult, error) {
	ipQueue := m.ipQueues[waiter.IP]
	if len(ipQueue) == 0 || ipQueue[0] != waiter.Token {
		return fairQueueResult{
			Result:     "pending",
			WaitToken:  waiter.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	if waiter.MaxSlotsIP > 0 && m.ipActive[waiter.IP] >= waiter.MaxSlotsIP {
		return fairQueueResult{
			Result:     "pending",
			WaitToken:  waiter.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	if len(m.guestGlobalQueue) == 0 || m.guestGlobalQueue[0] != waiter.Token {
		return fairQueueResult{
			Result:     "pending",
			WaitToken:  waiter.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	guestLimit := guestDownloadConcurrency()
	if guestLimit > 0 && m.guestTotalActive >= guestLimit {
		return fairQueueResult{
			Result:     "pending",
			WaitToken:  waiter.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	return m.grantQueuedGuestLocked(waiter, cfg)
}

func (m *fairQueueManager) grantQueuedGuestLocked(waiter *fairQueueWaiter, cfg conf.FairQueue) (fairQueueResult, error) {
	slotToken := random.String(16)
	now := time.Now()
	deadline := now.Add(fqGrantedCleanupDelay(cfg))
	slot := &fairQueueSlot{
		Token:     slotToken,
		IP:        waiter.IP,
		IsGuest:   true,
		State:     slotStateProvisional,
		GrantedAt: now,
	}

	m.slots[slotToken] = slot
	m.slotToWaiter[slotToken] = waiter.Token
	if waiter.MaxSlotsIP > 0 {
		m.ipActive[waiter.IP]++
	}
	m.guestTotalActive++

	waiter.State = waiterStateGrantedProvisional
	waiter.SlotToken = slotToken
	waiter.ProvisionalDeadline = deadline
	m.removeFromIPQueue(waiter)
	m.removeFromGlobalQueue(waiter)

	return fairQueueResult{
		Result:                "granted",
		WaitToken:             waiter.Token,
		SlotToken:             slotToken,
		ProvisionalDeadlineMs: deadline.UnixMilli(),
	}, nil
}

func (m *fairQueueManager) pollUserLocked(waiter *fairQueueWaiter, cfg conf.FairQueue) (fairQueueResult, error) {
	userQueue := m.userQueues[waiter.UserKey]
	if len(userQueue) == 0 || userQueue[0] != waiter.Token {
		return fairQueueResult{
			Result:     "pending",
			WaitToken:  waiter.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	if waiter.MaxSlotsUser > 0 && m.userActive[waiter.UserKey] >= waiter.MaxSlotsUser {
		return fairQueueResult{
			Result:     "pending",
			WaitToken:  waiter.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	return m.grantQueuedUserLocked(waiter, cfg)
}

func (m *fairQueueManager) grantQueuedUserLocked(waiter *fairQueueWaiter, cfg conf.FairQueue) (fairQueueResult, error) {
	slotToken := random.String(16)
	now := time.Now()
	deadline := now.Add(fqGrantedCleanupDelay(cfg))
	slot := &fairQueueSlot{
		Token:     slotToken,
		UserKey:   waiter.UserKey,
		IsGuest:   false,
		State:     slotStateProvisional,
		GrantedAt: now,
	}

	m.slots[slotToken] = slot
	m.slotToWaiter[slotToken] = waiter.Token
	if waiter.MaxSlotsUser > 0 {
		m.userActive[waiter.UserKey]++
	}

	waiter.State = waiterStateGrantedProvisional
	waiter.SlotToken = slotToken
	waiter.ProvisionalDeadline = deadline
	m.removeFromUserQueue(waiter)

	return fairQueueResult{
		Result:                "granted",
		WaitToken:             waiter.Token,
		SlotToken:             slotToken,
		ProvisionalDeadlineMs: deadline.UnixMilli(),
	}, nil
}

func (m *fairQueueManager) abandon(waitToken string) bool {
	if waitToken == "" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	waiter := m.waiters[waitToken]
	if waiter == nil {
		return false
	}
	if waiter.State == waiterStateGrantedProvisional && waiter.SlotToken != "" {
		if slot := m.slots[waiter.SlotToken]; slot != nil && slot.State == slotStateProvisional {
			m.releaseSlotLocked(slot)
		}
	}
	m.removeWaiterLocked(waiter)
	return true
}

func (m *fairQueueManager) activate(slotToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot := m.slots[slotToken]
	if slot == nil {
		return errs.ObjectNotFound
	}
	if slot.State == slotStateActive {
		return nil
	}
	if slot.State == slotStateProvisional {
		waitToken := m.slotToWaiter[slotToken]
		waiter := m.waiters[waitToken]
		if waiter == nil || (!waiter.ProvisionalDeadline.IsZero() && !waiter.ProvisionalDeadline.After(time.Now())) {
			m.releaseSlotLocked(slot)
			if waiter != nil {
				m.removeWaiterLocked(waiter)
			}
			return errs.ObjectNotFound
		}
	}

	slot.State = slotStateActive
	slot.ActivatedAt = time.Now()

	if waitToken := m.slotToWaiter[slotToken]; waitToken != "" {
		if waiter := m.waiters[waitToken]; waiter != nil {
			m.removeWaiterLocked(waiter)
		}
		delete(m.slotToWaiter, slotToken)
	}

	return nil
}

func (m *fairQueueManager) release(slotToken string, hitAt time.Time, reason FairQueueReleaseReason) error {
	if slotToken == "" {
		return nil
	}

	cfg := m.syncConfigFromGlobal()
	m.ensureGC()

	m.mu.Lock()
	slot := m.slots[slotToken]
	if slot == nil {
		m.mu.Unlock()
		return nil
	}
	if slot.Releasing {
		m.mu.Unlock()
		return nil
	}

	if reason == ReleaseReasonClientAbort || slot.State == slotStateProvisional {
		m.releaseSlotLocked(slot)
		m.mu.Unlock()
		return nil
	}

	slot.Releasing = true
	isGuest := slot.IsGuest
	userKey := slot.UserKey
	m.mu.Unlock()

	if hitAt.IsZero() {
		hitAt = time.Now()
	}

	minHoldMs := cfg.MinSlotHoldMs
	if minHoldMs < 0 {
		minHoldMs = 0
	}

	target := hitAt.Add(time.Duration(minHoldMs) * time.Millisecond)
	now := time.Now()
	if target.Before(now) {
		target = now
	}

	interval := time.Duration(0)
	if cfg.SmoothReleaseIntervalMs != nil && *cfg.SmoothReleaseIntervalMs > 0 {
		interval = time.Duration(*cfg.SmoothReleaseIntervalMs) * time.Millisecond
	}

	if interval > 0 {
		m.mu.Lock()
		key := userKey
		if isGuest {
			key = "guest"
		}
		releaser := m.smoothReleaser[key]
		if releaser == nil {
			releaser = &smoothHostReleaser{}
			m.smoothReleaser[key] = releaser
		}
		target = releaser.nextReleaseAfter(target, interval)
		m.mu.Unlock()
	}

	delay := time.Until(target)
	if delay < 0 {
		delay = 0
	}

	m.bgWG.Add(1)
	go func() {
		defer m.bgWG.Done()
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-m.stopCh:
				return
			}
		} else {
			select {
			case <-m.stopCh:
				return
			default:
			}
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		slot := m.slots[slotToken]
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

	if slot.IsGuest {
		if slot.IP != "" {
			if v := m.ipActive[slot.IP]; v > 1 {
				m.ipActive[slot.IP] = v - 1
			} else {
				delete(m.ipActive, slot.IP)
			}
		}
		if m.guestTotalActive > 0 {
			m.guestTotalActive--
		}
	} else if slot.UserKey != "" {
		if v := m.userActive[slot.UserKey]; v > 1 {
			m.userActive[slot.UserKey] = v - 1
		} else {
			delete(m.userActive, slot.UserKey)
		}
	}

	delete(m.slots, slot.Token)
	delete(m.slotToWaiter, slot.Token)
}

func (m *fairQueueManager) fastAcquire(user *model.User, ip string) (string, time.Time, error) {
	if user == nil {
		return "", time.Time{}, nil
	}

	m.syncConfigFromGlobal()
	m.ensureGC()
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	if user.IsGuest() {
		if ip == "" {
			return "", time.Time{}, errors.New("guest requires IP")
		}
		ipLimit := ipDownloadConcurrency()
		guestLimit := guestDownloadConcurrency()

		if len(m.ipQueues[ip]) > 0 || len(m.guestGlobalQueue) > 0 {
			return "", time.Time{}, errs.ExceedIPRateLimit
		}
		if ipLimit > 0 && m.ipActive[ip] >= ipLimit {
			return "", time.Time{}, errs.ExceedIPRateLimit
		}
		if guestLimit > 0 && m.guestTotalActive >= guestLimit {
			return "", time.Time{}, errs.ExceedUserRateLimit
		}

		slotToken := random.String(16)
		slot := &fairQueueSlot{
			Token:       slotToken,
			IP:          ip,
			IsGuest:     true,
			State:       slotStateActive,
			GrantedAt:   now,
			ActivatedAt: now,
		}
		m.slots[slotToken] = slot
		if ipLimit > 0 {
			m.ipActive[ip]++
		}
		m.guestTotalActive++
		return slotToken, now, nil
	}

	userKey := fmt.Sprintf("u:%d", user.ID)
	userLimit := userDownloadConcurrency(user)
	if userLimit <= 0 {
		return "", time.Time{}, nil
	}
	if len(m.userQueues[userKey]) > 0 {
		return "", time.Time{}, errs.ExceedUserRateLimit
	}
	if m.userActive[userKey] >= userLimit {
		return "", time.Time{}, errs.ExceedUserRateLimit
	}

	slotToken := random.String(16)
	slot := &fairQueueSlot{
		Token:       slotToken,
		UserKey:     userKey,
		IsGuest:     false,
		State:       slotStateActive,
		GrantedAt:   now,
		ActivatedAt: now,
	}
	m.slots[slotToken] = slot
	m.userActive[userKey]++
	return slotToken, now, nil
}

func FairQueueAcquire(user *model.User, ip, path string) (fairQueueResult, error) {
	return fairQueue.acquire(user, ip, path)
}

func FairQueuePoll(waitToken string) (fairQueueResult, error) {
	return fairQueue.poll(waitToken)
}

func FairQueueAbandon(waitToken string) bool {
	return fairQueue.abandon(waitToken)
}

func FairQueueActivate(slotToken string) error {
	return fairQueue.activate(slotToken)
}

func FairQueueRelease(slotToken string, hitAt time.Time, reason FairQueueReleaseReason) error {
	return fairQueue.release(slotToken, hitAt, reason)
}

func FairQueueFastAcquire(user *model.User, ip string) (string, time.Time, error) {
	return fairQueue.fastAcquire(user, ip)
}
