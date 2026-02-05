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

// fairQueueSession represents a waiting or granted download session.
type fairQueueSession struct {
	Token      string // Unique session identifier
	IP         string // Client IP (guest only)
	UserKey    string // "u:<id>" for registered user, "" for guest
	IsGuest    bool
	CreatedAt  time.Time
	LastSeenAt time.Time
	State      fairQueueSessionState
	SlotToken  string

	// Limits
	MaxSlotsIP   int // IP concurrency limit (guest only)
	MaxSlotsUser int // User concurrency limit (user only)

	// Queue tracking
	InIPQueue        bool // In ipQueues[ip]
	InGlobalQueue    bool // In guestGlobalQueue
	InUserQueue      bool // In userQueues[userKey]
	CleanupScheduled bool
}

// fairQueueSlot represents an active download slot.
type fairQueueSlot struct {
	Token      string
	IP         string // For guest: decrement ipActive and guestTotalActive on release
	UserKey    string // For user: decrement userActive on release
	IsGuest    bool
	AcquiredAt time.Time
	Releasing  bool
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

// fairQueueManager manages all fair queue state.
type fairQueueManager struct {
	mu sync.Mutex

	// Session management
	sessions      map[string]*fairQueueSession // queryToken → session
	slotToSession map[string]string            // slotToken → queryToken

	// Guest: dual queue system
	ipQueues         map[string][]string // ip → ordered list of waiting tokens
	ipActive         map[string]int      // ip → count of active slots
	guestGlobalQueue []string            // all guest tokens in FIFO order
	guestTotalActive int                 // total active guest slots

	// User: single queue system
	userQueues map[string][]string // "u:<id>" → ordered list of waiting tokens
	userActive map[string]int      // "u:<id>" → count of active slots

	// Slot management
	activeSlots map[string]*fairQueueSlot

	// Rate smoothing for releases
	smoothReleaser map[string]*smoothHostReleaser

	// Global state
	globalWaiters int
	gcOnce        sync.Once
}

var fairQueue = newFairQueueManager()

// userConcurrencyOverrides stores username -> downloadConcurrency for users with custom settings.
// Only users with non-null DownloadConcurrency are stored here.
var userConcurrencyOverrides = sync.Map{}

// SetUserConcurrencyOverride updates or removes a user's concurrency override.
// Call this when a user's DownloadConcurrency setting is created/updated/deleted.
func SetUserConcurrencyOverride(username string, concurrency *int) {
	if username == "" {
		return
	}
	if concurrency == nil {
		userConcurrencyOverrides.Delete(username)
	} else {
		userConcurrencyOverrides.Store(username, *concurrency)
	}
}

// GetUserConcurrencyOverride returns a user's custom concurrency limit if set.
func GetUserConcurrencyOverride(username string) (int, bool) {
	if v, ok := userConcurrencyOverrides.Load(username); ok {
		return v.(int), true
	}
	return 0, false
}

// LoadUserConcurrencyOverrides preloads all users with custom concurrency settings.
// Call this during application startup.
func LoadUserConcurrencyOverrides(overrides map[string]int) {
	for username, concurrency := range overrides {
		userConcurrencyOverrides.Store(username, concurrency)
	}
}

// userDownloadConcurrencyByName returns the download concurrency limit for a user by username.
func userDownloadConcurrencyByName(username string, isGuest bool) int {
	if isGuest {
		return setting.GetInt(conf.GuestDownloadConcurrency, defaultGuestDownloadConcurrency)
	}
	if override, ok := GetUserConcurrencyOverride(username); ok {
		return override
	}
	return setting.GetInt(conf.UserDefaultDownloadConcurrency, defaultUserDownloadConcurrency)
}

func newFairQueueManager() *fairQueueManager {
	return &fairQueueManager{
		sessions:       make(map[string]*fairQueueSession),
		slotToSession:  make(map[string]string),
		ipQueues:       make(map[string][]string),
		ipActive:       make(map[string]int),
		userQueues:     make(map[string][]string),
		userActive:     make(map[string]int),
		activeSlots:    make(map[string]*fairQueueSlot),
		smoothReleaser: make(map[string]*smoothHostReleaser),
	}
}

// Config helpers
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

// GC
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

	// Clean pending sessions
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

	// Zombie slot recovery
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

	// Clean orphaned granted sessions
	for _, sess := range m.sessions {
		if sess == nil || sess.State != sessionGranted {
			continue
		}
		if sess.SlotToken == "" || m.activeSlots[sess.SlotToken] == nil {
			m.removeSessionLocked(sess)
		}
	}
}

// Queue management
func (m *fairQueueManager) addToIPQueue(sess *fairQueueSession) {
	if sess.InIPQueue || sess.IP == "" {
		return
	}
	m.ipQueues[sess.IP] = append(m.ipQueues[sess.IP], sess.Token)
	sess.InIPQueue = true
}

func (m *fairQueueManager) removeFromIPQueue(sess *fairQueueSession) {
	if !sess.InIPQueue || sess.IP == "" {
		return
	}
	queue := m.ipQueues[sess.IP]
	out := queue[:0]
	for _, t := range queue {
		if t != sess.Token {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		delete(m.ipQueues, sess.IP)
	} else {
		m.ipQueues[sess.IP] = out
	}
	sess.InIPQueue = false
}

func (m *fairQueueManager) addToGlobalQueue(sess *fairQueueSession) {
	if sess.InGlobalQueue {
		return
	}
	m.guestGlobalQueue = append(m.guestGlobalQueue, sess.Token)
	sess.InGlobalQueue = true
	m.globalWaiters++
}

func (m *fairQueueManager) removeFromGlobalQueue(sess *fairQueueSession) {
	if !sess.InGlobalQueue {
		return
	}
	out := m.guestGlobalQueue[:0]
	for _, t := range m.guestGlobalQueue {
		if t != sess.Token {
			out = append(out, t)
		}
	}
	m.guestGlobalQueue = out
	sess.InGlobalQueue = false
	if m.globalWaiters > 0 {
		m.globalWaiters--
	}
}

func (m *fairQueueManager) addToUserQueue(sess *fairQueueSession) {
	if sess.InUserQueue || sess.UserKey == "" {
		return
	}
	m.userQueues[sess.UserKey] = append(m.userQueues[sess.UserKey], sess.Token)
	sess.InUserQueue = true
	m.globalWaiters++
}

func (m *fairQueueManager) removeFromUserQueue(sess *fairQueueSession) {
	if !sess.InUserQueue || sess.UserKey == "" {
		return
	}
	queue := m.userQueues[sess.UserKey]
	out := queue[:0]
	for _, t := range queue {
		if t != sess.Token {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		delete(m.userQueues, sess.UserKey)
	} else {
		m.userQueues[sess.UserKey] = out
	}
	sess.InUserQueue = false
	if m.globalWaiters > 0 {
		m.globalWaiters--
	}
}

func (m *fairQueueManager) removeSessionLocked(sess *fairQueueSession) {
	if sess == nil {
		return
	}
	if sess.IsGuest {
		m.removeFromIPQueue(sess)
		m.removeFromGlobalQueue(sess)
	} else {
		m.removeFromUserQueue(sess)
	}
	if sess.SlotToken != "" && m.activeSlots[sess.SlotToken] == nil {
		delete(m.slotToSession, sess.SlotToken)
	}
	delete(m.sessions, sess.Token)
}

// Core: acquire
func (m *fairQueueManager) acquire(username string, isGuest bool, ip string) (fairQueueResult, error) {
	if username == "" && !isGuest {
		return fairQueueResult{}, errors.New("username required for non-guest")
	}

	cfg := fairQueueConfig()
	m.ensureGC()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check global waiter limit
	if m.globalWaiters >= fqGlobalMaxWaiters(cfg) {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(defaultOverloadedRetryAfter / time.Millisecond),
			Reason:     "global_waiters",
		}, nil
	}

	if isGuest {
		return m.acquireGuestLocked(ip, cfg)
	}
	return m.acquireUserLocked(username, cfg)
}

func (m *fairQueueManager) acquireGuestLocked(ip string, cfg conf.FairQueue) (fairQueueResult, error) {
	if ip == "" {
		return fairQueueResult{}, errors.New("guest requires IP")
	}

	ipLimit := ipDownloadConcurrency()
	guestLimit := guestDownloadConcurrency()

	// Check per-IP waiter limit
	if max := cfg.MaxWaitersPerIP; max > 0 && len(m.ipQueues[ip]) >= max {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
			Reason:     "ip_waiters",
		}, nil
	}

	// Fast path: this IP has no pending AND has capacity AND guest total has capacity
	// Note: we don't check guestGlobalQueue - different IPs are independent
	canFastPath := len(m.ipQueues[ip]) == 0
	if canFastPath {
		ipOK := ipLimit <= 0 || m.ipActive[ip] < ipLimit
		guestOK := guestLimit <= 0 || m.guestTotalActive < guestLimit
		if ipOK && guestOK {
			return m.grantGuestSlotLocked(ip, ipLimit)
		}
	}

	// Slow path: create pending session
	token := random.String(16)
	now := time.Now()
	sess := &fairQueueSession{
		Token:      token,
		IP:         ip,
		IsGuest:    true,
		CreatedAt:  now,
		LastSeenAt: now,
		State:      sessionPending,
		MaxSlotsIP: ipLimit,
	}
	m.sessions[token] = sess
	m.addToIPQueue(sess)
	m.addToGlobalQueue(sess)

	return fairQueueResult{
		Result:     "pending",
		QueryToken: token,
		RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
	}, nil
}

func (m *fairQueueManager) acquireUserLocked(username string, cfg conf.FairQueue) (fairQueueResult, error) {
	userKey := fmt.Sprintf("u:%s", username)
	userLimit := userDownloadConcurrencyByName(username, false)

	if userLimit <= 0 {
		return fairQueueResult{Result: "granted"}, nil
	}

	// Check per-user waiter limit
	if max := fqMaxWaitersPerHost(cfg); max > 0 && len(m.userQueues[userKey]) >= max {
		return fairQueueResult{
			Result:     "overloaded",
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
			Reason:     "user_waiters",
		}, nil
	}

	// Fast path
	if len(m.userQueues[userKey]) == 0 && m.userActive[userKey] < userLimit {
		return m.grantUserSlotLocked(userKey, userLimit)
	}

	// Slow path
	token := random.String(16)
	now := time.Now()
	sess := &fairQueueSession{
		Token:        token,
		UserKey:      userKey,
		IsGuest:      false,
		CreatedAt:    now,
		LastSeenAt:   now,
		State:        sessionPending,
		MaxSlotsUser: userLimit,
	}
	m.sessions[token] = sess
	m.addToUserQueue(sess)

	return fairQueueResult{
		Result:     "pending",
		QueryToken: token,
		RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
	}, nil
}

func (m *fairQueueManager) grantGuestSlotLocked(ip string, ipLimit int) (fairQueueResult, error) {
	slotToken := random.String(16)
	now := time.Now()
	slot := &fairQueueSlot{
		Token:      slotToken,
		IP:         ip,
		IsGuest:    true,
		AcquiredAt: now,
	}
	m.activeSlots[slotToken] = slot
	if ipLimit > 0 {
		m.ipActive[ip]++
	}
	m.guestTotalActive++

	return fairQueueResult{
		Result:    "granted",
		SlotToken: slotToken,
	}, nil
}

func (m *fairQueueManager) grantUserSlotLocked(userKey string, userLimit int) (fairQueueResult, error) {
	slotToken := random.String(16)
	now := time.Now()
	slot := &fairQueueSlot{
		Token:      slotToken,
		UserKey:    userKey,
		IsGuest:    false,
		AcquiredAt: now,
	}
	m.activeSlots[slotToken] = slot
	if userLimit > 0 {
		m.userActive[userKey]++
	}

	return fairQueueResult{
		Result:    "granted",
		SlotToken: slotToken,
	}, nil
}

// Core: poll
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

	if sess.State == sessionGranted {
		return fairQueueResult{
			Result:     "granted",
			QueryToken: sess.Token,
			SlotToken:  sess.SlotToken,
		}, nil
	}

	if sess.IsGuest {
		return m.pollGuestLocked(sess, cfg)
	}
	return m.pollUserLocked(sess, cfg)
}

func (m *fairQueueManager) pollGuestLocked(sess *fairQueueSession, cfg conf.FairQueue) (fairQueueResult, error) {
	ip := sess.IP
	ipLimit := sess.MaxSlotsIP
	guestLimit := guestDownloadConcurrency()

	// Check 1: at front of IP queue
	ipQueue := m.ipQueues[ip]
	if len(ipQueue) == 0 || ipQueue[0] != sess.Token {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// Check 2: IP has capacity
	if ipLimit > 0 && m.ipActive[ip] >= ipLimit {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// Check 3: at front of global queue
	if len(m.guestGlobalQueue) == 0 || m.guestGlobalQueue[0] != sess.Token {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// Check 4: global guest has capacity
	if guestLimit > 0 && m.guestTotalActive >= guestLimit {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// All checks passed, grant the slot
	slotToken := random.String(16)
	now := time.Now()
	slot := &fairQueueSlot{
		Token:      slotToken,
		IP:         ip,
		IsGuest:    true,
		AcquiredAt: now,
	}
	m.activeSlots[slotToken] = slot
	m.slotToSession[slotToken] = sess.Token

	if ipLimit > 0 {
		m.ipActive[ip]++
	}
	m.guestTotalActive++

	sess.State = sessionGranted
	sess.SlotToken = slotToken
	m.removeFromIPQueue(sess)
	m.removeFromGlobalQueue(sess)

	m.scheduleGrantedCleanupLocked(sess.Token, fqGrantedCleanupDelay(cfg))

	return fairQueueResult{
		Result:     "granted",
		QueryToken: sess.Token,
		SlotToken:  slotToken,
	}, nil
}

func (m *fairQueueManager) pollUserLocked(sess *fairQueueSession, cfg conf.FairQueue) (fairQueueResult, error) {
	userKey := sess.UserKey
	userLimit := sess.MaxSlotsUser

	// Check 1: at front of user queue
	userQueue := m.userQueues[userKey]
	if len(userQueue) == 0 || userQueue[0] != sess.Token {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// Check 2: user has capacity
	if userLimit > 0 && m.userActive[userKey] >= userLimit {
		return fairQueueResult{
			Result:     "pending",
			QueryToken: sess.Token,
			RetryAfter: int(fqPollInterval(cfg) / time.Millisecond),
		}, nil
	}

	// Grant the slot
	slotToken := random.String(16)
	now := time.Now()
	slot := &fairQueueSlot{
		Token:      slotToken,
		UserKey:    userKey,
		IsGuest:    false,
		AcquiredAt: now,
	}
	m.activeSlots[slotToken] = slot
	m.slotToSession[slotToken] = sess.Token

	if userLimit > 0 {
		m.userActive[userKey]++
	}

	sess.State = sessionGranted
	sess.SlotToken = slotToken
	m.removeFromUserQueue(sess)

	m.scheduleGrantedCleanupLocked(sess.Token, fqGrantedCleanupDelay(cfg))

	return fairQueueResult{
		Result:     "granted",
		QueryToken: sess.Token,
		SlotToken:  slotToken,
	}, nil
}

func (m *fairQueueManager) scheduleGrantedCleanupLocked(token string, delay time.Duration) {
	sess := m.sessions[token]
	if sess == nil || sess.CleanupScheduled {
		return
	}
	sess.CleanupScheduled = true
	if delay <= 0 {
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
		if sess.SlotToken == "" || m.activeSlots[sess.SlotToken] == nil {
			m.removeSessionLocked(sess)
		}
	}()
}

// Core: cancel
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

// Core: release
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
	isGuest := slot.IsGuest
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
	}

	if interval > 0 {
		m.mu.Lock()
		var key string
		if isGuest {
			key = "guest"
		} else {
			key = slot.UserKey
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
	} else {
		if slot.UserKey != "" {
			if v := m.userActive[slot.UserKey]; v > 1 {
				m.userActive[slot.UserKey] = v - 1
			} else {
				delete(m.userActive, slot.UserKey)
			}
		}
	}

	delete(m.activeSlots, slot.Token)

	if sessToken := m.slotToSession[slot.Token]; sessToken != "" {
		if sess := m.sessions[sessToken]; sess != nil {
			m.removeSessionLocked(sess)
		} else {
			delete(m.slotToSession, slot.Token)
		}
	}
}

// FastAcquire for sync path (non-polling)
func (m *fairQueueManager) fastAcquire(username string, isGuest bool, ip string) (string, time.Time, error) {
	if username == "" && !isGuest {
		return "", time.Time{}, nil
	}

	m.ensureGC()

	m.mu.Lock()
	defer m.mu.Unlock()

	if isGuest {
		if ip == "" {
			return "", time.Time{}, errors.New("guest requires IP")
		}
		ipLimit := ipDownloadConcurrency()
		guestLimit := guestDownloadConcurrency()

		// Must have no pending
		if len(m.ipQueues[ip]) > 0 || len(m.guestGlobalQueue) > 0 {
			return "", time.Time{}, errs.ExceedIPRateLimit
		}
		// Check IP capacity
		if ipLimit > 0 && m.ipActive[ip] >= ipLimit {
			return "", time.Time{}, errs.ExceedIPRateLimit
		}
		// Check guest capacity
		if guestLimit > 0 && m.guestTotalActive >= guestLimit {
			return "", time.Time{}, errs.ExceedUserRateLimit
		}

		// Grant
		slotToken := random.String(16)
		now := time.Now()
		slot := &fairQueueSlot{
			Token:      slotToken,
			IP:         ip,
			IsGuest:    true,
			AcquiredAt: now,
		}
		m.activeSlots[slotToken] = slot
		if ipLimit > 0 {
			m.ipActive[ip]++
		}
		m.guestTotalActive++
		return slotToken, now, nil
	}

	// User path
	userKey := fmt.Sprintf("u:%s", username)
	userLimit := userDownloadConcurrencyByName(username, false)

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
	now := time.Now()
	slot := &fairQueueSlot{
		Token:      slotToken,
		UserKey:    userKey,
		IsGuest:    false,
		AcquiredAt: now,
	}
	m.activeSlots[slotToken] = slot
	m.userActive[userKey]++
	return slotToken, now, nil
}

// Public API
func FairQueueAcquire(username string, isGuest bool, ip string) (fairQueueResult, error) {
	return fairQueue.acquire(username, isGuest, ip)
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

func FairQueueFastAcquire(username string, isGuest bool, ip string) (string, time.Time, error) {
	return fairQueue.fastAcquire(username, isGuest, ip)
}
