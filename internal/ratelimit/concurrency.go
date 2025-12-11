package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils/random"
)

const downloadLeaseTTL = time.Second * 20
const (
	defaultUserDownloadConcurrency  = 3
	defaultGuestDownloadConcurrency = 3
	defaultIPDownloadConcurrency    = 3
)

type downloadLease struct {
	id        string
	userID    uint
	ip        string
	useUser   bool
	useIP     bool
	expiresAt time.Time
	released  bool
}

type downloadConcurrencyManager struct {
	mu         sync.Mutex
	userActive map[uint]int
	ipActive   map[string]int
	leases     map[string]*downloadLease
}

var downloadConcurrency = newDownloadConcurrencyManager()

func newDownloadConcurrencyManager() *downloadConcurrencyManager {
	return &downloadConcurrencyManager{
		userActive: make(map[uint]int),
		ipActive:   make(map[string]int),
		leases:     make(map[string]*downloadLease),
	}
}

func slotsFromValue(v int) int {
	if v <= 0 {
		return 0
	}
	return v
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

// AcquireDownload reserves a download concurrency slot for the given user/IP.
// It derives slot counts from the existing download RPS settings.
// Returns a leaseID (empty when unlimited) and a release func (always non-nil).
func AcquireDownload(ctx context.Context, user *model.User, ip string) (string, func(), error) {
	return downloadConcurrency.acquire(ctx, user, ip)
}

// RenewDownload extends an existing lease's TTL. Returns an error if the lease
// is missing or does not belong to the provided user/IP.
func RenewDownload(user *model.User, ip, leaseID string) error {
	return downloadConcurrency.renew(user, ip, leaseID)
}

// ReleaseDownload releases a previously acquired lease by ID, validating the user/IP.
// It returns true when a lease was actually released.
func ReleaseDownload(leaseID string, user *model.User, ip string) bool {
	return downloadConcurrency.release(leaseID, user, ip)
}

// StartDownloadLeaseRenewal periodically renews a download lease until the
// provided context is done or stop() is called. Returns a stop function safe to
// call multiple times.
func StartDownloadLeaseRenewal(ctx context.Context, user *model.User, ip, leaseID string, interval time.Duration) func() {
	if leaseID == "" || user == nil || interval <= 0 {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stopCh := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(stopCh) }) }

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = RenewDownload(user, ip, leaseID)
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			}
		}
	}()
	return stop
}

func (m *downloadConcurrencyManager) acquire(ctx context.Context, user *model.User, ip string) (string, func(), error) {
	if user == nil {
		return "", func() {}, nil
	}

	userLimit := userDownloadConcurrency(user)
	ipLimit := 0
	if user.IsGuest() && ip != "" {
		ipLimit = ipDownloadConcurrency()
	}
	userSlots := slotsFromValue(userLimit)
	ipSlots := slotsFromValue(ipLimit)
	if userSlots == 0 && ipSlots == 0 {
		return "", func() {}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupExpiredLocked()

	if ipSlots > 0 && ip != "" {
		if m.ipActive[ip] >= ipSlots {
			return "", func() {}, errs.ExceedIPRateLimit
		}
	}
	if userSlots > 0 {
		if m.userActive[user.ID] >= userSlots {
			return "", func() {}, errs.ExceedUserRateLimit
		}
	}

	leaseID := random.String(16)
	now := time.Now()
	lease := &downloadLease{
		id:        leaseID,
		userID:    user.ID,
		ip:        ip,
		useUser:   userSlots > 0,
		useIP:     ipSlots > 0 && ip != "",
		expiresAt: now.Add(downloadLeaseTTL),
	}
	if lease.useUser {
		m.userActive[user.ID]++
	}
	if lease.useIP {
		m.ipActive[ip]++
	}
	m.leases[leaseID] = lease

	var once sync.Once
	release := func() {
		once.Do(func() {
			m.release(leaseID, user, ip)
		})
	}

	if ctx != nil {
		go func() {
			<-ctx.Done()
			release()
		}()
	}

	return leaseID, release, nil
}

func (m *downloadConcurrencyManager) renew(user *model.User, ip, leaseID string) error {
	if leaseID == "" || user == nil {
		return errs.InvalidDownloadLease
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupExpiredLocked()

	lease, ok := m.leases[leaseID]
	if !ok || lease.released {
		return errs.InvalidDownloadLease
	}
	if lease.userID != user.ID {
		return errs.InvalidDownloadLease
	}
	if lease.useIP && lease.ip != ip {
		return errs.InvalidDownloadLease
	}

	lease.expiresAt = time.Now().Add(downloadLeaseTTL)
	return nil
}

func (m *downloadConcurrencyManager) release(leaseID string, user *model.User, ip string) bool {
	if leaseID == "" || user == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, ok := m.leases[leaseID]
	if !ok || lease.released {
		return false
	}

	if lease.userID != user.ID {
		return false
	}
	if lease.useIP && lease.ip != ip {
		return false
	}

	m.releaseLeaseLocked(lease)
	return true
}

func (m *downloadConcurrencyManager) cleanupExpiredLocked() {
	now := time.Now()
	for id, lease := range m.leases {
		if lease.expiresAt.After(now) || lease.released {
			continue
		}
		m.releaseLeaseLocked(lease)
		delete(m.leases, id)
	}
}

func (m *downloadConcurrencyManager) releaseLeaseLocked(lease *downloadLease) {
	if lease.released {
		return
	}
	lease.released = true
	if lease.useUser && lease.userID != 0 {
		if v := m.userActive[lease.userID]; v > 0 {
			if v == 1 {
				delete(m.userActive, lease.userID)
			} else {
				m.userActive[lease.userID] = v - 1
			}
		}
	}
	if lease.useIP && lease.ip != "" {
		if v := m.ipActive[lease.ip]; v > 0 {
			if v == 1 {
				delete(m.ipActive, lease.ip)
			} else {
				m.ipActive[lease.ip] = v - 1
			}
		}
	}
	delete(m.leases, lease.id)
}
