// Package gdlimit limits how many Google Drive files one IP downloads in a
// rolling 24 hours and records the downloads per author for migration planning.
//
// Requests for the same file from the same IP form one download while they stay
// within the session window, so multi-threaded downloaders and resumes count
// once, and a download in progress is never cut off by the limit.
package gdlimit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const (
	ReasonIPDailyLimit = "gd_ip_daily_limit"
	StatsArea          = "/3D"

	googleDriveDriver = "GoogleDrive"
	windowHours       = 24
	maxAliasDepth     = 4
	sweepInterval     = time.Minute
)

// Decision is the result of admitting one download request.
type Decision struct {
	Allowed bool
	Limit   int
	Used    int
	ResetAt time.Time
}

// Quota is what the frontend shows before a file is played or downloaded.
type Quota struct {
	Limit     int   `json:"limit"`
	Used      int   `json:"used"`
	Remaining int   `json:"remaining"`
	ResetAt   int64 `json:"reset_at"`
	Blocked   bool  `json:"blocked"`
}

var (
	mu        sync.Mutex
	sessions  = map[string]time.Time{}
	lastSweep time.Time
	nowFunc   = time.Now
	// resolveFile reports whether a file lives on Google Drive, and its size.
	resolveFile = resolveGoogleDrive
)

// Admit decides whether ip may download path, and counts it when it starts a new
// download. It fails open: an error here must not take downloads down.
func Admit(ctx context.Context, ip, path string) Decision {
	path = utils.FixAndCleanPath(path)
	limit := setting.GetInt(conf.GDDownloadIPDailyLimit, 0)
	ipKey := hashIP(ip)
	if ipKey == "" {
		return Decision{Allowed: true, Limit: limit}
	}
	key := ipKey + "\x00" + path
	now := nowFunc()
	window := sessionWindow()

	mu.Lock()
	sweepLocked(now, window)
	inSession := touchLocked(key, now, window)
	mu.Unlock()
	if inSession {
		return Decision{Allowed: true, Limit: limit}
	}

	isGD, size, err := resolveFile(ctx, path)
	if err != nil {
		log.Debugf("gdlimit: resolve %s: %v", path, err)
		return Decision{Allowed: true, Limit: limit}
	}

	mu.Lock()
	defer mu.Unlock()
	if !isGD {
		sessions[key] = now
		return Decision{Allowed: true, Limit: limit}
	}
	// another request of the same download may have counted it during the resolve
	if touchLocked(key, now, window) {
		return Decision{Allowed: true, Limit: limit}
	}
	used, resetAt, err := usage(ipKey, limit, now)
	if err != nil {
		log.Warnf("gdlimit: read usage: %v", err)
		return Decision{Allowed: true, Limit: limit}
	}
	if limit > 0 && used >= limit {
		return Decision{Allowed: false, Limit: limit, Used: used, ResetAt: resetAt}
	}
	sessions[key] = now
	if err := db.IncGDDownloadCount(ipKey, hourBucket(now)); err != nil {
		log.Warnf("gdlimit: count download: %v", err)
	}
	recordAuthor(path, size, now)
	return Decision{Allowed: true, Limit: limit, Used: used + 1, ResetAt: resetAt}
}

// CurrentQuota reports the quota of ip for path without counting anything. It
// returns nil when the limit is off or the file is not on Google Drive.
func CurrentQuota(ctx context.Context, ip, path string) *Quota {
	limit := setting.GetInt(conf.GDDownloadIPDailyLimit, 0)
	ipKey := hashIP(ip)
	if limit <= 0 || ipKey == "" {
		return nil
	}
	path = utils.FixAndCleanPath(path)
	isGD, _, err := resolveFile(ctx, path)
	if err != nil || !isGD {
		return nil
	}
	now := nowFunc()
	mu.Lock()
	lastSeen, inSession := sessions[ipKey+"\x00"+path]
	mu.Unlock()
	inSession = inSession && now.Sub(lastSeen) <= sessionWindow()
	used, resetAt, err := usage(ipKey, limit, now)
	if err != nil {
		log.Warnf("gdlimit: read usage: %v", err)
		return nil
	}
	return &Quota{
		Limit:     limit,
		Used:      used,
		Remaining: max(limit-used, 0),
		ResetAt:   resetAt.Unix(),
		Blocked:   used >= limit && !inSession,
	}
}

// TopAuthors returns the most downloaded authors of the last days days.
func TopAuthors(area string, days, limit int) ([]db.GDDownloadAuthor, error) {
	since := nowFunc().UTC().AddDate(0, 0, 1-days).Format(time.DateOnly)
	return db.TopGDDownloadAuthors(area, since, limit)
}

func touchLocked(key string, now time.Time, window time.Duration) bool {
	lastSeen, ok := sessions[key]
	if !ok || now.Sub(lastSeen) > window {
		return false
	}
	sessions[key] = now
	return true
}

func sessionWindow() time.Duration {
	return time.Duration(setting.GetInt(conf.GDDownloadSessionMinutes, 10)) * time.Minute
}

func hourBucket(t time.Time) int64 {
	return t.Unix() / 3600
}

// usage sums the rolling window and finds when the IP drops below the limit.
func usage(ipKey string, limit int, now time.Time) (int, time.Time, error) {
	rows, err := db.ListGDDownloadCounts(ipKey, hourBucket(now)-windowHours+1)
	if err != nil {
		return 0, time.Time{}, err
	}
	used := 0
	for _, row := range rows {
		used += row.Count
	}
	resetAt := now.Add(windowHours * time.Hour)
	left := used
	for _, row := range rows {
		left -= row.Count
		resetAt = time.Unix((row.HourBucket+windowHours)*3600, 0)
		if limit <= 0 || left < limit {
			break
		}
	}
	return used, resetAt, nil
}

func recordAuthor(path string, size int64, now time.Time) {
	author, _, found := strings.Cut(strings.TrimPrefix(path, StatsArea+"/"), "/")
	if !strings.HasPrefix(path, StatsArea+"/") || !found || author == "" {
		return
	}
	if err := db.IncGDDownloadStat(StatsArea, author, now.UTC().Format(time.DateOnly), size); err != nil {
		log.Warnf("gdlimit: record author: %v", err)
	}
}

func sweepLocked(now time.Time, window time.Duration) {
	if now.Sub(lastSweep) < sweepInterval {
		return
	}
	lastSweep = now
	for key, lastSeen := range sessions {
		if now.Sub(lastSeen) > window {
			delete(sessions, key)
		}
	}
	go func(before int64) {
		if err := db.DeleteGDDownloadCountsBefore(before); err != nil {
			log.Warnf("gdlimit: purge counts: %v", err)
		}
	}(hourBucket(now) - 2*windowHours)
}

// hashIP keeps the database free of client addresses. IPv6 clients usually own a
// whole /64, so one prefix counts as one IP.
func hashIP(ip string) string {
	addr, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return ""
	}
	addr = addr.Unmap()
	if addr.Is6() {
		prefix, err := addr.Prefix(64)
		if err != nil {
			return ""
		}
		addr = prefix.Addr()
	}
	mac := hmac.New(sha256.New, []byte(conf.Conf.JwtSecret))
	mac.Write([]byte(addr.String()))
	return hex.EncodeToString(mac.Sum(nil))[:32]
}

func resolveGoogleDrive(ctx context.Context, path string) (bool, int64, error) {
	for depth := 0; depth < maxAliasDepth; depth++ {
		storage, actualPath, err := op.PeekStorageAndActualPath(path)
		if err != nil {
			return false, 0, err
		}
		if resolver, ok := storage.(driver.BackingPathResolver); ok {
			path, err = resolver.ResolveBackingPath(ctx, actualPath)
			if err != nil {
				return false, 0, err
			}
			continue
		}
		if storage.GetStorage().Driver != googleDriveDriver {
			return false, 0, nil
		}
		obj, err := op.Get(ctx, storage, actualPath)
		if err != nil {
			return false, 0, err
		}
		if obj.IsDir() {
			return false, 0, nil
		}
		return true, obj.GetSize(), nil
	}
	return false, 0, nil
}
