package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestAcquireReleaseUserLimit(t *testing.T) {
	downloadConcurrency = newDownloadConcurrencyManager()

	limit := 1
	user := &model.User{ID: 1, Role: model.GENERAL, DownloadConcurrency: &limit}

	_, release, err := AcquireDownload(context.Background(), user, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer release()

	if _, _, err := AcquireDownload(context.Background(), user, ""); !errors.Is(err, errs.ExceedUserRateLimit) {
		t.Fatalf("expected user limit error, got %v", err)
	}

	release()

	if _, release2, err := AcquireDownload(context.Background(), user, ""); err != nil {
		t.Fatalf("expected second acquire after release to succeed, got %v", err)
	} else {
		release2()
	}
}

func TestAcquireReleaseGuestIPLimit(t *testing.T) {
	downloadConcurrency = newDownloadConcurrencyManager()

	limit := 2
	guest := &model.User{ID: 2, Role: model.GUEST, DownloadConcurrency: &limit}
	ip := ""

	var releases []func()
	for i := 0; i < 2; i++ {
		_, release, err := AcquireDownload(context.Background(), guest, ip)
		if err != nil {
			t.Fatalf("unexpected err on acquire %d: %v", i, err)
		}
		releases = append(releases, release)
	}

	if _, _, err := AcquireDownload(context.Background(), guest, ip); !errors.Is(err, errs.ExceedUserRateLimit) {
		t.Fatalf("expected guest/user limit error, got %v", err)
	}

	releases[0]()

	if _, release, err := AcquireDownload(context.Background(), guest, ip); err != nil {
		t.Fatalf("expected acquire after release to succeed, got %v", err)
	} else {
		release()
	}

	for _, r := range releases[1:] {
		r()
	}
}

func TestRenewDownloadExtendsTTL(t *testing.T) {
	downloadConcurrency = newDownloadConcurrencyManager()

	limit := 1
	user := &model.User{ID: 3, Role: model.GENERAL, DownloadConcurrency: &limit}

	leaseID, _, err := AcquireDownload(context.Background(), user, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	downloadConcurrency.mu.Lock()
	lease := downloadConcurrency.leases[leaseID]
	lease.expiresAt = time.Now().Add(time.Second)
	downloadConcurrency.leases[leaseID] = lease
	downloadConcurrency.mu.Unlock()

	if err := RenewDownload(user, "", leaseID); err != nil {
		t.Fatalf("renew should succeed, got %v", err)
	}

	downloadConcurrency.mu.Lock()
	downloadConcurrency.cleanupExpiredLocked()
	renewedLease, ok := downloadConcurrency.leases[leaseID]
	downloadConcurrency.mu.Unlock()

	if !ok {
		t.Fatalf("lease should still exist after renew + cleanup")
	}
	if renewedLease.expiresAt.Before(time.Now().Add(9 * time.Minute)) {
		t.Fatalf("expected lease to be extended close to TTL, got %v", renewedLease.expiresAt)
	}
}

func TestRenewDownloadFailsForInvalidLease(t *testing.T) {
	downloadConcurrency = newDownloadConcurrencyManager()

	limit := 1
	user := &model.User{ID: 4, Role: model.GENERAL, DownloadConcurrency: &limit}

	leaseID, _, err := AcquireDownload(context.Background(), user, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	downloadConcurrency.mu.Lock()
	downloadConcurrency.releaseLeaseLocked(downloadConcurrency.leases[leaseID])
	delete(downloadConcurrency.leases, leaseID)
	downloadConcurrency.mu.Unlock()

	if err := RenewDownload(user, "", leaseID); !errors.Is(err, errs.InvalidDownloadLease) {
		t.Fatalf("expected invalid lease error, got %v", err)
	}
}
