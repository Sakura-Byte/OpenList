package ratelimit

import (
	"context"
	"errors"
	"testing"

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
