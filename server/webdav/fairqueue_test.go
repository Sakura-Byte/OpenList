package webdav

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/OpenListTeam/OpenList/v4/drivers/local"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupWebdavTest(t *testing.T, root string) *model.User {
	t.Helper()
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.FairQueue.MinSlotHoldMs = 0
	op.Cache.ClearAll()
	op.Cache.SetSetting(conf.GuestDownloadConcurrency, &model.SettingItem{
		Key:   conf.GuestDownloadConcurrency,
		Value: strconv.Itoa(2),
	})
	op.Cache.SetSetting(conf.IPDownloadConcurrency, &model.SettingItem{
		Key:   conf.IPDownloadConcurrency,
		Value: strconv.Itoa(1),
	})

	dB, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("init db: %v", err)
	}
	db.Init(dB)

	addition, err := json.Marshal(map[string]string{"root_folder_path": root})
	if err != nil {
		t.Fatalf("marshal storage addition: %v", err)
	}
	storage := model.Storage{
		Driver:    "Local",
		MountPath: "/local",
		Addition:  string(addition),
	}
	if _, err := op.CreateStorage(context.Background(), storage); err != nil {
		t.Fatalf("create storage: %v", err)
	}

	return &model.User{
		ID:       1,
		Username: "guest",
		Role:     model.GUEST,
		BasePath: "/local",
	}
}

func TestWebDAVFairQueueGuestIP429(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "test.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	user := setupWebdavTest(t, root)
	ip := "1.2.3.4"

	slotToken, hitAt, err := ratelimit.FairQueueFastAcquire(user, ip)
	if err != nil {
		t.Fatalf("fast acquire: %v", err)
	}
	t.Cleanup(func() {
		_ = ratelimit.FairQueueRelease(slotToken, hitAt)
		time.Sleep(20 * time.Millisecond)
	})

	handler := &Handler{
		Prefix:     "/dav",
		LockSystem: NewMemLS(),
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/dav/test.txt", nil)
	req.Header.Set("CF-Connecting-IP", ip)
	req = req.WithContext(context.WithValue(req.Context(), conf.UserKey, user))
	recorder := httptest.NewRecorder()

	status, err := handler.handleGetHeadPost(recorder, req)
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d (err=%v)", status, err)
	}
	if !errors.Is(err, errs.ExceedIPRateLimit) {
		t.Fatalf("expected ip rate limit error, got %v", err)
	}
}
