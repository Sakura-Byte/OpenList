package handles

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/gin-gonic/gin"
)

type stubObj struct {
	name string
	size int64
	path string
}

func (s stubObj) GetSize() int64          { return s.size }
func (s stubObj) GetName() string         { return s.name }
func (s stubObj) ModTime() time.Time      { return time.Now() }
func (s stubObj) CreateTime() time.Time   { return time.Now() }
func (s stubObj) IsDir() bool             { return false }
func (s stubObj) GetHash() utils.HashInfo { return utils.HashInfo{} }
func (s stubObj) GetID() string           { return "" }
func (s stubObj) GetPath() string         { return s.path }

func setupProxyTest(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.FairQueue.MinSlotHoldMs = 0
	op.Cache.ClearAll()
	op.Cache.SetSetting(conf.UserDefaultDownloadConcurrency, &model.SettingItem{
		Key:   conf.UserDefaultDownloadConcurrency,
		Value: strconv.Itoa(1),
	})
	op.Cache.SetSetting(conf.ForwardDirectLinkParams, &model.SettingItem{
		Key:   conf.ForwardDirectLinkParams,
		Value: "0",
	})
	op.Cache.SetSetting(conf.FilterReadMeScripts, &model.SettingItem{
		Key:   conf.FilterReadMeScripts,
		Value: "0",
	})
}

func TestProxyFairQueueUserConcurrency429(t *testing.T) {
	setupProxyTest(t)

	user := &model.User{ID: 10, Role: model.GENERAL, Username: "test_user"}
	ip := "10.0.0.1"

	slotToken, hitAt, err := ratelimit.FairQueueFastAcquire(user.Username, user.IsGuest(), ip)
	if err != nil {
		t.Fatalf("fast acquire: %v", err)
	}
	t.Cleanup(func() {
		_ = ratelimit.FairQueueRelease(slotToken, hitAt)
		time.Sleep(20 * time.Millisecond)
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/d/test.txt", nil)
	reqCtx := context.WithValue(req.Context(), conf.UserKey, user)
	reqCtx = context.WithValue(reqCtx, conf.ClientIPKey, ip)
	req = req.WithContext(reqCtx)
	ctx.Request = req

	link := &model.Link{}
	file := stubObj{name: "test.txt", size: 12, path: "/test.txt"}

	proxy(ctx, link, file, false)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", recorder.Code)
	}
}
