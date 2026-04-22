package handles

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/ratelimit"
	"github.com/gin-gonic/gin"
)

type fairQueueHandlerResp struct {
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Data    map[string]string `json:"data"`
}

func configureFairQueueHandlerTest(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.FairQueue.MinSlotHoldMs = 0
	op.Cache.ClearAll()
	op.Cache.SetSetting(conf.GuestDownloadConcurrency, &model.SettingItem{
		Key:   conf.GuestDownloadConcurrency,
		Value: strconv.Itoa(1),
	})
	op.Cache.SetSetting(conf.IPDownloadConcurrency, &model.SettingItem{
		Key:   conf.IPDownloadConcurrency,
		Value: strconv.Itoa(1),
	})
}

func runFairQueueJSONHandler(t *testing.T, handler gin.HandlerFunc, target, body string) (int, fairQueueHandlerResp) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	handler(ctx)

	var resp fairQueueHandlerResp
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v; body=%s", err, recorder.Body.String())
	}
	return recorder.Code, resp
}

func TestFairQueueAbandonHandlerReturnsAbandonedThenGone(t *testing.T) {
	configureFairQueueHandlerTest(t)

	guest := &model.User{ID: 1, Role: model.GUEST}
	granted, err := ratelimit.FairQueueAcquire(guest, "8.8.8.1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if granted.WaitToken == "" {
		t.Fatalf("expected wait token, got %#v", granted)
	}

	code, resp := runFairQueueJSONHandler(t, FairQueueAbandon, "/api/admin/fairqueue/abandon", fmt.Sprintf(`{"waitToken":%q}`, granted.WaitToken))
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%+v", code, resp)
	}
	if got := resp.Data["result"]; got != "abandoned" {
		t.Fatalf("expected abandoned, got %q", got)
	}

	code, resp = runFairQueueJSONHandler(t, FairQueueAbandon, "/api/admin/fairqueue/abandon", fmt.Sprintf(`{"waitToken":%q}`, granted.WaitToken))
	if code != http.StatusOK {
		t.Fatalf("expected 200 on second abandon, got %d body=%+v", code, resp)
	}
	if got := resp.Data["result"]; got != "gone" {
		t.Fatalf("expected gone, got %q", got)
	}
}

func TestFairQueueActivateHandlerReturnsActivated(t *testing.T) {
	configureFairQueueHandlerTest(t)

	guest := &model.User{ID: 2, Role: model.GUEST}
	granted, err := ratelimit.FairQueueAcquire(guest, "8.8.8.2", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if granted.SlotToken == "" {
		t.Fatalf("expected slot token, got %#v", granted)
	}

	code, resp := runFairQueueJSONHandler(t, FairQueueActivate, "/api/admin/fairqueue/activate", fmt.Sprintf(`{"slotToken":%q}`, granted.SlotToken))
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%+v", code, resp)
	}
	if got := resp.Data["result"]; got != "activated" {
		t.Fatalf("expected activated, got %q", got)
	}

	if err := ratelimit.FairQueueRelease(granted.SlotToken, time.Now(), ratelimit.ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestFairQueueHandlersRequireTokensAndReason(t *testing.T) {
	configureFairQueueHandlerTest(t)

	tests := []struct {
		name    string
		handler gin.HandlerFunc
		target  string
		body    string
	}{
		{
			name:    "poll requires waitToken",
			handler: FairQueuePoll,
			target:  "/api/admin/fairqueue/poll",
			body:    `{}`,
		},
		{
			name:    "abandon requires waitToken",
			handler: FairQueueAbandon,
			target:  "/api/admin/fairqueue/abandon",
			body:    `{}`,
		},
		{
			name:    "activate requires slotToken",
			handler: FairQueueActivate,
			target:  "/api/admin/fairqueue/activate",
			body:    `{}`,
		},
		{
			name:    "release requires slotToken",
			handler: FairQueueRelease,
			target:  "/api/admin/fairqueue/release",
			body:    `{"reason":"stream_end"}`,
		},
		{
			name:    "release requires reason",
			handler: FairQueueRelease,
			target:  "/api/admin/fairqueue/release",
			body:    `{"slotToken":"slot-1"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, resp := runFairQueueJSONHandler(t, tt.handler, tt.target, tt.body)
			if code != http.StatusOK {
				t.Fatalf("expected HTTP 200 envelope, got %d body=%+v", code, resp)
			}
			if resp.Code != http.StatusBadRequest {
				t.Fatalf("expected response code 400, got %d body=%+v", resp.Code, resp)
			}
		})
	}
}

func TestFairQueueReleaseRejectsInvalidReason(t *testing.T) {
	configureFairQueueHandlerTest(t)

	code, resp := runFairQueueJSONHandler(t, FairQueueRelease, "/api/admin/fairqueue/release", `{"slotToken":"slot-1","reason":"bogus"}`)
	if code != http.StatusOK {
		t.Fatalf("expected HTTP 200 envelope, got %d body=%+v", code, resp)
	}
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected response code 400, got %d body=%+v", resp.Code, resp)
	}
}

func TestFairQueueActivateUnknownSlotReturnsConflict(t *testing.T) {
	configureFairQueueHandlerTest(t)

	code, resp := runFairQueueJSONHandler(t, FairQueueActivate, "/api/admin/fairqueue/activate", `{"slotToken":"missing-slot"}`)
	if code != http.StatusOK {
		t.Fatalf("expected HTTP 200 envelope, got %d body=%+v", code, resp)
	}
	if resp.Code != http.StatusConflict {
		t.Fatalf("expected response code 409, got %d body=%+v", resp.Code, resp)
	}
}
