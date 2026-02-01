package ratelimit

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func setupFairQueueTest(t *testing.T, settings map[string]int) {
	t.Helper()
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.FairQueue = conf.FairQueue{
		MaxWaitMs:                  20000,
		PollIntervalMs:             1,
		SessionIdleSeconds:         300,
		ZombieTimeoutSeconds:       60,
		GlobalMaxWaiters:           1000,
		MaxWaitersPerHost:          1000,
		MaxWaitersPerIP:            1000,
		DefaultGrantedCleanupDelay: 1,
	}
	op.Cache.ClearAll()
	for key, value := range settings {
		op.Cache.SetSetting(key, &model.SettingItem{
			Key:   key,
			Value: strconv.Itoa(value),
		})
	}
	fairQueue = newFairQueueManager()
}

func waitForSlotRelease(t *testing.T, token string) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		fairQueue.mu.Lock()
		_, exists := fairQueue.activeSlots[token]
		fairQueue.mu.Unlock()
		if !exists {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("slot not released: %s", token)
}

func TestFairQueueGuestIPConcurrency(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency:       2,
		conf.IPDownloadConcurrency:          1,
		conf.UserDefaultDownloadConcurrency: 2,
	})

	user := &model.User{ID: 1, Role: model.GUEST}
	ip := "1.2.3.4"

	first, err := FairQueueAcquire(user, ip)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "pending" || first.QueryToken == "" {
		t.Fatalf("expected pending query token, got: %#v", first)
	}

	second, err := FairQueueAcquire(user, ip)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "pending" || second.QueryToken == "" {
		t.Fatalf("expected pending query token, got: %#v", second)
	}

	pollSecond, err := FairQueuePoll(second.QueryToken)
	if err != nil {
		t.Fatalf("poll second: %v", err)
	}
	if pollSecond.Result != "pending" {
		t.Fatalf("expected second pending before head, got: %#v", pollSecond)
	}

	pollFirst, err := FairQueuePoll(first.QueryToken)
	if err != nil {
		t.Fatalf("poll first: %v", err)
	}
	if pollFirst.Result != "granted" || pollFirst.SlotToken == "" {
		t.Fatalf("expected first granted, got: %#v", pollFirst)
	}

	pollSecond, err = FairQueuePoll(second.QueryToken)
	if err != nil {
		t.Fatalf("poll second after first grant: %v", err)
	}
	if pollSecond.Result != "pending" {
		t.Fatalf("expected second pending due to ip limit, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(pollFirst.SlotToken, time.Now()); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, pollFirst.SlotToken)

	pollSecond, err = FairQueuePoll(second.QueryToken)
	if err != nil {
		t.Fatalf("poll second after release: %v", err)
	}
	if pollSecond.Result != "granted" || pollSecond.SlotToken == "" {
		t.Fatalf("expected second granted after release, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(pollSecond.SlotToken, time.Now()); err != nil {
		t.Fatalf("release second slot: %v", err)
	}
	waitForSlotRelease(t, pollSecond.SlotToken)
}

func TestFairQueueUserConcurrency(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.UserDefaultDownloadConcurrency: 1,
	})

	user := &model.User{ID: 42, Role: model.GENERAL}

	first, err := FairQueueAcquire(user, "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	pollFirst, err := FairQueuePoll(first.QueryToken)
	if err != nil {
		t.Fatalf("poll first: %v", err)
	}
	if pollFirst.Result != "granted" || pollFirst.SlotToken == "" {
		t.Fatalf("expected first granted, got: %#v", pollFirst)
	}

	second, err := FairQueueAcquire(user, "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	pollSecond, err := FairQueuePoll(second.QueryToken)
	if err != nil {
		t.Fatalf("poll second: %v", err)
	}
	if pollSecond.Result != "pending" {
		t.Fatalf("expected second pending due to user limit, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(pollFirst.SlotToken, time.Now()); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, pollFirst.SlotToken)

	pollSecond, err = FairQueuePoll(second.QueryToken)
	if err != nil {
		t.Fatalf("poll second after release: %v", err)
	}
	if pollSecond.Result != "granted" || pollSecond.SlotToken == "" {
		t.Fatalf("expected second granted after release, got: %#v", pollSecond)
	}
	if err := FairQueueRelease(pollSecond.SlotToken, time.Now()); err != nil {
		t.Fatalf("release second slot: %v", err)
	}
	waitForSlotRelease(t, pollSecond.SlotToken)
}

func TestFairQueueFastAcquireFailFast(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 1,
		conf.IPDownloadConcurrency:    1,
	})

	user := &model.User{ID: 7, Role: model.GUEST}
	ip := "5.6.7.8"

	pending, err := FairQueueAcquire(user, ip)
	if err != nil {
		t.Fatalf("acquire pending: %v", err)
	}
	if pending.Result != "pending" || pending.QueryToken == "" {
		t.Fatalf("expected pending query token, got: %#v", pending)
	}

	_, _, err = FairQueueFastAcquire(user, ip)
	if !errors.Is(err, errs.ExceedUserRateLimit) {
		t.Fatalf("expected fail fast user limit, got: %v", err)
	}

	if !FairQueueCancel(pending.QueryToken) {
		t.Fatalf("cancel pending session failed")
	}
}
