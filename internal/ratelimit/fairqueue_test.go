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

	// First acquire should be granted directly (fast path) since no one is waiting
	first, err := FairQueueAcquire(user, ip)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}

	// Second acquire should be pending since IP limit is 1
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
		t.Fatalf("expected second pending due to ip limit, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(first.SlotToken, time.Now()); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)

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

	// First acquire should be granted directly (fast path)
	first, err := FairQueueAcquire(user, "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}

	// Second acquire should be pending since user limit is 1
	second, err := FairQueueAcquire(user, "")
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
		t.Fatalf("expected second pending due to user limit, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(first.SlotToken, time.Now()); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)

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

	// First acquire should be granted directly (fast path)
	first, err := FairQueueAcquire(user, ip)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}

	// FastAcquire should fail fast since there's already an active slot
	_, _, err = FairQueueFastAcquire(user, ip)
	if !errors.Is(err, errs.ExceedUserRateLimit) && !errors.Is(err, errs.ExceedIPRateLimit) {
		t.Fatalf("expected fail fast rate limit, got: %v", err)
	}

	// Cleanup
	if err := FairQueueRelease(first.SlotToken, time.Now()); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)
}

// TestFairQueueNewIPFastPathWhileOthersQueued verifies that a new guest IP can
// get a fast path grant even when other guest IPs are waiting in the queue.
// This prevents the "timeout for ip" error for first-time download IPs.
func TestFairQueueNewIPFastPathWhileOthersQueued(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 9999, // High enough to not be the limiting factor
		conf.IPDownloadConcurrency:    1,    // Each IP can only have 1 concurrent download
	})

	guest := &model.User{ID: 1, Role: model.GUEST}
	ip1 := "10.0.0.1"
	ip2 := "10.0.0.2"
	ip3 := "10.0.0.3"

	// IP1 gets the first slot
	first, err := FairQueueAcquire(guest, ip1)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}

	// IP1 tries to get a second slot, should be pending (IP limit = 1)
	second, err := FairQueueAcquire(guest, ip1)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "pending" || second.QueryToken == "" {
		t.Fatalf("expected second to be pending, got: %#v", second)
	}

	// Now we have IP1 queued. IP2 (a new IP with no pending or active) should still
	// get a fast path grant, NOT be blocked by IP1's queue entry.
	third, err := FairQueueAcquire(guest, ip2)
	if err != nil {
		t.Fatalf("acquire third (new IP): %v", err)
	}
	if third.Result != "granted" || third.SlotToken == "" {
		t.Fatalf("BUG: new IP2 should get fast path grant, got: %#v", third)
	}

	// IP3 (another new IP) should also get fast path grant
	fourth, err := FairQueueAcquire(guest, ip3)
	if err != nil {
		t.Fatalf("acquire fourth (new IP3): %v", err)
	}
	if fourth.Result != "granted" || fourth.SlotToken == "" {
		t.Fatalf("BUG: new IP3 should get fast path grant, got: %#v", fourth)
	}

	// Cleanup
	FairQueueCancel(second.QueryToken)
	if err := FairQueueRelease(first.SlotToken, time.Now()); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)
	if err := FairQueueRelease(third.SlotToken, time.Now()); err != nil {
		t.Fatalf("release third slot: %v", err)
	}
	waitForSlotRelease(t, third.SlotToken)
	if err := FairQueueRelease(fourth.SlotToken, time.Now()); err != nil {
		t.Fatalf("release fourth slot: %v", err)
	}
	waitForSlotRelease(t, fourth.SlotToken)
}
