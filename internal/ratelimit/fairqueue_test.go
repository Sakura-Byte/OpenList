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
	replaceFairQueueManager(newFairQueueManager())
}

func fairQueueHasSlot(token string) bool {
	fairQueue.mu.Lock()
	defer fairQueue.mu.Unlock()
	_, exists := fairQueue.slots[token]
	return exists
}

func waitForSlotRelease(t *testing.T, token string) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !fairQueueHasSlot(token) {
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
	first, err := FairQueueAcquire(user, ip, "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.WaitToken == "" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}
	if err := FairQueueActivate(first.SlotToken); err != nil {
		t.Fatalf("activate first slot: %v", err)
	}

	// Second acquire should be pending since IP limit is 1
	second, err := FairQueueAcquire(user, ip, "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "pending" || second.WaitToken == "" {
		t.Fatalf("expected pending wait token, got: %#v", second)
	}

	pollSecond, err := FairQueuePoll(second.WaitToken)
	if err != nil {
		t.Fatalf("poll second: %v", err)
	}
	if pollSecond.Result != "pending" {
		t.Fatalf("expected second pending due to ip limit, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(first.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)

	pollSecond, err = FairQueuePoll(second.WaitToken)
	if err != nil {
		t.Fatalf("poll second after release: %v", err)
	}
	if pollSecond.Result != "granted" || pollSecond.WaitToken == "" || pollSecond.SlotToken == "" {
		t.Fatalf("expected second granted after release, got: %#v", pollSecond)
	}
	if err := FairQueueActivate(pollSecond.SlotToken); err != nil {
		t.Fatalf("activate second slot: %v", err)
	}

	if err := FairQueueRelease(pollSecond.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
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
	first, err := FairQueueAcquire(user, "", "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.WaitToken == "" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}
	if err := FairQueueActivate(first.SlotToken); err != nil {
		t.Fatalf("activate first slot: %v", err)
	}

	// Second acquire should be pending since user limit is 1
	second, err := FairQueueAcquire(user, "", "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "pending" || second.WaitToken == "" {
		t.Fatalf("expected pending wait token, got: %#v", second)
	}

	pollSecond, err := FairQueuePoll(second.WaitToken)
	if err != nil {
		t.Fatalf("poll second: %v", err)
	}
	if pollSecond.Result != "pending" {
		t.Fatalf("expected second pending due to user limit, got: %#v", pollSecond)
	}

	if err := FairQueueRelease(first.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)

	pollSecond, err = FairQueuePoll(second.WaitToken)
	if err != nil {
		t.Fatalf("poll second after release: %v", err)
	}
	if pollSecond.Result != "granted" || pollSecond.WaitToken == "" || pollSecond.SlotToken == "" {
		t.Fatalf("expected second granted after release, got: %#v", pollSecond)
	}
	if err := FairQueueActivate(pollSecond.SlotToken); err != nil {
		t.Fatalf("activate second slot: %v", err)
	}
	if err := FairQueueRelease(pollSecond.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
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
	first, err := FairQueueAcquire(user, ip, "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.WaitToken == "" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}
	if err := FairQueueActivate(first.SlotToken); err != nil {
		t.Fatalf("activate first slot: %v", err)
	}

	// FastAcquire should fail fast since there's already an active slot
	_, _, err = FairQueueFastAcquire(user, ip)
	if !errors.Is(err, errs.ExceedUserRateLimit) && !errors.Is(err, errs.ExceedIPRateLimit) {
		t.Fatalf("expected fail fast rate limit, got: %v", err)
	}

	// Cleanup
	if err := FairQueueRelease(first.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
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
	first, err := FairQueueAcquire(guest, ip1, "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.WaitToken == "" || first.SlotToken == "" {
		t.Fatalf("expected first granted directly, got: %#v", first)
	}
	if err := FairQueueActivate(first.SlotToken); err != nil {
		t.Fatalf("activate first slot: %v", err)
	}

	// IP1 tries to get a second slot, should be pending (IP limit = 1)
	second, err := FairQueueAcquire(guest, ip1, "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "pending" || second.WaitToken == "" {
		t.Fatalf("expected second to be pending, got: %#v", second)
	}

	// Now we have IP1 queued. IP2 (a new IP with no pending or active) should still
	// get a fast path grant, NOT be blocked by IP1's queue entry.
	third, err := FairQueueAcquire(guest, ip2, "")
	if err != nil {
		t.Fatalf("acquire third (new IP): %v", err)
	}
	if third.Result != "granted" || third.WaitToken == "" || third.SlotToken == "" {
		t.Fatalf("BUG: new IP2 should get fast path grant, got: %#v", third)
	}
	if err := FairQueueActivate(third.SlotToken); err != nil {
		t.Fatalf("activate third slot: %v", err)
	}

	// IP3 (another new IP) should also get fast path grant
	fourth, err := FairQueueAcquire(guest, ip3, "")
	if err != nil {
		t.Fatalf("acquire fourth (new IP3): %v", err)
	}
	if fourth.Result != "granted" || fourth.WaitToken == "" || fourth.SlotToken == "" {
		t.Fatalf("BUG: new IP3 should get fast path grant, got: %#v", fourth)
	}
	if err := FairQueueActivate(fourth.SlotToken); err != nil {
		t.Fatalf("activate fourth slot: %v", err)
	}

	// Cleanup
	if !FairQueueAbandon(second.WaitToken) {
		t.Fatalf("abandon second waiter")
	}
	if err := FairQueueRelease(first.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	waitForSlotRelease(t, first.SlotToken)
	if err := FairQueueRelease(third.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release third slot: %v", err)
	}
	waitForSlotRelease(t, third.SlotToken)
	if err := FairQueueRelease(fourth.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release fourth slot: %v", err)
	}
	waitForSlotRelease(t, fourth.SlotToken)
}

// TestFairQueueGuestDualLock verifies that guests are limited by BOTH:
// 1. Per-IP concurrency (each IP independent)
// 2. Total guest slots (all guests share, FIFO when full)
func TestFairQueueGuestDualLock(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 2, // Total guest limit = 2
		conf.IPDownloadConcurrency:    5, // Per-IP limit = 5 (higher than total)
	})

	guest := &model.User{ID: 1, Role: model.GUEST}

	// IP1 gets slot 1 (guestTotal=1)
	r1, err := FairQueueAcquire(guest, "1.1.1.1", "")
	if err != nil {
		t.Fatalf("acquire r1: %v", err)
	}
	if r1.Result != "granted" || r1.WaitToken == "" || r1.SlotToken == "" {
		t.Fatalf("expected r1 granted, got: %#v", r1)
	}
	if err := FairQueueActivate(r1.SlotToken); err != nil {
		t.Fatalf("activate r1: %v", err)
	}

	// IP2 gets slot 2 (guestTotal=2, now at limit)
	r2, err := FairQueueAcquire(guest, "2.2.2.2", "")
	if err != nil {
		t.Fatalf("acquire r2: %v", err)
	}
	if r2.Result != "granted" || r2.WaitToken == "" || r2.SlotToken == "" {
		t.Fatalf("expected r2 granted, got: %#v", r2)
	}
	if err := FairQueueActivate(r2.SlotToken); err != nil {
		t.Fatalf("activate r2: %v", err)
	}

	// IP3 should be PENDING (guest total limit reached, even though IP3 has 0 active)
	r3, err := FairQueueAcquire(guest, "3.3.3.3", "")
	if err != nil {
		t.Fatalf("acquire r3: %v", err)
	}
	if r3.Result != "pending" {
		t.Fatalf("expected r3 pending due to guest total limit, got: %#v", r3)
	}

	// Release one slot, IP3 should get granted
	if err := FairQueueRelease(r1.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	poll3, err := FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3: %v", err)
	}
	if poll3.Result != "granted" || poll3.WaitToken == "" || poll3.SlotToken == "" {
		t.Fatalf("expected r3 granted after release, got: %#v", poll3)
	}
	if err := FairQueueActivate(poll3.SlotToken); err != nil {
		t.Fatalf("activate r3: %v", err)
	}

	// Cleanup
	if err := FairQueueRelease(r2.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release r2: %v", err)
	}
	waitForSlotRelease(t, r2.SlotToken)
	if err := FairQueueRelease(poll3.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release r3: %v", err)
	}
	waitForSlotRelease(t, poll3.SlotToken)
}

func TestFairQueueProvisionalGrantLifecycle(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 1,
		conf.IPDownloadConcurrency:    1,
	})

	guest := &model.User{ID: 1, Role: model.GUEST}
	res, err := FairQueueAcquire(guest, "1.2.3.4", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if res.Result != "granted" || res.WaitToken == "" || res.SlotToken == "" {
		t.Fatalf("expected provisional grant with wait+slot token, got %#v", res)
	}

	if err := FairQueueActivate(res.SlotToken); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if abandoned := FairQueueAbandon(res.WaitToken); abandoned {
		t.Fatalf("active slot must not be released by wait-token abandon")
	}

	second, err := FairQueueAcquire(guest, "1.2.3.4", "")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second.Result != "pending" {
		t.Fatalf("expected second acquire to wait while first slot is active, got %#v", second)
	}

	if err := FairQueueRelease(res.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestFairQueueAbandonReleasesProvisionalCapacity(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 1,
		conf.IPDownloadConcurrency:    1,
	})

	guest := &model.User{ID: 1, Role: model.GUEST}
	first, err := FairQueueAcquire(guest, "2.2.2.2", "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.WaitToken == "" || first.SlotToken == "" {
		t.Fatalf("expected provisional grant, got %#v", first)
	}

	if !FairQueueAbandon(first.WaitToken) {
		t.Fatalf("expected abandon to consume provisional waiter")
	}

	second, err := FairQueueAcquire(guest, "2.2.2.2", "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "granted" {
		t.Fatalf("expected slot to be available after abandon, got %#v", second)
	}
}

func TestFairQueueClientAbortReleaseBypassesHold(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.UserDefaultDownloadConcurrency: 1,
	})
	conf.Conf.FairQueue.MinSlotHoldMs = 5000

	user := &model.User{ID: 42, Role: model.GENERAL}
	res, err := FairQueueAcquire(user, "", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := FairQueueActivate(res.SlotToken); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := FairQueueRelease(res.SlotToken, time.Now(), ReleaseReasonClientAbort); err != nil {
		t.Fatalf("release: %v", err)
	}

	waitForSlotRelease(t, res.SlotToken)
}

func TestFairQueueProvisionalDeadlineAutoReleasesCapacity(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 1,
		conf.IPDownloadConcurrency:    1,
	})

	guest := &model.User{ID: 1, Role: model.GUEST}
	first, err := FairQueueAcquire(guest, "3.3.3.3", "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.Result != "granted" || first.WaitToken == "" || first.SlotToken == "" {
		t.Fatalf("expected provisional grant, got %#v", first)
	}

	time.Sleep(1100 * time.Millisecond)
	waitForSlotRelease(t, first.SlotToken)

	second, err := FairQueueAcquire(guest, "3.3.3.3", "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Result != "granted" {
		t.Fatalf("expected capacity to be released after provisional deadline, got %#v", second)
	}
}

func TestFairQueueStreamEndReleaseObeysHoldAndSmooth(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.UserDefaultDownloadConcurrency: 2,
	})
	conf.Conf.FairQueue.MinSlotHoldMs = 40
	smooth := int64(60)
	conf.Conf.FairQueue.SmoothReleaseIntervalMs = &smooth

	user := &model.User{ID: 7, Role: model.GENERAL}
	first, err := FairQueueAcquire(user, "", "")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	second, err := FairQueueAcquire(user, "", "")
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if err := FairQueueActivate(first.SlotToken); err != nil {
		t.Fatalf("activate first: %v", err)
	}
	if err := FairQueueActivate(second.SlotToken); err != nil {
		t.Fatalf("activate second: %v", err)
	}

	hitAt := time.Now()
	if err := FairQueueRelease(first.SlotToken, hitAt, ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if err := FairQueueRelease(second.SlotToken, hitAt, ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release second: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if !fairQueueHasSlot(first.SlotToken) || !fairQueueHasSlot(second.SlotToken) {
		t.Fatalf("slots released before hold elapsed")
	}

	time.Sleep(35 * time.Millisecond)
	if fairQueueHasSlot(first.SlotToken) {
		t.Fatalf("first slot should be releasable after hold")
	}
	if !fairQueueHasSlot(second.SlotToken) {
		t.Fatalf("second slot should still wait for smoothing interval")
	}

	waitForSlotRelease(t, second.SlotToken)
}

func TestFairQueueActivateRejectsExpiredOrUnknownSlot(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.UserDefaultDownloadConcurrency: 1,
	})

	user := &model.User{ID: 9, Role: model.GENERAL}
	res, err := FairQueueAcquire(user, "", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if res.Result != "granted" || res.SlotToken == "" {
		t.Fatalf("expected provisional grant, got %#v", res)
	}

	fairQueue.mu.Lock()
	waitToken := fairQueue.slotToWaiter[res.SlotToken]
	waiter := fairQueue.waiters[waitToken]
	if waiter == nil {
		fairQueue.mu.Unlock()
		t.Fatalf("expected waiter for slot %s", res.SlotToken)
	}
	waiter.ProvisionalDeadline = time.Now().Add(-time.Millisecond)
	fairQueue.mu.Unlock()

	if err := FairQueueActivate(res.SlotToken); !errors.Is(err, errs.ObjectNotFound) {
		t.Fatalf("expected expired slot activate to fail with ObjectNotFound, got %v", err)
	}
	if err := FairQueueActivate("missing-slot"); !errors.Is(err, errs.ObjectNotFound) {
		t.Fatalf("expected unknown slot activate to fail with ObjectNotFound, got %v", err)
	}
}
