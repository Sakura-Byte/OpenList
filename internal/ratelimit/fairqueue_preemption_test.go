package ratelimit

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestFairQueueSamePathWaitsForRelease_User(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.UserDefaultDownloadConcurrency: 2,
	})

	user := &model.User{ID: 100, Role: model.GENERAL}
	pathA := "/video.mp4"
	pathB := "/other.zip"

	// 1. Acquire Path A (Slot 1)
	r1, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r1 failed: %v", err)
	}
	if r1.Result != "granted" {
		t.Fatalf("r1 not granted: %v", r1)
	}
	if r1.WaitToken == "" || r1.SlotToken == "" {
		t.Fatalf("r1 missing wait/slot token: %v", r1)
	}
	if err := FairQueueActivate(r1.SlotToken); err != nil {
		t.Fatalf("activate r1: %v", err)
	}

	// 2. Acquire Path B (Slot 2)
	r2, err := FairQueueAcquire(user, "", pathB)
	if err != nil {
		t.Fatalf("r2 failed: %v", err)
	}
	if r2.Result != "granted" {
		t.Fatalf("r2 not granted: %v", r2)
	}
	if r2.WaitToken == "" || r2.SlotToken == "" {
		t.Fatalf("r2 missing wait/slot token: %v", r2)
	}
	if err := FairQueueActivate(r2.SlotToken); err != nil {
		t.Fatalf("activate r2: %v", err)
	}

	// 3. Acquire Path A again (Slot 3) -> Should wait for the existing A slot to release
	r3, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "pending" || r3.WaitToken == "" {
		t.Fatalf("r3 should be pending until the existing A slot releases: %v", r3)
	}

	pollR3, err := FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3 while A is still active: %v", err)
	}
	if pollR3.Result != "pending" {
		t.Fatalf("r3 should remain pending while the existing A slot is active: %v", pollR3)
	}

	if err := FairQueueRelease(r1.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	pollR3, err = FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3 after releasing A: %v", err)
	}
	if pollR3.Result != "granted" || pollR3.SlotToken == "" {
		t.Fatalf("r3 should be granted after the existing A slot releases: %v", pollR3)
	}
	if pollR3.WaitToken == "" {
		t.Fatalf("poll r3 missing wait token: %v", pollR3)
	}
	if err := FairQueueActivate(pollR3.SlotToken); err != nil {
		t.Fatalf("activate r3: %v", err)
	}

	// Cleanup
	FairQueueRelease(r2.SlotToken, time.Now(), ReleaseReasonStreamEnd)
	FairQueueRelease(pollR3.SlotToken, time.Now(), ReleaseReasonStreamEnd)
}

func TestFairQueueSamePathWaitsForRelease_Guest(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.GuestDownloadConcurrency: 2,
		conf.IPDownloadConcurrency:    2,
	})

	guest := &model.User{ID: 102, Role: model.GUEST}
	ip := "9.8.7.6"
	pathA := "/video.mp4"
	pathB := "/other.zip"

	r1, err := FairQueueAcquire(guest, ip, pathA)
	if err != nil {
		t.Fatalf("r1 failed: %v", err)
	}
	if r1.Result != "granted" {
		t.Fatalf("r1 not granted: %v", r1)
	}
	if r1.WaitToken == "" || r1.SlotToken == "" {
		t.Fatalf("r1 missing wait/slot token: %v", r1)
	}
	if err := FairQueueActivate(r1.SlotToken); err != nil {
		t.Fatalf("activate r1: %v", err)
	}

	r2, err := FairQueueAcquire(guest, ip, pathB)
	if err != nil {
		t.Fatalf("r2 failed: %v", err)
	}
	if r2.Result != "granted" {
		t.Fatalf("r2 not granted: %v", r2)
	}
	if r2.WaitToken == "" || r2.SlotToken == "" {
		t.Fatalf("r2 missing wait/slot token: %v", r2)
	}
	if err := FairQueueActivate(r2.SlotToken); err != nil {
		t.Fatalf("activate r2: %v", err)
	}

	r3, err := FairQueueAcquire(guest, ip, pathA)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "pending" || r3.WaitToken == "" {
		t.Fatalf("r3 should be pending until the existing A slot releases: %v", r3)
	}

	pollR3, err := FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3 while A is still active: %v", err)
	}
	if pollR3.Result != "pending" {
		t.Fatalf("r3 should remain pending while the existing A slot is active: %v", pollR3)
	}

	if err := FairQueueRelease(r1.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	pollR3, err = FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3 after releasing A: %v", err)
	}
	if pollR3.Result != "granted" || pollR3.SlotToken == "" {
		t.Fatalf("r3 should be granted after the existing A slot releases: %v", pollR3)
	}
	if pollR3.WaitToken == "" {
		t.Fatalf("poll r3 missing wait token: %v", pollR3)
	}
	if err := FairQueueActivate(pollR3.SlotToken); err != nil {
		t.Fatalf("activate r3: %v", err)
	}

	FairQueueRelease(r2.SlotToken, time.Now(), ReleaseReasonStreamEnd)
	FairQueueRelease(pollR3.SlotToken, time.Now(), ReleaseReasonStreamEnd)
}

func TestFairQueueDifferentPathWaitsForRelease_User(t *testing.T) {
	setupFairQueueTest(t, map[string]int{
		conf.UserDefaultDownloadConcurrency: 2,
	})

	user := &model.User{ID: 101, Role: model.GENERAL}
	pathA := "/video.mp4"
	pathB := "/new.zip"

	// 1. Acquire Path A (Slot 1)
	r1, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r1 failed: %v", err)
	}
	if r1.WaitToken == "" || r1.SlotToken == "" {
		t.Fatalf("r1 missing wait/slot token: %v", r1)
	}
	if err := FairQueueActivate(r1.SlotToken); err != nil {
		t.Fatalf("activate r1: %v", err)
	}

	// 2. Acquire Path A (Slot 2)
	r2, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r2 failed: %v", err)
	}
	if r2.WaitToken == "" || r2.SlotToken == "" {
		t.Fatalf("r2 missing wait/slot token: %v", r2)
	}
	if err := FairQueueActivate(r2.SlotToken); err != nil {
		t.Fatalf("activate r2: %v", err)
	}

	// 3. Acquire Path B (Slot 3) -> Should wait because all paths share the same pool
	r3, err := FairQueueAcquire(user, "", pathB)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "pending" || r3.WaitToken == "" {
		t.Fatalf("r3 should be pending until an existing slot releases: %v", r3)
	}

	pollR3, err := FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3 while slots are still active: %v", err)
	}
	if pollR3.Result != "pending" {
		t.Fatalf("r3 should remain pending while the pool is full: %v", pollR3)
	}

	if err := FairQueueRelease(r1.SlotToken, time.Now(), ReleaseReasonStreamEnd); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	pollR3, err = FairQueuePoll(r3.WaitToken)
	if err != nil {
		t.Fatalf("poll r3 after releasing a slot: %v", err)
	}
	if pollR3.Result != "granted" || pollR3.SlotToken == "" {
		t.Fatalf("r3 should be granted after a slot releases: %v", pollR3)
	}
	if pollR3.WaitToken == "" {
		t.Fatalf("poll r3 missing wait token: %v", pollR3)
	}
	if err := FairQueueActivate(pollR3.SlotToken); err != nil {
		t.Fatalf("activate r3: %v", err)
	}

	// Cleanup
	FairQueueRelease(r2.SlotToken, time.Now(), ReleaseReasonStreamEnd)
	FairQueueRelease(pollR3.SlotToken, time.Now(), ReleaseReasonStreamEnd)
}
