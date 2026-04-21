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

	// 2. Acquire Path B (Slot 2)
	r2, err := FairQueueAcquire(user, "", pathB)
	if err != nil {
		t.Fatalf("r2 failed: %v", err)
	}
	if r2.Result != "granted" {
		t.Fatalf("r2 not granted: %v", r2)
	}

	// 3. Acquire Path A again (Slot 3) -> Should wait for the existing A slot to release
	r3, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "pending" || r3.QueryToken == "" {
		t.Fatalf("r3 should be pending until the existing A slot releases: %v", r3)
	}

	pollR3, err := FairQueuePoll(r3.QueryToken)
	if err != nil {
		t.Fatalf("poll r3 while A is still active: %v", err)
	}
	if pollR3.Result != "pending" {
		t.Fatalf("r3 should remain pending while the existing A slot is active: %v", pollR3)
	}

	if err := FairQueueRelease(r1.SlotToken, time.Now()); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	pollR3, err = FairQueuePoll(r3.QueryToken)
	if err != nil {
		t.Fatalf("poll r3 after releasing A: %v", err)
	}
	if pollR3.Result != "granted" || pollR3.SlotToken == "" {
		t.Fatalf("r3 should be granted after the existing A slot releases: %v", pollR3)
	}

	// Cleanup
	FairQueueRelease(r2.SlotToken, time.Now())
	FairQueueRelease(pollR3.SlotToken, time.Now())
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

	r2, err := FairQueueAcquire(guest, ip, pathB)
	if err != nil {
		t.Fatalf("r2 failed: %v", err)
	}
	if r2.Result != "granted" {
		t.Fatalf("r2 not granted: %v", r2)
	}

	r3, err := FairQueueAcquire(guest, ip, pathA)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "pending" || r3.QueryToken == "" {
		t.Fatalf("r3 should be pending until the existing A slot releases: %v", r3)
	}

	pollR3, err := FairQueuePoll(r3.QueryToken)
	if err != nil {
		t.Fatalf("poll r3 while A is still active: %v", err)
	}
	if pollR3.Result != "pending" {
		t.Fatalf("r3 should remain pending while the existing A slot is active: %v", pollR3)
	}

	if err := FairQueueRelease(r1.SlotToken, time.Now()); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	pollR3, err = FairQueuePoll(r3.QueryToken)
	if err != nil {
		t.Fatalf("poll r3 after releasing A: %v", err)
	}
	if pollR3.Result != "granted" || pollR3.SlotToken == "" {
		t.Fatalf("r3 should be granted after the existing A slot releases: %v", pollR3)
	}

	FairQueueRelease(r2.SlotToken, time.Now())
	FairQueueRelease(pollR3.SlotToken, time.Now())
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

	// 2. Acquire Path A (Slot 2)
	r2, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r2 failed: %v", err)
	}

	// 3. Acquire Path B (Slot 3) -> Should wait because all paths share the same pool
	r3, err := FairQueueAcquire(user, "", pathB)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "pending" || r3.QueryToken == "" {
		t.Fatalf("r3 should be pending until an existing slot releases: %v", r3)
	}

	pollR3, err := FairQueuePoll(r3.QueryToken)
	if err != nil {
		t.Fatalf("poll r3 while slots are still active: %v", err)
	}
	if pollR3.Result != "pending" {
		t.Fatalf("r3 should remain pending while the pool is full: %v", pollR3)
	}

	if err := FairQueueRelease(r1.SlotToken, time.Now()); err != nil {
		t.Fatalf("release r1: %v", err)
	}
	waitForSlotRelease(t, r1.SlotToken)

	pollR3, err = FairQueuePoll(r3.QueryToken)
	if err != nil {
		t.Fatalf("poll r3 after releasing a slot: %v", err)
	}
	if pollR3.Result != "granted" || pollR3.SlotToken == "" {
		t.Fatalf("r3 should be granted after a slot releases: %v", pollR3)
	}

	// Cleanup
	FairQueueRelease(r2.SlotToken, time.Now())
	FairQueueRelease(pollR3.SlotToken, time.Now())
}
