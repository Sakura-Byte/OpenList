package ratelimit

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestFairQueuePathPreemption_SamePath(t *testing.T) {
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

	// 3. Acquire Path A again (Slot 3) -> Should preempt r1 (oldest A)
	r3, err := FairQueueAcquire(user, "", pathA)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "granted" {
		t.Fatalf("r3 not granted (preemption failed): %v", r3)
	}

	// Verify r1 is gone
	waitForSlotRelease(t, r1.SlotToken)

	// Cleanup
	FairQueueRelease(r2.SlotToken, time.Now())
	FairQueueRelease(r3.SlotToken, time.Now())
}

func TestFairQueuePathPreemption_ExcessPath(t *testing.T) {
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

	// 3. Acquire Path B (Slot 3) -> Should preempt r1 (oldest excess from A)
	r3, err := FairQueueAcquire(user, "", pathB)
	if err != nil {
		t.Fatalf("r3 failed: %v", err)
	}
	if r3.Result != "granted" {
		t.Fatalf("r3 not granted (excess preemption failed): %v", r3)
	}

	// Verify r1 is gone
	waitForSlotRelease(t, r1.SlotToken)

	// Cleanup
	FairQueueRelease(r2.SlotToken, time.Now())
	FairQueueRelease(r3.SlotToken, time.Now())
}
