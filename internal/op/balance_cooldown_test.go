package op_test

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func createBalancedStorages(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if _, err := op.CreateStorage(t.Context(), model.Storage{
			Driver:    "Local",
			MountPath: path,
			Addition:  `{"root_folder_path":"."}`,
		}); err != nil {
			t.Fatalf("create storage %s: %v", path, err)
		}
	}
}

func TestBalancedStorageSkipsCoolingStorage(t *testing.T) {
	setupStorageTestEnv(t)
	cold := "/cool-a.balance1"
	createBalancedStorages(t, "/cool-a", cold, "/cool-a.balance2")
	op.CoolBalancedStorage(cold, time.Minute)
	seen := map[string]int{}
	for range 30 {
		seen[op.GetBalancedStorage("/cool-a/f").GetStorage().MountPath]++
	}
	if seen[cold] != 0 || len(seen) != 2 {
		t.Fatalf("cooling storage must be skipped and the others used, got %v", seen)
	}
}

func TestBalancedStorageFallsBackWhenAllCooling(t *testing.T) {
	setupStorageTestEnv(t)
	paths := []string{"/cool-b", "/cool-b.balance1"}
	createBalancedStorages(t, paths...)
	for _, path := range paths {
		op.CoolBalancedStorage(path, time.Minute)
	}
	seen := map[string]int{}
	for range 10 {
		seen[op.GetBalancedStorage("/cool-b/f").GetStorage().MountPath]++
	}
	if len(seen) != 2 {
		t.Fatalf("all cooling must fall back to round robin, got %v", seen)
	}
}

func TestCooldownExpires(t *testing.T) {
	setupStorageTestEnv(t)
	paths := []string{"/cool-c", "/cool-c.balance1"}
	createBalancedStorages(t, paths...)
	op.CoolBalancedStorage(paths[1], time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	seen := map[string]int{}
	for range 10 {
		seen[op.GetBalancedStorage("/cool-c/f").GetStorage().MountPath]++
	}
	if seen[paths[1]] == 0 {
		t.Fatalf("storage must return after its cooldown, got %v", seen)
	}
}

func TestPeekDoesNotAdvanceBalance(t *testing.T) {
	setupStorageTestEnv(t)
	createBalancedStorages(t, "/cool-d", "/cool-d.balance1")
	first := op.GetBalancedStorage("/cool-d/f").GetStorage().MountPath
	for range 3 {
		if _, _, err := op.PeekStorageAndActualPath("/cool-d/f"); err != nil {
			t.Fatal(err)
		}
	}
	if next := op.GetBalancedStorage("/cool-d/f").GetStorage().MountPath; next == first {
		t.Fatalf("peek must not move the counter: both calls returned %s", next)
	}
}
