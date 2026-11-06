package gdlimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setup(t *testing.T, limit int) *time.Time {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	dB, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := dB.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	prevConf := conf.Conf
	conf.Conf = conf.DefaultConfig("data")
	conf.Conf.JwtSecret = "test-secret"
	db.Init(dB)
	for key, value := range map[string]int{conf.GDDownloadIPDailyLimit: limit, conf.GDDownloadSessionMinutes: 10} {
		if err := op.SaveSettingItem(&model.SettingItem{Key: key, Value: fmt.Sprint(value), Type: conf.TypeNumber}); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 11, 7, 12, 30, 0, 0, time.UTC)
	prevNow, prevResolve := nowFunc, resolveFile
	nowFunc = func() time.Time { return now }
	resolveFile = func(_ context.Context, path string) (bool, int64, error) {
		return path != "/union/3dlocal/a.mp4", 100, nil
	}
	mu.Lock()
	sessions = map[string]time.Time{}
	lastSweep = now
	mu.Unlock()
	t.Cleanup(func() {
		nowFunc, resolveFile = prevNow, prevResolve
		conf.Conf = prevConf
		_ = sqlDB.Close()
	})
	return &now
}

func TestRejectsAfterLimitWithoutCountingRejections(t *testing.T) {
	setup(t, 3)
	ctx := context.Background()
	for i := range 3 {
		if d := Admit(ctx, "1.2.3.4", fmt.Sprintf("/Galgames/%d", i)); !d.Allowed {
			t.Fatalf("download %d rejected", i)
		}
	}
	for range 2 {
		d := Admit(ctx, "1.2.3.4", "/Galgames/extra")
		if d.Allowed || d.Limit != 3 || d.Used != 3 {
			t.Fatalf("expected rejection at 3/3, got %+v", d)
		}
	}
	if !Admit(ctx, "5.6.7.8", "/Galgames/extra").Allowed {
		t.Fatal("another IP must have its own quota")
	}
}

func TestSessionWindowCountsOnceAndSurvivesTheLimit(t *testing.T) {
	now := setup(t, 2)
	ctx := context.Background()
	for range 5 {
		Admit(ctx, "1.2.3.4", "/Galgames/a")
	}
	if q := CurrentQuota(ctx, "1.2.3.4", "/Galgames/a"); q == nil || q.Used != 1 {
		t.Fatalf("requests in the window must count once, got %+v", q)
	}
	Admit(ctx, "1.2.3.4", "/Galgames/b")
	*now = now.Add(9 * time.Minute)
	if !Admit(ctx, "1.2.3.4", "/Galgames/a").Allowed {
		t.Fatal("a download in progress must not be cut off at the limit")
	}
	if q := CurrentQuota(ctx, "1.2.3.4", "/Galgames/a"); q == nil || q.Blocked {
		t.Fatalf("file in its session must not be blocked, got %+v", q)
	}
	if q := CurrentQuota(ctx, "1.2.3.4", "/Galgames/c"); q == nil || !q.Blocked || q.Remaining != 0 {
		t.Fatalf("new file at the limit must be blocked, got %+v", q)
	}
	*now = now.Add(11 * time.Minute)
	if Admit(ctx, "1.2.3.4", "/Galgames/a").Allowed {
		t.Fatal("downloading again after the window is a new download")
	}
}

func TestNonGoogleDriveFilesAreNotCounted(t *testing.T) {
	setup(t, 1)
	ctx := context.Background()
	for range 3 {
		if !Admit(ctx, "1.2.3.4", "/union/3dlocal/a.mp4").Allowed {
			t.Fatal("local file must not be limited")
		}
	}
	if q := CurrentQuota(ctx, "1.2.3.4", "/union/3dlocal/a.mp4"); q != nil {
		t.Fatalf("local file must have no quota, got %+v", q)
	}
	if q := CurrentQuota(ctx, "1.2.3.4", "/Galgames/a"); q == nil || q.Used != 0 {
		t.Fatalf("local downloads must not use the quota, got %+v", q)
	}
}

func TestWindowRollsAndResetTimeIsWhenBelowLimit(t *testing.T) {
	now := setup(t, 2)
	ctx := context.Background()
	start := *now
	Admit(ctx, "1.2.3.4", "/Galgames/a")
	*now = now.Add(2 * time.Hour)
	Admit(ctx, "1.2.3.4", "/Galgames/b")
	d := Admit(ctx, "1.2.3.4", "/Galgames/c")
	want := start.Truncate(time.Hour).Add(24 * time.Hour)
	if d.Allowed || !d.ResetAt.Equal(want) {
		t.Fatalf("expected rejection until %s, got %+v", want, d)
	}
	*now = want
	if !Admit(ctx, "1.2.3.4", "/Galgames/c").Allowed {
		t.Fatal("oldest bucket left the window, one download must be free again")
	}
}

func TestIPv6PrefixCountsAsOneIP(t *testing.T) {
	setup(t, 1)
	ctx := context.Background()
	Admit(ctx, "2001:db8:1:2::1", "/Galgames/a")
	if Admit(ctx, "2001:db8:1:2::ffff", "/Galgames/b").Allowed {
		t.Fatal("same /64 must share the quota")
	}
	if !Admit(ctx, "2001:db8:1:3::1", "/Galgames/b").Allowed {
		t.Fatal("another /64 has its own quota")
	}
}

func TestLimitZeroDisablesAndAuthorsAreRecorded(t *testing.T) {
	setup(t, 0)
	ctx := context.Background()
	for i := range 3 {
		if !Admit(ctx, "1.2.3.4", fmt.Sprintf("/3D/alice/%d.mp4", i)).Allowed {
			t.Fatal("limit 0 means off")
		}
	}
	Admit(ctx, "1.2.3.4", "/3D/bob/1.mp4")
	Admit(ctx, "1.2.3.4", "/3D/loose-file.mp4")
	Admit(ctx, "1.2.3.4", "/Galgames/x/1.bin")
	rows, err := TopAuthors(StatsArea, 7, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Author != "alice" || rows[0].Count != 3 || rows[0].Bytes != 300 || rows[1].Author != "bob" {
		t.Fatalf("unexpected report: %+v", rows)
	}
	if q := CurrentQuota(ctx, "1.2.3.4", "/Galgames/x/1.bin"); q != nil {
		t.Fatalf("limit 0 must report no quota, got %+v", q)
	}
}
