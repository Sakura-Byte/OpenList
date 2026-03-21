package updatesite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

func TestScanPublicPathPageCursorLifecycle(t *testing.T) {
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		if rawPath != "/" {
			t.Fatalf("unexpected rawPath: %s", rawPath)
		}
		if maxDepth != -1 {
			t.Fatalf("unexpected maxDepth: %d", maxDepth)
		}
		batches := []driver.UpdateSiteChunk{
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/3D", ParentPath: "/", Name: "3D", IsDir: true}, {VisiblePath: "/ASMR", ParentPath: "/", Name: "ASMR", IsDir: true}}},
			{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/3D/Alpha", ParentPath: "/3D", Name: "Alpha"}}},
		}
		for _, batch := range batches {
			if err := onChunk(batch); err != nil {
				return err
			}
		}
		return nil
	})

	first, err := ScanPublicPathPage(context.Background(), ScanPageRequest{Path: "/", MaxDepth: -1, IncludeThumb: true, ChunkLimit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done {
		t.Fatalf("expected more pages")
	}
	if first.Cursor == "" {
		t.Fatalf("expected cursor")
	}
	if first.Stats.PageCount != 1 || first.Stats.DirCount != 2 || first.Stats.EntryCount != 3 {
		t.Fatalf("unexpected first stats: %+v", first.Stats)
	}
	if first.Stats.PendingEntries != 2 {
		t.Fatalf("expected 2 pending entries after first page, got %d", first.Stats.PendingEntries)
	}
	if len(first.Entries) != 1 || first.Entries[0].VisiblePath != "/3D" {
		t.Fatalf("unexpected first entries: %+v", first.Entries)
	}

	second, err := ScanPublicPathPage(context.Background(), ScanPageRequest{Path: "/", MaxDepth: -1, IncludeThumb: true, ChunkLimit: 2, Cursor: first.Cursor})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if !second.Done {
		t.Fatalf("expected completion on second page")
	}
	if second.Stats.PageCount != 2 || second.Stats.PendingEntries != 0 {
		t.Fatalf("unexpected second stats: %+v", second.Stats)
	}
	if len(second.Entries) != 2 {
		t.Fatalf("expected 2 entries on second page, got %d", len(second.Entries))
	}
	if second.Entries[0].VisiblePath != "/ASMR" || second.Entries[1].VisiblePath != "/3D/Alpha" {
		t.Fatalf("unexpected second entries: %+v", second.Entries)
	}
}

func TestScanPublicPathPageInvalidCursor(t *testing.T) {
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		return nil
	})
	if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{Path: "/", MaxDepth: -1, IncludeThumb: true, ChunkLimit: 1, Cursor: "missing"}); !errors.Is(err, ErrScanCursorExpired) {
		t.Fatalf("expected ErrScanCursorExpired, got %v", err)
	}
}

func TestScanPublicPathPageCursorMismatch(t *testing.T) {
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		if err := onChunk(driver.UpdateSiteChunk{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/child", ParentPath: "/", Name: "child", IsDir: true}}}); err != nil {
			return err
		}
		return onChunk(driver.UpdateSiteChunk{Entries: []driver.UpdateSiteEntry{{VisiblePath: "/child/file", ParentPath: "/child", Name: "file"}}})
	})
	first, err := ScanPublicPathPage(context.Background(), ScanPageRequest{Path: "/", MaxDepth: -1, IncludeThumb: true, ChunkLimit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{Path: "/other", MaxDepth: -1, IncludeThumb: true, ChunkLimit: 1, Cursor: first.Cursor}); !errors.Is(err, ErrScanCursorMismatch) {
		t.Fatalf("expected ErrScanCursorMismatch, got %v", err)
	}
}

func TestScanPublicPathPageUsesBoundedBuffer(t *testing.T) {
	started := make(chan struct{})
	released := make(chan struct{})
	var produced atomic.Int64
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		close(started)
		for i := 0; i < defaultChunkBufferSize+5; i++ {
			if err := onChunk(driver.UpdateSiteChunk{
				Entries: []driver.UpdateSiteEntry{{VisiblePath: "/x", ParentPath: "/", Name: "x"}},
			}); err != nil {
				return err
			}
			produced.Add(1)
			if i == defaultChunkBufferSize {
				<-released
			}
		}
		return nil
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
			Path:         "/",
			MaxDepth:     -1,
			IncludeThumb: false,
			ChunkLimit:   1,
		}); err != nil {
			t.Errorf("first scan page: %v", err)
		}
	}()

	<-started
	time.Sleep(100 * time.Millisecond)
	if got := produced.Load(); got > defaultChunkBufferSize+1 {
		t.Fatalf("expected bounded producer progress, got %d", got)
	}
	close(released)
	<-firstDone
}

func withFakeScanWalker(t *testing.T, walk func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error) {
	t.Helper()

	resetScanSessions()
	prevWalker := walkPublicScan
	walkPublicScan = walk
	t.Cleanup(func() {
		walkPublicScan = prevWalker
		resetScanSessions()
	})
}

func resetScanSessions() {
	scanSessions.mu.Lock()
	sessions := make([]*scanSession, 0, len(scanSessions.sessions))
	for _, session := range scanSessions.sessions {
		sessions = append(sessions, session)
	}
	scanSessions.sessions = make(map[string]*scanSession)
	scanSessions.mu.Unlock()
	for _, session := range sessions {
		session.close()
	}
}
