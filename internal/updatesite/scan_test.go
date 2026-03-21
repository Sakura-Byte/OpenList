package updatesite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestScanPublicPathPageCursorLifecycle(t *testing.T) {
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		if rawPath != "/" {
			t.Fatalf("unexpected rawPath: %s", rawPath)
		}
		if maxDepth != -1 {
			t.Fatalf("unexpected maxDepth: %d", maxDepth)
		}
		chunks := []driver.UpdateSiteChunk{
			{Parent: "/", Entries: []model.Obj{fakeObj("3D", true), fakeObj("ASMR", true)}, ParentDone: true},
			{Parent: "/3D", Entries: []model.Obj{fakeObj("Alpha", false)}, ParentDone: true},
			{Parent: "/ASMR", Entries: []model.Obj{}, ParentDone: true},
		}
		for _, chunk := range chunks {
			if err := onChunk(chunk); err != nil {
				return err
			}
		}
		return nil
	})

	first, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		ChunkLimit:   1,
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Done {
		t.Fatalf("expected more pages")
	}
	if first.Cursor == "" {
		t.Fatalf("expected cursor")
	}
	if first.Stats.PageCount != 1 || first.Stats.ChunkCount != 3 || first.Stats.DirCount != 3 || first.Stats.EntryCount != 3 {
		t.Fatalf("unexpected first stats: %+v", first.Stats)
	}
	if got := first.Stats.PendingChunks; got < 0 || got > 2 {
		t.Fatalf("unexpected pending chunk count: %d", got)
	}
	if got := first.Chunks[0].ParentPath; got != "/" {
		t.Fatalf("expected root chunk, got %s", got)
	}

	second, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		ChunkLimit:   1,
		Cursor:       first.Cursor,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.Done {
		t.Fatalf("expected third page")
	}
	if second.Stats.PageCount != 2 || second.Stats.ChunkCount != 3 || second.Stats.DirCount != 3 || second.Stats.EntryCount != 3 {
		t.Fatalf("unexpected second stats: %+v", second.Stats)
	}
	if got := second.Chunks[0].ParentPath; got != "/3D" {
		t.Fatalf("expected /3D chunk, got %s", got)
	}

	third, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		ChunkLimit:   1,
		Cursor:       second.Cursor,
	})
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	if !third.Done || third.Cursor != "" {
		t.Fatalf("expected done final page, got done=%v cursor=%q", third.Done, third.Cursor)
	}
	if third.Stats.PageCount != 3 || third.Stats.ChunkCount != 3 || third.Stats.DirCount != 3 || third.Stats.EntryCount != 3 || third.Stats.PendingChunks != 0 {
		t.Fatalf("unexpected third stats: %+v", third.Stats)
	}
	if got := third.Chunks[0].ParentPath; got != "/ASMR" {
		t.Fatalf("expected /ASMR chunk, got %s", got)
	}
}

func TestScanPublicPathPageInvalidCursor(t *testing.T) {
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		return nil
	})
	if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		ChunkLimit:   1,
		Cursor:       "missing",
	}); !errors.Is(err, ErrScanCursorExpired) {
		t.Fatalf("expected ErrScanCursorExpired, got %v", err)
	}
}

func TestScanPublicPathPageCursorMismatch(t *testing.T) {
	withFakeScanWalker(t, func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error {
		if err := onChunk(driver.UpdateSiteChunk{Parent: "/", ParentDone: true}); err != nil {
			return err
		}
		return onChunk(driver.UpdateSiteChunk{Parent: "/child", ParentDone: true})
	})
	first, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/",
		MaxDepth:     -1,
		IncludeThumb: true,
		ChunkLimit:   1,
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if _, err := ScanPublicPathPage(context.Background(), ScanPageRequest{
		Path:         "/other",
		MaxDepth:     -1,
		IncludeThumb: true,
		ChunkLimit:   1,
		Cursor:       first.Cursor,
	}); !errors.Is(err, ErrScanCursorMismatch) {
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
				Parent:     "/",
				Entries:    []model.Obj{fakeObj("x", false)},
				ParentDone: i == defaultChunkBufferSize+4,
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

func fakeObj(name string, isDir bool) model.Obj {
	return &model.Object{
		Name:     name,
		Path:     "/" + name,
		IsFolder: isDir,
	}
}
