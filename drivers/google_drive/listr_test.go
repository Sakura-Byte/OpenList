package google_drive

import (
	"testing"
)

func TestBuildListRParentsQuery(t *testing.T) {
	got := buildListRParentsQuery([]string{"a", "b"})
	want := "('a' in parents or 'b' in parents) and trashed = false"
	if got != want {
		t.Fatalf("unexpected query: got=%q want=%q", got, want)
	}
}

func TestMapGoogleDriveListRFiles(t *testing.T) {
	files := []File{
		{Id: "dir1", Name: "docs", MimeType: "application/vnd.google-apps.folder", Parents: []string{"p1"}},
		{Id: "f1", Name: "a.txt", MimeType: "text/plain", Parents: []string{"p1", "p2"}},
		{Id: "f2", Name: "b.txt", MimeType: "text/plain"},
	}
	parentIDs := []string{"p1"}
	taskByParentID := map[string]googleDriveListRTask{
		"p1": {id: "p1", path: "/", depth: 0},
		"p2": {id: "p2", path: "/other", depth: 0},
	}
	visited := map[string]struct{}{}
	entriesByParent, next := mapGoogleDriveListRFiles(files, parentIDs, taskByParentID, -1, visited)

	if len(entriesByParent["/"]) != 3 {
		t.Fatalf("expected 3 entries for root parent, got %d", len(entriesByParent["/"]))
	}
	if len(entriesByParent["/other"]) != 1 {
		t.Fatalf("expected 1 entry for /other, got %d", len(entriesByParent["/other"]))
	}
	if len(next) != 1 {
		t.Fatalf("expected 1 next dir task, got %d", len(next))
	}
	if next[0].id != "dir1" || next[0].path != "/docs" || next[0].depth != 1 {
		t.Fatalf("unexpected next task: %+v", next[0])
	}
}

func TestMapGoogleDriveListRFiles_MaxDepthStopsDirExpansion(t *testing.T) {
	files := []File{{Id: "dir1", Name: "docs", MimeType: "application/vnd.google-apps.folder", Parents: []string{"p1"}}}
	parentIDs := []string{"p1"}
	taskByParentID := map[string]googleDriveListRTask{"p1": {id: "p1", path: "/", depth: 0}}
	visited := map[string]struct{}{}
	_, next := mapGoogleDriveListRFiles(files, parentIDs, taskByParentID, 1, visited)
	if len(next) != 0 {
		t.Fatalf("expected no next dir task when maxDepth reached, got %d", len(next))
	}
}

func TestGoogleDriveListRGroupingState(t *testing.T) {
	d := &GoogleDrive{}
	if got := d.getListRGrouping(); got != listRGrouping {
		t.Fatalf("expected default grouping %d, got %d", listRGrouping, got)
	}
	if !d.disableListRGrouping() {
		t.Fatalf("expected disableListRGrouping to report changed state")
	}
	if got := d.getListRGrouping(); got != 1 {
		t.Fatalf("expected grouping to be 1, got %d", got)
	}
	d.markListREmpties([]string{"a", "b"})
	if d.maybeReenableListRGrouping("a") {
		t.Fatalf("expected not re-enable when empties still remain")
	}
	if !d.maybeReenableListRGrouping("b") {
		t.Fatalf("expected re-enable when empties are cleared")
	}
	if got := d.getListRGrouping(); got != listRGrouping {
		t.Fatalf("expected grouping restored to %d, got %d", listRGrouping, got)
	}
}
