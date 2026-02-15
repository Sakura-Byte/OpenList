package onedrive

import "testing"

func TestListRPathDepth(t *testing.T) {
	if got := listRPathDepth("/", "/a/b"); got != 2 {
		t.Fatalf("expected depth 2, got %d", got)
	}
	if got := listRPathDepth("/root", "/root/a"); got != 1 {
		t.Fatalf("expected depth 1, got %d", got)
	}
	if got := listRPathDepth("/root", "/root"); got != 0 {
		t.Fatalf("expected depth 0, got %d", got)
	}
}

func TestResolveOnedriveListRItems(t *testing.T) {
	fileType := &struct {
		MimeType string `json:"mimeType"`
	}{MimeType: "text/plain"}
	items := []File{
		{Id: "d1", Name: "docs", ParentReference: struct {
			DriveId string `json:"driveId"`
			Id      string `json:"id"`
			Path    string `json:"path"`
		}{Id: "root"}},
		{Id: "f1", Name: "a.txt", File: fileType, ParentReference: struct {
			DriveId string `json:"driveId"`
			Id      string `json:"id"`
			Path    string `json:"path"`
		}{Id: "d1"}},
		{Id: "f2", Name: "b.txt", File: fileType, ParentReference: struct {
			DriveId string `json:"driveId"`
			Id      string `json:"id"`
			Path    string `json:"path"`
		}{Id: "root"}},
	}
	res, shared := resolveOnedriveListRItems("/", "root", items, -1)
	if len(res["/"]) != 2 {
		t.Fatalf("expected 2 root entries, got %d", len(res["/"]))
	}
	if len(res["/docs"]) != 1 {
		t.Fatalf("expected 1 /docs entry, got %d", len(res["/docs"]))
	}
	if len(shared) != 0 {
		t.Fatalf("did not expect shared dirs, got %d", len(shared))
	}
}

func TestResolveOnedriveListRItems_MaxDepth(t *testing.T) {
	fileType := &struct {
		MimeType string `json:"mimeType"`
	}{MimeType: "text/plain"}
	items := []File{
		{Id: "d1", Name: "docs", ParentReference: struct {
			DriveId string `json:"driveId"`
			Id      string `json:"id"`
			Path    string `json:"path"`
		}{Id: "root"}},
		{Id: "f1", Name: "a.txt", File: fileType, ParentReference: struct {
			DriveId string `json:"driveId"`
			Id      string `json:"id"`
			Path    string `json:"path"`
		}{Id: "d1"}},
	}
	res, shared := resolveOnedriveListRItems("/", "root", items, 1)
	if len(res["/"]) != 1 {
		t.Fatalf("expected only root level entries at maxDepth=1, got %d", len(res["/"]))
	}
	if _, ok := res["/docs"]; ok {
		t.Fatalf("did not expect /docs entries at maxDepth=1")
	}
	if len(shared) != 0 {
		t.Fatalf("did not expect shared dirs, got %d", len(shared))
	}
}

func TestCompactOnedriveDeltaItemsFirstSeen_Dedup(t *testing.T) {
	deleted := &struct{}{}
	fileType := &struct {
		MimeType string `json:"mimeType"`
	}{MimeType: "text/plain"}
	items := []File{
		{Id: "a", Name: "old", File: fileType},
		{Id: "b", Name: "keep", File: fileType},
		{Id: "a", Name: "new", File: fileType},
		{Id: "b", Name: "keep", Deleted: deleted},
	}
	compacted := compactOnedriveDeltaItemsFirstSeen(items)
	if len(compacted) != 2 {
		t.Fatalf("expected 2 items after compact, got %d", len(compacted))
	}
	if compacted[0].Id != "a" || compacted[0].Name != "old" {
		t.Fatalf("unexpected compacted item: %+v", compacted[0])
	}
	if compacted[1].Id != "b" || compacted[1].Name != "keep" {
		t.Fatalf("unexpected compacted item: %+v", compacted[1])
	}
}

func TestResolveOnedriveListRItems_CollectSharedDirs(t *testing.T) {
	items := []File{
		{
			Id:   "d1",
			Name: "shared",
			RemoteItem: &struct {
				Folder *struct{} `json:"folder"`
			}{Folder: &struct{}{}},
			ParentReference: struct {
				DriveId string `json:"driveId"`
				Id      string `json:"id"`
				Path    string `json:"path"`
			}{Id: "root"},
		},
	}
	res, shared := resolveOnedriveListRItems("/", "root", items, -1)
	if len(res["/"]) != 1 {
		t.Fatalf("expected root entries to include shared dir, got %d", len(res["/"]))
	}
	if len(shared) != 1 {
		t.Fatalf("expected 1 shared dir, got %d", len(shared))
	}
	if shared[0].GetPath() != "/shared" {
		t.Fatalf("unexpected shared dir path: %s", shared[0].GetPath())
	}
}

func TestParseOnedriveParentPath(t *testing.T) {
	if got, ok := parseOnedriveParentPath("/drive/root:/A/B"); !ok || got != "/A/B" {
		t.Fatalf("unexpected parse result: ok=%v got=%q", ok, got)
	}
	if got, ok := parseOnedriveParentPath("/drives/xxx/root"); !ok || got != "/" {
		t.Fatalf("unexpected root parse result: ok=%v got=%q", ok, got)
	}
	if _, ok := parseOnedriveParentPath("invalid"); ok {
		t.Fatalf("expected invalid path parse to fail")
	}
}
