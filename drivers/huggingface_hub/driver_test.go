package huggingface_hub

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func newTestDriver(serverURL string) *HuggingFaceHub {
	if conf.Conf == nil {
		conf.Conf = conf.DefaultConfig("")
	}
	return &HuggingFaceHub{
		Addition: Addition{
			RootPath: driver.RootPath{
				RootFolderPath: "/",
			},
			APIEndpoint: serverURL,
			RepoType:    "model",
			RepoID:      "owner/repo",
			Revision:    "main",
		},
	}
}

func TestInitSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/models/owner/repo/tree/main" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	d := newTestDriver(server.URL)
	if err := d.Init(context.Background()); err != nil {
		t.Fatalf("expected init success, got error: %v", err)
	}
}

func TestInitInvalidRepoType(t *testing.T) {
	d := newTestDriver("https://huggingface.co")
	d.RepoType = "invalid"
	if err := d.Init(context.Background()); err == nil {
		t.Fatal("expected invalid repo_type error")
	}
}

func TestInitInvalidRepoID(t *testing.T) {
	d := newTestDriver("https://huggingface.co")
	d.RepoID = "owner-only"
	if err := d.Init(context.Background()); err == nil {
		t.Fatal("expected invalid repo_id error")
	}
}

func TestInitInvalidRevision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Error-Code", "RevisionNotFound")
		w.Header().Set("X-Error-Message", "Invalid rev id: bad-rev")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"Invalid rev id: bad-rev"}`))
	}))
	defer server.Close()

	d := newTestDriver(server.URL)
	d.Revision = "bad-rev"
	err := d.Init(context.Background())
	if err == nil {
		t.Fatal("expected invalid revision error")
	}
	if got := err.Error(); !containsAll(got, "invalid revision", "bad-rev") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestInitRootNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/models/owner/repo/tree/main/missing" {
			w.Header().Set("X-Error-Code", "EntryNotFound")
			w.Header().Set("X-Error-Message", "missing does not exist on \"main\"")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"missing does not exist on \"main\""}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	d := newTestDriver(server.URL)
	d.RootFolderPath = "/missing"
	err := d.Init(context.Background())
	if err == nil {
		t.Fatal("expected root not found error")
	}
	if got := err.Error(); !containsAll(got, "root folder path", "/missing") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestListMapsEntries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
{"type":"directory","oid":"tree1","size":0,"path":"sub"},
{"type":"file","oid":"blob1","size":12,"path":"README.md"}
]`))
	}))
	defer server.Close()

	d := newTestDriver(server.URL)
	if err := d.Init(context.Background()); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	objs, err := d.List(context.Background(), &model.Object{Path: "/", IsFolder: true}, model.ListArgs{})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objs))
	}
	if objs[0].GetName() != "sub" || !objs[0].IsDir() {
		t.Fatalf("unexpected first object: %+v", objs[0])
	}
	if objs[1].GetName() != "README.md" || objs[1].IsDir() {
		t.Fatalf("unexpected second object: %+v", objs[1])
	}
}

func TestListFollowsPagination(t *testing.T) {
	var requestCount int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Query().Get("cursor") == "2" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"type":"file","oid":"2","size":2,"path":"b.txt"}]`))
			return
		}
		w.Header().Set("Link", "<"+server.URL+"/api/models/owner/repo/tree/main?cursor=2>; rel=\"next\"")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"type":"file","oid":"1","size":1,"path":"a.txt"}]`))
	}))
	defer server.Close()

	d := newTestDriver(server.URL)
	if err := d.Init(context.Background()); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	objs, err := d.List(context.Background(), &model.Object{Path: "/", IsFolder: true}, model.ListArgs{})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objs))
	}
	if requestCount < 2 {
		t.Fatalf("expected pagination requests, got %d", requestCount)
	}
}

func TestListMissingFolderMapsObjectNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/models/owner/repo/tree/main/missing" {
			w.Header().Set("X-Error-Code", "EntryNotFound")
			w.Header().Set("X-Error-Message", "missing does not exist on \"main\"")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"missing does not exist on \"main\""}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	d := newTestDriver(server.URL)
	if err := d.Init(context.Background()); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	_, err := d.List(context.Background(), &model.Object{Path: "/missing", IsFolder: true}, model.ListArgs{})
	if !errors.Is(err, errs.ObjectNotFound) {
		t.Fatalf("expected object not found error, got: %v", err)
	}
}

func TestLinkGeneratesRepoTypePaths(t *testing.T) {
	testCases := []struct {
		repoType string
		wantURL  string
	}{
		{repoType: "model", wantURL: "https://huggingface.co/owner/repo/resolve/main/dir%2Ff.txt"},
		{repoType: "dataset", wantURL: "https://huggingface.co/datasets/owner/repo/resolve/main/dir%2Ff.txt"},
		{repoType: "space", wantURL: "https://huggingface.co/spaces/owner/repo/resolve/main/dir%2Ff.txt"},
	}

	for _, tc := range testCases {
		d := &HuggingFaceHub{
			Addition: Addition{
				RepoType: tc.repoType,
				RepoID:   "owner/repo",
				Revision: "main",
			},
			repoType:          tc.repoType,
			repoID:            "owner/repo",
			revision:          "main",
			normalizedAPIBase: "https://huggingface.co",
		}

		link, err := d.Link(context.Background(), &model.Object{
			Path:     "/dir/f.txt",
			Name:     "f.txt",
			IsFolder: false,
		}, model.LinkArgs{})
		if err != nil {
			t.Fatalf("link failed for repo type %s: %v", tc.repoType, err)
		}
		if link.URL != tc.wantURL {
			t.Fatalf("repo type %s url mismatch, got=%s want=%s", tc.repoType, link.URL, tc.wantURL)
		}
	}
}

func TestLinkUsesDownloadEndpoint(t *testing.T) {
	d := &HuggingFaceHub{
		Addition: Addition{
			RepoType:         "model",
			RepoID:           "owner/repo",
			Revision:         "main",
			DownloadEndpoint: "https://hf-mirror.test",
		},
		repoType:               "model",
		repoID:                 "owner/repo",
		revision:               "main",
		normalizedAPIBase:      "https://huggingface.co",
		normalizedDownloadBase: "https://hf-mirror.test",
	}

	link, err := d.Link(context.Background(), &model.Object{
		Path:     "/f.bin",
		Name:     "f.bin",
		IsFolder: false,
	}, model.LinkArgs{})
	if err != nil {
		t.Fatalf("link failed: %v", err)
	}
	if got, want := link.URL, "https://hf-mirror.test/owner/repo/resolve/main/f.bin"; got != want {
		t.Fatalf("download endpoint not used, got=%s want=%s", got, want)
	}
}

func TestLinkTokenForwardingRules(t *testing.T) {
	apiURL, _ := url.Parse("https://huggingface.co")
	differentDownloadURL, _ := url.Parse("https://hf-mirror.test")
	sameDownloadURL, _ := url.Parse("https://huggingface.co")

	testCases := []struct {
		name                   string
		downloadBase           string
		downloadURL            *url.URL
		forwardToMirror        bool
		args                   model.LinkArgs
		wantAuthorizationValue string
	}{
		{
			name:                   "empty download endpoint uses api and forwards token",
			downloadBase:           "",
			downloadURL:            nil,
			forwardToMirror:        false,
			args:                   model.LinkArgs{},
			wantAuthorizationValue: "Bearer hf_token",
		},
		{
			name:                   "same host download endpoint forwards token in proxy flow",
			downloadBase:           "https://huggingface.co",
			downloadURL:            sameDownloadURL,
			forwardToMirror:        false,
			args:                   model.LinkArgs{},
			wantAuthorizationValue: "Bearer hf_token",
		},
		{
			name:                   "302 redirect keeps download anonymous",
			downloadBase:           "",
			downloadURL:            nil,
			forwardToMirror:        false,
			args:                   model.LinkArgs{Redirect: true},
			wantAuthorizationValue: "",
		},
		{
			name:                   "different host mirror does not forward by default",
			downloadBase:           "https://hf-mirror.test",
			downloadURL:            differentDownloadURL,
			forwardToMirror:        false,
			args:                   model.LinkArgs{},
			wantAuthorizationValue: "",
		},
		{
			name:                   "different host mirror forwards when opted in",
			downloadBase:           "https://hf-mirror.test",
			downloadURL:            differentDownloadURL,
			forwardToMirror:        true,
			args:                   model.LinkArgs{},
			wantAuthorizationValue: "Bearer hf_token",
		},
		{
			name:                   "302 redirect stays anonymous even when mirror forwarding is enabled",
			downloadBase:           "https://hf-mirror.test",
			downloadURL:            differentDownloadURL,
			forwardToMirror:        true,
			args:                   model.LinkArgs{Redirect: true},
			wantAuthorizationValue: "",
		},
		{
			name:            "admin get-link can include auth on same host",
			downloadBase:    "https://huggingface.co",
			downloadURL:     sameDownloadURL,
			forwardToMirror: false,
			args: model.LinkArgs{
				Redirect: true,
				Type:     model.LinkTypeAdminGetLink,
			},
			wantAuthorizationValue: "Bearer hf_token",
		},
		{
			name:            "admin get-link keeps mirror safety by default",
			downloadBase:    "https://hf-mirror.test",
			downloadURL:     differentDownloadURL,
			forwardToMirror: false,
			args: model.LinkArgs{
				Redirect: true,
				Type:     model.LinkTypeAdminGetLink,
			},
			wantAuthorizationValue: "",
		},
		{
			name:            "admin get-link can forward to mirror when opted in",
			downloadBase:    "https://hf-mirror.test",
			downloadURL:     differentDownloadURL,
			forwardToMirror: true,
			args: model.LinkArgs{
				Redirect: true,
				Type:     model.LinkTypeAdminGetLink,
			},
			wantAuthorizationValue: "Bearer hf_token",
		},
	}

	for _, tc := range testCases {
		d := &HuggingFaceHub{
			Addition: Addition{
				RepoType:                     "model",
				RepoID:                       "owner/repo",
				Revision:                     "main",
				ForwardTokenToDownloadMirror: tc.forwardToMirror,
			},
			repoType:               "model",
			repoID:                 "owner/repo",
			revision:               "main",
			token:                  "hf_token",
			normalizedAPIBase:      "https://huggingface.co",
			apiEndpointURL:         apiURL,
			normalizedDownloadBase: tc.downloadBase,
			downloadEndpointURL:    tc.downloadURL,
		}

		link, err := d.Link(context.Background(), &model.Object{
			Path:     "/large.bin",
			Name:     "large.bin",
			IsFolder: false,
		}, tc.args)
		if err != nil {
			t.Fatalf("%s: link failed: %v", tc.name, err)
		}
		if got := link.Header.Get("Authorization"); got != tc.wantAuthorizationValue {
			t.Fatalf("%s: unexpected Authorization header: got=%q want=%q", tc.name, got, tc.wantAuthorizationValue)
		}
	}
}

func containsAll(input string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(input, p) {
			return false
		}
	}
	return true
}
