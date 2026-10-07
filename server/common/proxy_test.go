package common

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestProxyOverridesUpstreamContentDisposition(t *testing.T) {
	previousConfig := conf.Conf
	conf.Conf = conf.DefaultConfig("data")
	t.Cleanup(func() {
		conf.Conf = previousConfig
	})

	const content = "archive content"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="download"`)
		w.Header().Set("Content-Type", "application/x-rar-compressed")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, content)
	}))
	t.Cleanup(upstream.Close)

	file := &model.Object{
		Name: "测试文件.rar",
		Size: int64(len(content)),
	}
	link := &model.Link{URL: upstream.URL}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/sd/example", nil)

	err := Proxy(recorder, request, link, file)
	if err != nil {
		t.Fatalf("Proxy() error = %v", err)
	}
	if got, want := recorder.Code, http.StatusOK; got != want {
		t.Fatalf("status code = %d, want %d", got, want)
	}
	if got, want := recorder.Header().Get("Content-Disposition"), utils.GenerateContentDisposition(file.GetName()); got != want {
		t.Errorf("Content-Disposition = %q, want %q", got, want)
	}
	if got, want := recorder.Header().Get("Content-Type"), "application/x-rar-compressed"; got != want {
		t.Errorf("Content-Type = %q, want %q", got, want)
	}
	if got, want := recorder.Body.String(), content; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestProxyRangePreservesUnknownSizeCover(t *testing.T) {
	previous := conf.Conf
	conf.Conf = conf.DefaultConfig("data")
	t.Cleanup(func() { conf.Conf = previous })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Content-Length", "5")
		if r.Method != "HEAD" {
			io.WriteString(w, "cover")
		}
	}))
	defer upstream.Close()
	file := &model.Object{Name: "cover.jpg"}
	link := &model.Link{URL: upstream.URL}
	wrapped := ProxyRange(context.Background(), link, 0)
	if wrapped != link {
		t.Fatal("unknown size cover was wrapped as an empty file")
	}
	for _, method := range []string{"GET", "HEAD"} {
		request := httptest.NewRequest(method, "/p/cover.jpg", nil)
		response := httptest.NewRecorder()
		if err := Proxy(response, request, wrapped, file); err != nil {
			t.Fatal(err)
		}
		if response.Code != 200 {
			t.Fatalf("status=%d", response.Code)
		}
		if method == "GET" && response.Body.String() != "cover" {
			t.Fatal("cover body missing")
		}
		if method == "HEAD" && response.Body.Len() != 0 {
			t.Fatal("HEAD returned body")
		}
	}
}
