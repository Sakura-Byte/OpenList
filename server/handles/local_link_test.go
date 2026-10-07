package handles

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type localTextReader []byte

func (b localTextReader) RangeRead(ctx context.Context, r http_range.Range) (io.ReadCloser, error) {
	end := int64(len(b))
	if r.Length >= 0 {
		end = min(end, r.Start+r.Length)
	}
	return io.NopCloser(bytes.NewReader(b[r.Start:end])), nil
}

func TestDirectDownloadServesLocalReadmeRangeAndHead(t *testing.T) {
	setupProxyTest(t)
	for _, method := range []string{"GET", "HEAD"} {
		t.Run(method, func(t *testing.T) {
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			request := httptest.NewRequest(method, "http://example.com/d/README.md", nil)
			request.Header.Set("Range", "bytes=2-5")
			request = request.WithContext(context.WithValue(request.Context(), conf.UserKey, &model.User{ID: 1, Role: model.ADMIN}))
			c.Request = request
			data := localTextReader("# Readme\n")
			link := &model.Link{RangeReader: data, ContentLength: int64(len(data))}
			file := &model.Object{Name: "README.md", Size: int64(len(data))}
			redirect(c, link, file)
			expected := 206
			if method == "HEAD" {
				expected = 200
			}
			require.Equal(t, expected, response.Code)
			require.Empty(t, response.Header().Get("Location"))
			if method == "GET" {
				require.Equal(t, "Read", response.Body.String())
			} else {
				require.Empty(t, response.Body.String())
			}
		})
	}
}
