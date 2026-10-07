package asmrone

import (
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadmeCoverRecognizedAsRemoteImage(t *testing.T) {
	for _, field := range []string{"cover", "thumbnail"} {
		t.Run(field, func(t *testing.T) {
			w := work{Title: "Info"}
			cover := "https://media.example/cover(1).jpg?type=main&name=<cover>"
			if field == "cover" {
				w.Cover = cover
				w.Thumbnail = "https://media.example/thumb.jpg"
			} else {
				w.Thumbnail = cover
			}
			// OpenList's Markdown preprocessor recognizes remote URLs before
			// rendering and otherwise rewrites them to unsigned local /d paths.
			match := regexp.MustCompile(`!\[Cover\]\((.*?)\)`).FindSubmatch(w.readme())
			require.Len(t, match, 2)
			imageURL := string(match[1])
			require.True(t, strings.HasPrefix(imageURL, "https://"), "cover would be rewritten as a local file")
			parsed, err := url.Parse(imageURL)
			require.NoError(t, err)
			require.Equal(t, "/cover(1).jpg", parsed.Path)
			require.Equal(t, "main", parsed.Query().Get("type"))
			require.Equal(t, "<cover>", parsed.Query().Get("name"))
		})
	}
}
