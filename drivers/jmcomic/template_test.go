package jmcomic

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

const javascriptChapterTemplate = `
const config = {
    jmid: '42',
    imghost: 'https://images.example',
    cache: '?v=1791340457'
};
const result = {
    images: ['00001.webp','00002.webp',]
};
var scramble_id = 220980;
`

func TestClientReaderJavaScriptChapterTemplate(t *testing.T) {
	var trees, images atomic.Int32
	s := newMockServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/album":
			replyJSON(w, r, detail{ID: "42", Name: "Example", Series: []chapter{{ID: "42", Name: "Chapter", Sort: "1"}}})
		case "/chapter_view_template":
			trees.Add(1)
			io.WriteString(w, javascriptChapterTemplate)
		default:
			images.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()
	d := newTestDriver(t, s.URL, t.TempDir(), "client")
	a := seedAlbum(t, d, "42", "Example", "2026-10-07", 1)
	album, err := d.Get(context.Background(), a.Album.canonical())
	require.NoError(t, err)
	m, err := d.Other(context.Background(), model.OtherArgs{Obj: album, Method: "jm_reader"})
	require.NoError(t, err)
	chapter, err := d.Get(context.Background(), m.(*driver.JMReaderManifest).Chapters[0].Path)
	require.NoError(t, err)
	m, err = d.Other(context.Background(), model.OtherArgs{Obj: chapter, Method: "jm_reader"})
	require.NoError(t, err)
	manifest := m.(*driver.JMReaderManifest)
	require.Equal(t, "client", manifest.Mode)
	require.Len(t, manifest.Pages, 2)
	require.Equal(t, "https://images.example/media/photos/42/00001.webp?v=1791340457", manifest.Pages[0].URL)
	require.Equal(t, "0002.png", manifest.Pages[1].Name)
	_, err = d.Other(context.Background(), model.OtherArgs{Obj: chapter, Method: "jm_reader"})
	require.NoError(t, err)
	require.EqualValues(t, 1, trees.Load())
	require.Zero(t, images.Load())
}

func TestTemplateLiteralCompatibility(t *testing.T) {
	for _, test := range []struct {
		name, input string
	}{
		{"strict JSON", `{"host":"https://images.example","images":["00001.webp"]}`},
		{"JavaScript", `{host:'https://images.example',images:['00001.webp',],}`},
		{"comments", `{/* } ignored */ host:'https://images.example', // trailing comment
		 images:['00001.webp', /* , } ] */],}`},
		{"escaped slash", `{host:'https:\/\/images.example',images:['00001.webp',]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var value struct {
				Host   string   `json:"host"`
				Images []string `json:"images"`
			}
			require.NoError(t, decodeTemplateObject(test.input, &value))
			require.Equal(t, "https://images.example", value.Host)
			require.Equal(t, []string{"00001.webp"}, value.Images)
		})
	}
	var value map[string]string
	require.NoError(t, decodeTemplateObject(`{quote:'a\'b"c',brackets:'{x:[1,],}',hex:'\x41',unicode:'\u0042'}`, &value))
	require.Equal(t, "a'b\"c", value["quote"])
	require.Equal(t, "{x:[1,],}", value["brackets"])
	require.Equal(t, "A", value["hex"])
	require.Equal(t, "B", value["unicode"])
	object, err := assignedObject(`const config={/* } [ */host:'https://images.example',};`, "config")
	require.NoError(t, err)
	require.NoError(t, decodeTemplateObject(object, &value))
}

func TestTemplateRejectsExpressionsAndMalformedLiterals(t *testing.T) {
	for _, input := range []string{
		`{host:fetch('https://example.com')}`,
		`{host:(()=>{throw new Error('must not execute')})()}`,
		`{images:[location.href]}`,
		`{host:'https://'+'example.com'}`,
		`{host:'unterminated}`,
		`{host:'bad\xZZ'}`,
		`{host:'bad\q'}`,
		`{host:'example',/* unterminated}`,
		`{images:['a',,'b']}`,
	} {
		var value any
		require.Error(t, decodeTemplateObject(input, &value), input)
	}
}
