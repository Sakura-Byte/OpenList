package jmcomic

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type id string

func (v *id) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		s = ""
	}
	*v = id(s)
	return nil
}

type number int64

func (n *number) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" || s == "" {
		*n = 0
		return nil
	}
	i, e := strconv.ParseInt(s, 10, 64)
	*n = number(i)
	return e
}
func validID(s string) bool { n, e := strconv.ParseUint(s, 10, 64); return e == nil && n > 0 }
func safeName(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		switch r {
		case '/':
			return '／'
		case '\\':
			return '＼'
		case ':':
			return '：'
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if s == "" || s == "." || s == ".." {
		return "untitled"
	}
	return s
}

type album struct {
	ID      id     `json:"id"`
	Name    string `json:"name"`
	Author  string `json:"author"`
	Image   string `json:"image"`
	Date    string `json:"adddate"`
	Updated number `json:"update_at"`
}

func (a album) name() string { return "JM" + string(a.ID) + " " + safeName(a.Name) }
func (a album) datePath() string {
	t, e := time.Parse("2006-01-02", a.Date)
	if e != nil {
		return "/所有漫画/日期未知"
	}
	return "/所有漫画/" + t.Format("2006/01/02")
}
func (a album) canonical() string { return path.Join(a.datePath(), a.name()) }

type chapter struct {
	ID   id     `json:"id"`
	Name string `json:"name"`
	Sort id     `json:"sort"`
}

func (c chapter) name(index int) string {
	return fmt.Sprintf("%03d %s [%s]", index+1, safeName(c.Name), c.ID)
}

type detail struct {
	ID          id        `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Authors     []string  `json:"author"`
	Tags        []string  `json:"tags"`
	Series      []chapter `json:"series"`
}
type page struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Segments int    `json:"segments"`
	GIF      bool   `json:"gif"`
}
type chapterTree struct {
	ID      string `json:"id"`
	AlbumID string `json:"album_id"`
	Pages   []page `json:"pages"`
	Version int64  `json:"version"`
}
type cachedAlbum struct {
	Album         album
	Detail        *detail
	DetailVersion int64
	Active        bool
	Seen          string
	Error         string
	RetryAt       int64
}
type feed struct {
	Path    string   `json:"path"`
	IDs     []string `json:"ids"`
	Updated int64    `json:"updated"`
}
type syncState struct {
	Mode          string `json:"mode"`
	Paused        bool   `json:"paused"`
	LastSync      int64  `json:"last_sync"`
	LastFull      int64  `json:"last_full"`
	LastFeeds     int64  `json:"last_feeds"`
	Watermark     int64  `json:"watermark"`
	FullCompleted bool   `json:"full_completed"`
	Requested     string `json:"requested"`
	FeedRequested bool   `json:"feed_requested"`
	RetryAt       int64  `json:"retry_at"`
	FeedRetryAt   int64  `json:"feed_retry_at"`
	Error         string `json:"error"`
}

func encode(v any) string { b, _ := json.Marshal(v); return string(b) }
