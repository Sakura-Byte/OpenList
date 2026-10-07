package driver

import (
	"context"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type JMComicStatus struct {
	Phase        string `json:"phase"`
	Paused       bool   `json:"paused"`
	Works        int64  `json:"works"`
	Cached       int64  `json:"cached"`
	Chapters     int64  `json:"chapters"`
	Pending      int64  `json:"pending"`
	Failed       int64  `json:"failed"`
	Pages        int    `json:"pages"`
	CurrentTask  string `json:"current_task"`
	LastSync     int64  `json:"last_sync"`
	LastFullSync int64  `json:"last_full_sync"`
	LastFeeds    int64  `json:"last_feeds"`
	NextSync     int64  `json:"next_sync"`
	NextFullSync int64  `json:"next_full_sync"`
	NextFeeds    int64  `json:"next_feeds"`
	CacheBytes   int64  `json:"cache_bytes"`
	LastError    string `json:"last_error"`
}
type JMComicManager interface {
	JMComicStatus(context.Context) (JMComicStatus, error)
	JMComicAction(context.Context, string, string) error
}

// ContentGuard can reject content paths absent from a browsing-only mount
// before object lookup. Metadata and directory listing remain available.
type ContentGuard interface{ CheckContentPath(string) error }

type ReaderDirectory interface {
	DirectoryReader(context.Context, string) (*model.ReaderInfo, error)
}
type JMReaderChapter struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Path string `json:"path"`
}
type JMReaderPage struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Segments int    `json:"segments"`
	GIF      bool   `json:"gif"`
}
type JMReaderTag struct {
	Name string `json:"name"`
	Path string `json:"path"`
}
type JMReaderManifest struct {
	Source      string            `json:"source"`
	Mode        string            `json:"mode"`
	AlbumID     string            `json:"album_id"`
	ChapterID   string            `json:"chapter_id,omitempty"`
	AlbumPath   string            `json:"album_path"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Authors     []string          `json:"authors"`
	Tags        []string          `json:"tags"`
	TagLinks    []JMReaderTag     `json:"tag_links,omitempty"`
	Date        string            `json:"date"`
	Updated     int64             `json:"updated"`
	Cover       string            `json:"cover"`
	Chapters    []JMReaderChapter `json:"chapters"`
	Pages       []JMReaderPage    `json:"pages"`
	Unavailable bool              `json:"unavailable,omitempty"`
}
