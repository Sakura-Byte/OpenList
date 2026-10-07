package jmcomic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func (d *JMComic) DirectoryReader(ctx context.Context, p string) (*model.ReaderInfo, error) {
	o, e := d.Get(ctx, p)
	if e != nil {
		return nil, e
	}
	return model.GetReaderInfo(o), nil
}
func (d *JMComic) CheckContentPath(p string) error {
	if !d.clientMode.Load() {
		return nil
	}
	switch strings.ToLower(path.Ext(p)) {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif":
		return fmt.Errorf("%w: JMComic client mode supports the web reader and browser exports; select server mode for filesystem image downloads", errs.NotSupport)
	}
	return nil
}
func (d *JMComic) Other(ctx context.Context, args model.OtherArgs) (any, error) {
	c, done, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if args.Method != "jm_reader" {
		return nil, errs.NotSupport
	}
	o, ok := args.Obj.(*object)
	if !ok || o.reader == nil {
		return nil, errors.New("JMComic reader requires a work or chapter directory")
	}
	var opts struct {
		Refresh bool `json:"refresh"`
	}
	if args.Data != nil {
		b, e := json.Marshal(args.Data)
		if e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &opts); e != nil {
			return nil, e
		}
	}
	if opts.Refresh {
		d.mu.Lock()
		if d.readerRefresh == nil {
			d.readerRefresh = map[string]time.Time{}
		}
		key := o.ID
		if time.Since(d.readerRefresh[key]) < 5*time.Minute {
			opts.Refresh = false
		} else {
			d.readerRefresh[key] = time.Now()
		}
		d.mu.Unlock()
	}
	a, e := d.store.album(c, o.albumID)
	if e != nil {
		return nil, e
	}
	dt, e := d.loadDetail(c, a, opts.Refresh && o.chapterID == "", true)
	missing := errors.Is(e, errNoAlbumDetail) && o.chapterID == ""
	if missing {
		if a.Detail != nil {
			dt = *a.Detail
		} else {
			dt = detail{ID: a.Album.ID, Name: a.Album.Name}
			if a.Album.Author != "" {
				dt.Authors = []string{a.Album.Author}
			}
		}
	} else if e != nil {
		return nil, e
	}
	if d.coverURL(a.Album) == "" && (a.Album.Image == "" || strings.HasPrefix(a.Album.Image, "/")) {
		// Cached details may predate the image-host cache. Bootstrap metadata
		// once on explicit reader access; ordinary lists never fetch settings.
		_ = d.ensureSession(c, true)
	}
	albumPath := o.Path
	if o.chapterID != "" {
		albumPath = path.Dir(o.Path)
	}
	r := &driver.JMReaderManifest{Source: d.fingerprint, Mode: d.Mode, AlbumID: o.albumID, ChapterID: o.chapterID, AlbumPath: albumPath, Title: a.Album.Name, Description: dt.Description, Authors: dt.Authors, Tags: dt.Tags, Date: a.Album.Date, Updated: int64(a.Album.Updated), Cover: d.coverURL(a.Album), Chapters: []driver.JMReaderChapter{}, Pages: []driver.JMReaderPage{}}
	r.Unavailable = missing
	for _, tag := range dt.Tags {
		if strings.TrimSpace(tag) != "" {
			r.TagLinks = append(r.TagLinks, driver.JMReaderTag{Name: tag, Path: tagPath(tag)})
		}
	}
	for i, ch := range dt.Series {
		r.Chapters = append(r.Chapters, driver.JMReaderChapter{ID: string(ch.ID), Name: ch.Name, Path: path.Join(albumPath, ch.name(i))})
	}
	if o.chapterID != "" {
		tree, e := d.loadTree(c, a, o.chapterID, opts.Refresh, true)
		if e != nil {
			return nil, e
		}
		for _, pg := range tree.Pages {
			u := ""
			if d.Mode == "client" {
				u = pg.URL
			}
			r.Pages = append(r.Pages, driver.JMReaderPage{Name: pg.Name, URL: u, Segments: pg.Segments, GIF: pg.GIF})
		}
	}
	return r, nil
}
