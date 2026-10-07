package jmcomic

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
)

type JMComic struct {
	model.Storage
	Addition
	store                                      *cacheStore
	ctx                                        context.Context
	cancel                                     context.CancelFunc
	wg                                         sync.WaitGroup
	mu                                         sync.Mutex
	closing                                    bool
	state                                      syncState
	phase, current, feedCurrent, manualCurrent string
	pages                                      int
	backgroundCancel, feedCancel               context.CancelFunc
	scheduler                                  *apiScheduler
	host, fingerprint                          string
	apiClient                                  *http.Client
	detailGroup                                singleflight.Group[detail]
	treeGroup                                  singleflight.Group[chapterTree]
	tagGroup                                   singleflight.Group[bool]
	catalogGroup                               singleflight.Group[bool]
	readerRefresh                              map[string]time.Time
	sessionGroup                               singleflight.Group[bool]
	sessionReady                               bool
	clientMode                                 atomic.Bool
}

func (d *JMComic) Config() driver.Config          { return config }
func (d *JMComic) GetAddition() driver.Additional { return &d.Addition }
func (d *JMComic) Init(ctx context.Context) error {
	if d.APIURL == "" {
		d.APIURL = "https://www.cdnhjk.net"
	}
	u, e := url.Parse(strings.TrimRight(d.APIURL, "/"))
	if e != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid JMComic API URL")
	}
	d.APIURL = u.String()
	d.host = strings.ToLower(u.Host)
	if d.Mode == "" {
		d.Mode = "client"
	}
	if d.Mode != "client" && d.Mode != "server" {
		return errors.New("invalid JMComic mode")
	}
	d.clientMode.Store(d.Mode == "client")
	if d.RootFolderPath != "" && d.RootFolderPath != "/" {
		return errors.New("JMComic root must be /")
	}
	d.RootFolderPath = "/"
	if d.RequestsPerSecond == 0 {
		d.RequestsPerSecond = 1
	}
	if d.RequestsPerSecond <= 0 || d.RequestsPerSecond > 100 {
		return errors.New("invalid JMComic request rate")
	}
	defaults := []struct {
		p *int
		v int
	}{{&d.SyncHours, 24}, {&d.FullSyncDays, 7}, {&d.FeedHours, 6}, {&d.FeedLimit, 200}, {&d.InputMiB, 16}, {&d.OutputMiB, 32}, {&d.ConversionConcurrency, 1}}
	for _, v := range defaults {
		if *v.p == 0 {
			*v.p = v.v
		}
		if *v.p < 1 {
			return errors.New("JMComic limits and intervals must be positive")
		}
	}
	if d.FeedLimit > 10000 || d.InputMiB > 1024 || d.OutputMiB > 1024 || d.ConversionConcurrency > 32 {
		return errors.New("JMComic limit exceeds maximum")
	}
	if d.MaxPixels == 0 {
		d.MaxPixels = 16000000
	}
	if d.MaxPixels < 1 || d.MaxPixels > 256000000 {
		return errors.New("invalid JMComic pixel limit")
	}
	d.fingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte(d.APIURL)))[:16]
	root := d.CacheRoot
	if root == "" {
		root = filepath.Join(flags.DataDir, "cache", "jmcomic")
	}
	root, e = filepath.Abs(root)
	if e != nil {
		return e
	}
	d.store, e = openStore(filepath.Join(root, strconv.FormatUint(uint64(d.ID), 10), d.fingerprint, "cache.db"))
	if e != nil {
		return e
	}
	d.state, e = d.store.loadState(ctx)
	if e != nil {
		d.store.db.Close()
		return e
	}
	if e = d.store.saveTagCatalog(ctx, strings.Split(strings.ReplaceAll(d.Tags, "\r", ""), "\n")); e != nil {
		d.store.db.Close()
		return e
	}
	api := *base.APIClientFor(d)
	api.Timeout = 30 * time.Second
	api.Jar, e = cookiejar.New(nil)
	if e != nil {
		d.store.db.Close()
		return e
	}
	d.apiClient = &api
	if d.state.Mode != d.Mode {
		d.state.Mode = d.Mode
		if e = d.store.saveState(ctx, d.state); e != nil {
			d.store.db.Close()
			return e
		}
	}
	d.scheduler = acquireScheduler(d.host, d, d.RequestsPerSecond)
	acquireConversionOwner(d, d.ConversionConcurrency)
	d.mu.Lock()
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.closing = false
	d.phase = "idle"
	d.sessionReady = false
	d.wg.Add(3)
	d.mu.Unlock()
	go d.catalogLoop()
	go d.feedLoop()
	go d.taskLoop()
	return nil
}
func (d *JMComic) begin(ctx context.Context) (context.Context, func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, nil, e
	}
	d.mu.Lock()
	if d.closing || d.cancel == nil {
		d.mu.Unlock()
		return nil, nil, errors.New("JMComic storage stopped")
	}
	d.wg.Add(1)
	life := d.ctx
	d.mu.Unlock()
	c, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	return c, func() { stop(); cancel(); d.wg.Done() }, nil
}
func (d *JMComic) Drop(ctx context.Context) error {
	d.mu.Lock()
	if d.cancel == nil {
		d.mu.Unlock()
		return nil
	}
	d.closing = true
	d.cancel()
	d.mu.Unlock()
	d.wg.Wait()
	releaseScheduler(d.host, d)
	releaseConversionOwner(d)
	e := d.store.db.Close()
	d.mu.Lock()
	d.cancel = nil
	d.mu.Unlock()
	return e
}

type object struct {
	model.Object
	reader             *model.ReaderInfo
	albumID, chapterID string
	cover              string
	page               *page
	content            []byte
	tag                *model.TagInfo
}

func (o *object) GetTagInfo() *model.TagInfo { return o.tag }

func (o *object) GetReaderInfo() *model.ReaderInfo { return o.reader }
func (o *object) Thumb() string                    { return o.cover }
func folder(p string) *object {
	return &object{Object: model.Object{ID: p, Path: p, Name: path.Base(p), IsFolder: true}}
}
func (d *JMComic) albumObject(a album, parent string) *object {
	o := folder(path.Join(parent, a.name()))
	o.albumID = string(a.ID)
	o.ID = "album/" + o.albumID
	o.Modified = time.Unix(int64(a.Updated), 0)
	o.reader = &model.ReaderInfo{Kind: "album", Mode: d.Mode, AlbumID: o.albumID}
	o.cover = d.coverURL(a)
	return o
}
func validImageURL(s string) string {
	u, e := url.Parse(s)
	if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return ""
	}
	return s
}
func validImageHost(s string) string {
	s = strings.TrimRight(strings.TrimSpace(s), "/")
	u, e := url.Parse(s)
	if e != nil || validImageURL(s) == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return ""
	}
	return s
}
func (d *JMComic) coverURL(a album) string {
	if s := validImageURL(a.Image); s != "" {
		return s
	}
	d.mu.Lock()
	host := validImageHost(d.state.ImageHost)
	d.mu.Unlock()
	if host == "" {
		return ""
	}
	if a.Image == "" {
		if !validID(string(a.ID)) {
			return ""
		}
		return host + "/media/albums/" + string(a.ID) + "_3x4.jpg"
	}
	u, e := url.Parse(a.Image)
	if e != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(path.Clean(u.Path), "/media/") {
		return ""
	}
	base, _ := url.Parse(host)
	return base.ResolveReference(u).String()
}
func (d *JMComic) Get(ctx context.Context, p string) (model.Obj, error) {
	c, done, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	p = path.Clean("/" + strings.TrimPrefix(p, "/"))
	o := folder("/")
	if p == "/" {
		return o, nil
	}
	for _, part := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		children, e := d.list(c, o, driver.IsCachedTraversal(c))
		if e != nil {
			return nil, e
		}
		var found *object
		for _, v := range children {
			if v.GetName() == part {
				found = v.(*object)
				break
			}
		}
		if found == nil {
			return nil, errs.ObjectNotFound
		}
		o = found
	}
	return o, nil
}
func (d *JMComic) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	c, done, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	o, ok := dir.(*object)
	if !ok {
		resolved, err := d.Get(c, dir.GetPath())
		if err != nil {
			return nil, err
		}
		o, ok = resolved.(*object)
		if !ok {
			return nil, errs.ObjectNotFound
		}
	}
	return d.list(c, o, driver.IsCachedTraversal(c))
}
func (d *JMComic) ListCached(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	return d.List(driver.WithCachedTraversal(ctx), dir, args)
}

var rootNames = []string{"今日推荐", "推荐", "排行榜", "分类", "标签", "最近更新", "所有漫画"}

func (d *JMComic) list(ctx context.Context, o *object, cached bool) ([]model.Obj, error) {
	if !o.IsDir() {
		return nil, errs.NotFolder
	}
	p := o.Path
	out := []model.Obj{}
	if p == "/" {
		for _, name := range rootNames {
			if cached && name != "所有漫画" {
				continue
			}
			out = append(out, folder("/"+name))
		}
		return out, nil
	}
	if o.albumID != "" {
		return d.albumChildren(ctx, o, cached)
	}
	if p == "/标签" {
		if cached {
			return out, nil
		}
		return d.tagFolders(ctx)
	}
	if o.tag != nil {
		if cached {
			return out, nil
		}
		t := cachedTag{Name: o.tag.Name, Path: o.Path}
		var fetchError error
		if !cached {
			fetchError = d.ensureTagFeed(ctx, t.Name, false, true)
		}
		items, e := d.store.tagAlbums(ctx, t)
		if e != nil {
			return nil, e
		}
		if fetchError != nil && len(items) == 0 {
			return nil, fetchError
		}
		for _, a := range items {
			out = append(out, d.albumObject(a.Album, p))
		}
		return out, nil
	}
	if strings.HasPrefix(p, "/所有漫画") {
		rows, e := d.store.db.QueryContext(ctx, `SELECT DISTINCT date FROM albums WHERE active=1 ORDER BY date DESC`)
		if e != nil {
			return nil, e
		}
		var dates []string
		for rows.Next() {
			var s string
			if e = rows.Scan(&s); e != nil {
				rows.Close()
				return nil, e
			}
			a := album{Date: s}
			dates = append(dates, a.datePath())
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		prefix := strings.TrimRight(p, "/") + "/"
		names := map[string]bool{}
		leaf := false
		for _, date := range dates {
			if date == p {
				leaf = true
			}
			if strings.HasPrefix(date, prefix) {
				names[strings.Split(strings.TrimPrefix(date, prefix), "/")[0]] = true
			}
		}
		if !leaf {
			var ordered []string
			for name := range names {
				ordered = append(ordered, name)
			}
			sort.Sort(sort.Reverse(sort.StringSlice(ordered)))
			for _, name := range ordered {
				out = append(out, folder(path.Join(p, name)))
			}
			return out, nil
		}
		items, e := d.store.albums(ctx, `SELECT `+albumColumns+` FROM albums WHERE active=1 AND substr(path,1,length(?))=? ORDER BY CAST(id AS INTEGER) DESC`, prefix, prefix)
		if e != nil {
			return nil, e
		}
		for _, a := range items {
			out = append(out, d.albumObject(a.Album, p))
		}
		return out, nil
	}
	if cached {
		return out, nil
	}
	if p == "/推荐" || p == "/排行榜" || p == "/分类" || p == "/标签" {
		rows, e := d.store.db.QueryContext(ctx, `SELECT path FROM feeds ORDER BY path`)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		prefix := p + "/"
		for rows.Next() {
			var name string
			if e = rows.Scan(&name); e != nil {
				return nil, e
			}
			if strings.HasPrefix(name, prefix) && !strings.Contains(strings.TrimPrefix(name, prefix), "/") {
				out = append(out, folder(name))
			}
		}
		return out, rows.Err()
	}
	items, e := d.store.albums(ctx, `SELECT a.data,a.detail,a.detail_version,a.active,a.seen,a.error,a.retry_at FROM albums a JOIN feed_members m ON m.id=a.id WHERE m.path=? AND a.active=1 ORDER BY m.position`, p)
	if e != nil {
		return nil, e
	}
	for _, a := range items {
		out = append(out, d.albumObject(a.Album, p))
	}
	return out, nil
}
func (d *JMComic) albumChildren(ctx context.Context, o *object, cached bool) ([]model.Obj, error) {
	a, e := d.store.album(ctx, o.albumID)
	if e != nil {
		return nil, e
	}
	dt := a.Detail
	if !cached {
		v, e := d.loadDetail(ctx, a, false, true)
		if e != nil && !errors.Is(e, errNoAlbumDetail) {
			return nil, e
		}
		if e == nil {
			dt = &v
		}
	}
	out := []model.Obj{}
	readme := fmt.Sprintf("# %s\n\n- JM ID: %s\n- 上架日期: %s\n- Mode: %s\n", a.Album.Name, o.albumID, a.Album.Date, d.Mode)
	if dt != nil {
		readme += "\n" + dt.Description + "\n\nAuthors: " + strings.Join(dt.Authors, ", ") + "\nTags: " + strings.Join(dt.Tags, ", ") + "\n"
	}
	r := &object{Object: model.Object{ID: o.ID + "/readme", Name: "README.md", Path: path.Join(o.Path, "README.md"), Size: int64(len([]byte(readme)))}, content: []byte(readme)}
	out = append(out, r)
	if o.chapterID == "" {
		if cover := d.coverURL(a.Album); d.Mode == "server" && cover != "" {
			out = append(out, &object{Object: model.Object{ID: o.ID + "/cover", Name: "cover.jpg", Path: path.Join(o.Path, "cover.jpg"), Size: 0}, albumID: o.albumID, page: &page{URL: cover}})
		}
		if dt != nil {
			for i, c := range dt.Series {
				v := folder(path.Join(o.Path, c.name(i)))
				v.albumID = o.albumID
				v.chapterID = string(c.ID)
				v.ID = "chapter/" + v.chapterID
				v.Modified = o.Modified
				v.reader = &model.ReaderInfo{Kind: "chapter", Mode: d.Mode, AlbumID: o.albumID, ChapterID: v.chapterID}
				out = append(out, v)
			}
		}
		return out, nil
	}
	if d.Mode == "client" {
		return out, nil
	}
	var tree chapterTree
	if cached {
		tree, e = d.store.tree(ctx, o.chapterID)
		if errors.Is(e, sql.ErrNoRows) {
			return out, nil
		}
	} else {
		tree, e = d.loadTree(ctx, a, o.chapterID, false, true)
	}
	if e != nil {
		return nil, e
	}
	for _, pg := range tree.Pages {
		v := pg
		out = append(out, &object{Object: model.Object{ID: "page/" + o.chapterID + "/" + v.Name, Name: v.Name, Path: path.Join(o.Path, v.Name), Size: 0, Modified: o.Modified}, albumID: o.albumID, chapterID: o.chapterID, page: &v})
	}
	return out, nil
}
func (d *JMComic) loadDetail(ctx context.Context, a cachedAlbum, force, high bool) (detail, error) {
	if !force && a.Detail != nil && a.DetailVersion == int64(a.Album.Updated) {
		return *a.Detail, nil
	}
	if !force {
		var message string
		var retryAt int64
		e := d.store.db.QueryRowContext(ctx, `SELECT error,retry_at FROM tasks WHERE kind='album' AND key=?`, string(a.Album.ID)).Scan(&message, &retryAt)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return detail{}, e
		}
		missing := fmt.Errorf("get JM%s details: %w", a.Album.ID, errNoAlbumDetail)
		if e == nil && retryAt > time.Now().Unix() && message == missing.Error() {
			if a.Detail != nil {
				return *a.Detail, nil
			}
			return detail{}, missing
		}
	}
	d.mu.Lock()
	life := d.ctx
	d.mu.Unlock()
	ch := d.detailGroup.DoChan(string(a.Album.ID), func() (detail, error) {
		c, done, e := d.begin(life)
		if e != nil {
			return detail{}, e
		}
		defer done()
		fresh, e := d.store.album(c, string(a.Album.ID))
		if e != nil {
			return detail{}, e
		}
		if !force && fresh.Detail != nil && fresh.DetailVersion == int64(fresh.Album.Updated) {
			return *fresh.Detail, nil
		}
		dt, e := d.fetchDetail(c, string(a.Album.ID), high)
		if e == nil {
			e = d.store.saveDetail(c, fresh, dt)
		}
		if e == nil && force {
			_, e = d.store.db.ExecContext(c, `UPDATE chapters SET version=-1 WHERE album_id=?`, string(a.Album.ID))
			if e == nil {
				for _, ch := range dt.Series {
					var count int
					e = d.store.db.QueryRowContext(c, `SELECT COUNT(*) FROM chapters WHERE id=?`, string(ch.ID)).Scan(&count)
					if e != nil {
						break
					}
					if count > 0 {
						if high {
							e = d.store.queueManual(c, "chapter", string(a.Album.ID)+"/"+string(ch.ID))
						} else {
							e = d.store.queue(c, "chapter", string(a.Album.ID)+"/"+string(ch.ID))
						}
						if e != nil {
							break
						}
					}
				}
			}
		}
		if e != nil {
			d.recordFailure(c, "album", string(a.Album.ID), e)
		}
		return dt, e
	})
	select {
	case <-ctx.Done():
		return detail{}, ctx.Err()
	case r := <-ch:
		if r.Err != nil && !force && a.Detail != nil {
			return *a.Detail, nil
		}
		return r.Val, r.Err
	}
}
func (d *JMComic) loadTree(ctx context.Context, a cachedAlbum, key string, force, high bool) (chapterTree, error) {
	old, oldErr := d.store.tree(ctx, key)
	if !force && oldErr == nil && old.Version == int64(a.Album.Updated) {
		return old, nil
	}
	d.mu.Lock()
	life := d.ctx
	d.mu.Unlock()
	ch := d.treeGroup.DoChan(key, func() (chapterTree, error) {
		c, done, e := d.begin(life)
		if e != nil {
			return chapterTree{}, e
		}
		defer done()
		v, e := d.store.tree(c, key)
		if !force && e == nil && v.Version == int64(a.Album.Updated) {
			return v, nil
		}
		q := url.Values{"id": {key}, "app_img_shunt": {"1"}, "mode": {"vertical"}, "page": {"0"}, "express": {"off"}, "v": {strconv.FormatInt(time.Now().Unix(), 10)}}
		b, e := d.request(c, "/chapter_view_template", q, high, true)
		if e == nil {
			v, e = parseTemplate(b, key, string(a.Album.ID), int64(a.Album.Updated))
		}
		if e == nil {
			e = d.store.saveTree(c, a.Album, v)
		}
		if e != nil {
			d.recordFailure(c, "chapter", string(a.Album.ID)+"/"+key, e)
		}
		return v, e
	})
	select {
	case <-ctx.Done():
		return chapterTree{}, ctx.Err()
	case r := <-ch:
		if r.Err != nil && !force && oldErr == nil {
			return old, nil
		}
		return r.Val, r.Err
	}
}

var _ driver.Driver = (*JMComic)(nil)
var _ driver.CachedTree = (*JMComic)(nil)
