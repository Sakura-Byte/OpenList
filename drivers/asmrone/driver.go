package asmrone

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
)

type ASMRONE struct {
	model.Storage
	Addition
	store             *cacheStore
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	mu                sync.Mutex
	state             syncState
	phase, current    string
	manualCurrent     string
	pages, totalPages int
	closing           bool
	scheduler         *apiScheduler
	host              string
	apiClient         *http.Client
	wake              chan struct{}
	manualWake        chan struct{}
	tracksGroup       singleflight.Group[[]track]
	recoveryGroup     singleflight.Group[[]track]
	recoveryAt        map[int64]time.Time
	backgroundCancel  context.CancelFunc
}

func (d *ASMRONE) Config() driver.Config          { return config }
func (d *ASMRONE) GetAddition() driver.Additional { return &d.Addition }
func (d *ASMRONE) Init(ctx context.Context) error {
	if d.APIURL == "" {
		d.APIURL = "https://api.asmr.one"
	}
	u, err := url.Parse(strings.TrimRight(d.APIURL, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return errors.New("invalid ASMR.ONE API URL")
	}
	d.APIURL = u.String()
	d.host = strings.ToLower(u.Host)
	if d.RequestsPerSecond == 0 {
		d.RequestsPerSecond = 1
	}
	if d.RequestsPerSecond < 0 || d.RequestsPerSecond > 100 {
		return errors.New("requests_per_second must be between 0 and 100")
	}
	if d.SyncHours == 0 {
		d.SyncHours = 24
	}
	if d.FullSyncDays == 0 {
		d.FullSyncDays = 7
	}
	if d.SyncHours < 1 || d.FullSyncDays < 1 {
		return errors.New("sync intervals must be positive")
	}
	if d.RootFolderPath != "" && d.RootFolderPath != "/" {
		return errors.New("ASMR.ONE root_folder_path must be /")
	}
	d.RootFolderPath = "/"
	root := d.CacheRoot
	if root == "" {
		root = filepath.Join(flags.DataDir, "cache", "asmrone")
	}
	if !filepath.IsAbs(root) {
		root, err = filepath.Abs(root)
		if err != nil {
			return err
		}
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256([]byte(d.APIURL)))[:16]
	d.store, err = openStore(filepath.Join(root, strconv.FormatUint(uint64(d.ID), 10), fingerprint, "cache.db"))
	if err != nil {
		return err
	}
	d.state, err = d.store.loadState(ctx)
	if err != nil {
		d.store.db.Close()
		return err
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.closing = false
	d.phase = "idle"
	d.current = ""
	d.pages = 0
	d.totalPages = 0
	d.recoveryAt = map[int64]time.Time{}
	d.wake = make(chan struct{}, 1)
	d.manualWake = make(chan struct{}, 1)
	api := *base.APIClientFor(d)
	api.Timeout = 30 * time.Second
	d.apiClient = &api
	d.scheduler = acquireScheduler(d.host, d, d.RequestsPerSecond)
	d.wg.Add(1)
	go d.background()
	d.wg.Add(1)
	go d.manualTasks()
	return nil
}
func (d *ASMRONE) Drop(ctx context.Context) error {
	d.mu.Lock()
	if d.cancel == nil {
		d.mu.Unlock()
		return nil
	}
	d.closing = true
	d.cancel()
	if d.backgroundCancel != nil {
		d.backgroundCancel()
	}
	d.mu.Unlock()
	d.wg.Wait()
	releaseScheduler(d.host, d)
	return d.store.db.Close()
}

type object struct {
	model.Object
	workID  int64
	node    track
	content []byte
	cover   string
}

func (o *object) Thumb() string { return o.cover }
func workObject(w cachedWork) *object {
	return &object{Object: model.Object{ID: strconv.FormatInt(int64(w.ID), 10), Name: w.name(), Path: "/" + w.name(), IsFolder: true, Modified: w.modified()}, workID: int64(w.ID), cover: w.Thumbnail}
}
func nodeObject(w cachedWork, n track, parent string) *object {
	id := n.Hash
	if id == "" {
		id = path.Join(parent, n.Title)
	}
	return &object{Object: model.Object{ID: id, Name: n.Title, Path: path.Join(parent, n.Title), IsFolder: n.isDir(), Size: int64(n.Size), Modified: w.modified()}, workID: int64(w.ID), node: n}
}
func (d *ASMRONE) Get(ctx context.Context, p string) (model.Obj, error) {
	p = path.Clean("/" + strings.TrimPrefix(p, "/"))
	if p == "/" {
		return &object{Object: model.Object{ID: "/", Path: "/", Name: "/", IsFolder: true}}, nil
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	w, err := d.store.byName(ctx, parts[0])
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errs.ObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(parts) == 1 {
		return workObject(w), nil
	}
	nodes, err := d.workChildren(ctx, w, "/"+parts[0], driver.IsCachedTraversal(ctx))
	if err != nil {
		return nil, err
	}
	var found model.Obj
	for i, segment := range parts[1:] {
		found = nil
		for _, obj := range nodes {
			if obj.GetName() == segment {
				found = obj
				break
			}
		}
		if found == nil {
			return nil, errs.ObjectNotFound
		}
		if i < len(parts)-2 {
			o := found.(*object)
			if !o.IsDir() {
				return nil, errs.NotFolder
			}
			nodes = make([]model.Obj, 0, len(o.node.Children))
			for _, n := range o.node.Children {
				nodes = append(nodes, nodeObject(w, n, o.Path))
			}
		}
	}
	return found, nil
}
func (d *ASMRONE) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	return d.list(ctx, dir, driver.IsCachedTraversal(ctx))
}
func (d *ASMRONE) ListCached(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	return d.list(driver.WithCachedTraversal(ctx), dir, true)
}
func (d *ASMRONE) list(ctx context.Context, dir model.Obj, cachedOnly bool) ([]model.Obj, error) {
	p := path.Clean(dir.GetPath())
	if p == "/" {
		works, err := d.store.works(ctx)
		if err != nil {
			return nil, err
		}
		result := make([]model.Obj, 0, len(works))
		for _, w := range works {
			result = append(result, workObject(w))
		}
		return result, nil
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	w, err := d.store.byName(ctx, parts[0])
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errs.ObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(parts) == 1 {
		return d.workChildren(ctx, w, p, cachedOnly)
	}
	obj, err := d.Get(ctx, p)
	if err != nil {
		return nil, err
	}
	o := obj.(*object)
	if !o.IsDir() {
		return nil, errs.NotFolder
	}
	result := make([]model.Obj, 0, len(o.node.Children))
	for _, n := range o.node.Children {
		result = append(result, nodeObject(w, n, p))
	}
	return result, nil
}
func (d *ASMRONE) workChildren(ctx context.Context, w cachedWork, p string, cachedOnly bool) ([]model.Obj, error) {
	nodes := w.Tracks
	if !w.HasTracks && !cachedOnly {
		var err error
		nodes, err = d.loadTracks(ctx, int64(w.ID), false, true)
		if err != nil {
			return nil, err
		}
	}
	result := make([]model.Obj, 0, len(nodes)+2)
	names := map[string]bool{}
	for _, n := range nodes {
		result = append(result, nodeObject(w, n, p))
		names[strings.ToLower(n.Title)] = true
	}
	if !names["readme.md"] {
		content := w.readme()
		result = append(result, &object{Object: model.Object{ID: fmt.Sprintf("%d:readme", w.ID), Name: "README.md", Path: path.Join(p, "README.md"), Size: int64(len(content)), Modified: w.modified()}, workID: int64(w.ID), content: content})
	}
	if w.Cover != "" && !names["cover.jpg"] {
		result = append(result, &object{Object: model.Object{ID: fmt.Sprintf("%d:cover", w.ID), Name: "cover.jpg", Path: path.Join(p, "cover.jpg"), Modified: w.modified()}, workID: int64(w.ID), cover: w.Cover})
	}
	return result, nil
}

type memoryReader struct{ data []byte }

func (r memoryReader) RangeRead(ctx context.Context, hr http_range.Range) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := hr.Start
	if start < 0 || start > int64(len(r.data)) {
		return nil, errors.New("invalid range")
	}
	end := int64(len(r.data))
	if hr.Length >= 0 && hr.Length < end-start {
		end = start + hr.Length
	}
	return io.NopCloser(bytes.NewReader(r.data[start:end])), nil
}
func (d *ASMRONE) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	// Resolve from disk rather than retaining URL metadata in OpenList's link cache.
	obj, err := d.Get(ctx, file.GetPath())
	if err != nil {
		return nil, err
	}
	o := obj.(*object)
	if o.IsDir() {
		return nil, errs.NotFile
	}
	expiry := time.Nanosecond
	if o.content != nil {
		return &model.Link{RangeReader: memoryReader{o.content}, ContentLength: o.Size, Expiration: &expiry}, nil
	}
	linkURL := mediaURL(o.node)
	if o.cover != "" {
		linkURL = o.cover
	}
	if linkURL == "" {
		return nil, errs.ObjectNotFound
	}
	client := *base.TransferClientFor(d)
	if o.cover == "" {
		client.Transport = &recoveryTransport{driver: d, base: client.Transport, workID: o.workID, hash: o.node.Hash, nodePath: o.Path}
	}
	return &model.Link{URL: linkURL, HTTPClient: &client, Expiration: &expiry}, nil
}
func mediaURL(n track) string {
	if n.Stream != "" {
		return n.Stream
	}
	return n.Download
}

var _ driver.Driver = (*ASMRONE)(nil)
var _ driver.Getter = (*ASMRONE)(nil)
var _ driver.CachedTree = (*ASMRONE)(nil)
var _ driver.ASMRONEManager = (*ASMRONE)(nil)

type recoveryTransport struct {
	driver         *ASMRONE
	base           http.RoundTripper
	workID         int64
	hash, nodePath string
}

func (t *recoveryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	transport := t.base
	if transport == nil {
		transport = http.DefaultTransport
	}
	res, err := transport.RoundTrip(req)
	if err != nil || (res.StatusCode != 403 && res.StatusCode != 404 && res.StatusCode != 410) {
		return res, err
	}
	d := t.driver
	key := strconv.FormatInt(t.workID, 10)
	nodes, refreshErr, _ := d.recoveryGroup.Do(key, func() ([]track, error) {
		d.mu.Lock()
		if time.Since(d.recoveryAt[t.workID]) < 5*time.Minute {
			d.mu.Unlock()
			return nil, errors.New("ASMR.ONE link recovery cooling down")
		}
		d.recoveryAt[t.workID] = time.Now()
		d.mu.Unlock()
		return d.loadTracks(req.Context(), t.workID, true, true)
	})
	if refreshErr != nil {
		return res, nil
	}
	var link string
	var find func([]track, string)
	find = func(children []track, parent string) {
		for _, n := range children {
			relative := path.Join(parent, n.Title)
			if (t.hash != "" && n.Hash == t.hash) || (t.hash == "" && strings.TrimPrefix(t.nodePath, "/"+strings.Split(strings.TrimPrefix(t.nodePath, "/"), "/")[0]+"/") == relative) {
				link = mediaURL(n)
			}
			if n.isDir() {
				find(n.Children, relative)
			}
		}
	}
	find(nodes, "")
	u, parseErr := url.Parse(link)
	if link == "" || parseErr != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return res, nil
	}
	res.Body.Close()
	retry := req.Clone(req.Context())
	retry.URL = u
	retry.Host = ""
	retry.Header = req.Header.Clone()
	retry.Header.Del("Authorization")
	retry.Header.Del("Cookie")
	return transport.RoundTrip(retry)
}
