package jmcomic

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func (d *JMComic) recordFailure(ctx context.Context, kind, key string, e error) {
	if ctx.Err() != nil {
		return
	}
	d.store.queue(ctx, kind, key)
	d.store.db.ExecContext(ctx, `UPDATE tasks SET error=?,retry_at=?,manual=0 WHERE kind=? AND key=?`, e.Error(), time.Now().Add(time.Hour).Unix(), kind, key)
	d.mu.Lock()
	d.state.Error = e.Error()
	d.store.saveState(ctx, d.state)
	d.mu.Unlock()
}
func sleepContext(ctx context.Context, t time.Duration) bool {
	timer := time.NewTimer(t)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (d *JMComic) catalogLoop() {
	defer d.wg.Done()
	for d.ctx.Err() == nil {
		d.mu.Lock()
		now := time.Now().Unix()
		mode := d.state.Requested
		if mode == "" {
			if !d.state.FullCompleted || now >= d.state.LastFull+int64(d.FullSyncDays)*86400 {
				mode = "full"
			} else if now >= d.state.LastSync+int64(d.SyncHours)*3600 {
				mode = "incremental"
			}
		}
		if d.state.Paused || now < d.state.RetryAt || mode == "" {
			d.mu.Unlock()
			if !sleepContext(d.ctx, time.Second) {
				return
			}
			continue
		}
		d.state.Requested = mode
		d.store.saveState(d.ctx, d.state)
		c, cancel := context.WithCancel(d.ctx)
		d.backgroundCancel = cancel
		d.phase = mode
		d.pages = 0
		watermark := d.state.Watermark
		d.mu.Unlock()
		next, e := d.syncCatalog(c, mode == "full", watermark)
		cancel()
		d.mu.Lock()
		d.backgroundCancel = nil
		d.phase = "idle"
		d.current = ""
		if e == nil {
			d.state.LastSync = time.Now().Unix()
			d.state.Watermark = max(d.state.Watermark, next)
			d.state.RetryAt = 0
			if mode == "full" {
				d.state.LastFull = d.state.LastSync
				d.state.FullCompleted = true
			}
			if d.state.Requested == mode {
				d.state.Requested = ""
			}
		} else if c.Err() == nil || !errors.Is(e, context.Canceled) {
			d.state.Error = e.Error()
			d.state.RetryAt = time.Now().Add(time.Hour).Unix()
		}
		d.store.saveState(d.ctx, d.state)
		d.mu.Unlock()
	}
}
func (d *JMComic) syncCatalog(ctx context.Context, full bool, watermark int64) (int64, error) {
	generation := strconv.FormatInt(time.Now().UnixNano(), 10)
	signatures := map[string]bool{}
	maxUpdate := watermark
	cutoff := (watermark+8*3600)/86400*86400 - 8*3600 // Re-read the upstream's UTC+8 calendar day.
	for n := 1; n <= 100000; n++ {
		items, e := d.catalogPage(ctx, n, "0", "mr", false)
		if e != nil {
			return watermark, e
		}
		d.mu.Lock()
		d.pages = n
		d.current = fmt.Sprintf("page %d", n)
		d.mu.Unlock()
		if len(items) == 0 {
			if n == 1 {
				return watermark, errors.New("JMComic catalog unexpectedly empty")
			}
			if full {
				if e = d.confirmMissing(ctx, generation); e != nil {
					return watermark, e
				}
			}
			return maxUpdate, nil
		}
		seen := map[id]bool{}
		var ids []string
		newCount := 0
		older := watermark > 0
		for _, a := range items {
			if !validID(string(a.ID)) || a.Name == "" || seen[a.ID] {
				return watermark, errors.New("invalid or duplicate JMComic catalog page")
			}
			seen[a.ID] = true
			ids = append(ids, string(a.ID))
			old, e := d.store.album(ctx, string(a.ID))
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return watermark, e
			}
			if old.Seen != generation {
				newCount++
			}
			maxUpdate = max(maxUpdate, int64(a.Updated))
			if int64(a.Updated) >= cutoff || a.Updated == 0 {
				older = false
			}
		}
		signature := digest(strings.Join(ids, ","))
		if signatures[signature] || newCount == 0 {
			return watermark, errors.New("JMComic catalog pagination made no progress")
		}
		signatures[signature] = true
		if e = d.store.upsert(ctx, items, generation); e != nil {
			return watermark, e
		}
		if !full && older {
			return maxUpdate, nil
		}
	}
	return watermark, errors.New("JMComic catalog exceeded pagination safety limit")
}
func (d *JMComic) confirmMissing(ctx context.Context, generation string) error {
	missing, e := d.store.albums(ctx, `SELECT `+albumColumns+` FROM albums WHERE active=1 AND seen<>?`, generation)
	if e != nil {
		return e
	}
	var removed []album
	for _, a := range missing {
		dt, e := d.fetchDetail(ctx, string(a.Album.ID), false)
		if unavailable(e) {
			removed = append(removed, a.Album)
		} else if e != nil {
			return e
		} else if e = d.store.saveDetail(ctx, a, dt); e != nil {
			return e
		}
	}
	return d.store.hideBatch(ctx, removed)
}

type section struct {
	ID      id      `json:"id"`
	Title   string  `json:"title"`
	Name    string  `json:"name"`
	Type    string  `json:"type"`
	Slug    string  `json:"slug"`
	Filter  id      `json:"filter_val"`
	Content []album `json:"content"`
}

func parseAlbumPage(b []byte) ([]album, error) {
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(b, &fields); e != nil {
		var list []album
		if e = json.Unmarshal(b, &list); e != nil {
			return nil, e
		}
		if list == nil {
			return nil, errors.New("missing JMComic feed list")
		}
		return list, nil
	}
	for _, key := range []string{"content", "list", "albums", "photos"} {
		if raw, ok := fields[key]; ok {
			var list []album
			if e := json.Unmarshal(raw, &list); e != nil {
				return nil, e
			}
			if list == nil {
				return nil, errors.New("null JMComic feed list")
			}
			return list, nil
		}
	}
	return nil, errors.New("missing JMComic feed content")
}
func (d *JMComic) pagedFeed(ctx context.Context, route string, q url.Values) ([]album, error) {
	var out []album
	seen := map[id]bool{}
	for n := 1; len(out) < d.FeedLimit; n++ {
		q.Set("page", strconv.Itoa(n))
		b, e := d.request(ctx, route, q, false, false)
		if e != nil {
			return nil, e
		}
		items, e := parseAlbumPage(b)
		if e != nil {
			return nil, e
		}
		if len(items) == 0 {
			return out, nil
		}
		added := 0
		for _, a := range items {
			if !validID(string(a.ID)) || a.Name == "" {
				return nil, errors.New("invalid JMComic feed album")
			}
			if seen[a.ID] {
				continue
			}
			seen[a.ID] = true
			out = append(out, a)
			added++
			if len(out) == d.FeedLimit {
				return out, nil
			}
		}
		if added == 0 {
			return nil, errors.New("JMComic feed pagination repeated")
		}
	}
	return out, nil
}
func (d *JMComic) syncFeeds(ctx context.Context) error {
	now := time.Now().Unix()
	var raw json.RawMessage
	if e := d.json(ctx, "/promote", url.Values{"_": {strconv.FormatInt(time.Now().UnixMilli(), 10)}}, false, &raw); e != nil {
		return e
	}
	var sections []section
	if e := json.Unmarshal(raw, &sections); e != nil {
		var one section
		if e = json.Unmarshal(raw, &one); e != nil {
			return e
		}
		sections = []section{one}
	}
	var today []album
	seen := map[id]bool{}
	for _, s := range sections {
		for _, a := range s.Content {
			if !seen[a.ID] && len(today) < d.FeedLimit {
				seen[a.ID] = true
				today = append(today, a)
			}
		}
	}
	if e := d.store.saveFeed(ctx, "/今日推荐", today, now); e != nil {
		return e
	}
	for _, s := range sections {
		title := s.Title
		if title == "" {
			title = s.Name
		}
		if title == "" {
			continue
		}
		p := "/推荐/" + safeName(title) + " [" + safeName(string(s.ID)) + "]"
		var route string
		var q url.Values
		switch s.Type {
		case "promote":
			key := s.ID
			if key == "" {
				key = s.Filter
			}
			if key == "" {
				continue
			}
			route = "/promote_list"
			q = url.Values{"id": {string(key)}}
		case "category_id":
			if s.Slug == "" {
				continue
			}
			route = "/categories/filter"
			q = url.Values{"c": {s.Slug}, "order": {""}, "o": {"mr"}}
		case "not_in_category_id":
			query := s.Slug
			if query == "" {
				query = title
			}
			route = "/search"
			q = url.Values{"search_query": {query}, "main_tag": {"0"}, "o": {"mr"}, "t": {"a"}}
		default:
			continue
		}
		d.mu.Lock()
		d.feedCurrent = p
		d.mu.Unlock()
		items, e := d.pagedFeed(ctx, route, q)
		if e != nil {
			return e
		}
		if e = d.store.saveFeed(ctx, p, items, now); e != nil {
			return e
		}
	}
	filters := []struct{ path, category, order string }{{"/最近更新", "0", "mr"}, {"/排行榜/日榜", "0", "mv_t"}, {"/排行榜/周榜", "0", "mv_w"}, {"/排行榜/月榜", "0", "mv_m"}, {"/排行榜/总榜", "0", "mv"}, {"/分类/同人", "doujin", "mr"}, {"/分类/单本", "single", "mr"}, {"/分类/短篇", "short", "mr"}, {"/分类/其他", "another", "mr"}, {"/分类/韩漫", "hanman", "mr"}, {"/分类/美漫", "meiman", "mr"}, {"/分类/Cosplay", "doujin_cosplay", "mr"}, {"/分类/3D", "3D", "mr"}, {"/分类/英文", "english_site", "mr"}}
	for _, f := range filters {
		d.mu.Lock()
		d.feedCurrent = f.path
		d.mu.Unlock()
		items, e := d.pagedFeed(ctx, "/categories/filter", url.Values{"c": {f.category}, "o": {f.order}, "order": {""}})
		if e != nil {
			return e
		}
		if e = d.store.saveFeed(ctx, f.path, items, now); e != nil {
			return e
		}
	}
	for _, tag := range strings.Split(strings.ReplaceAll(d.Tags, "\r", ""), "\n") {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		p := "/标签/" + safeName(tag)
		d.mu.Lock()
		d.feedCurrent = p
		d.mu.Unlock()
		items, e := d.pagedFeed(ctx, "/search", url.Values{"search_query": {tag}, "main_tag": {"3"}, "o": {"mr"}, "t": {"a"}})
		if e != nil {
			return e
		}
		if e = d.store.saveFeed(ctx, p, items, now); e != nil {
			return e
		}
	}
	// Replace the set of dynamic entry names only after a complete successful run.
	_, e := d.store.db.ExecContext(ctx, `DELETE FROM feed_members WHERE path IN (SELECT path FROM feeds WHERE updated<>?)`, now)
	if e != nil {
		return e
	}
	_, e = d.store.db.ExecContext(ctx, `DELETE FROM feeds WHERE updated<>?`, now)
	return e
}
func (d *JMComic) feedLoop() {
	defer d.wg.Done()
	for d.ctx.Err() == nil {
		d.mu.Lock()
		now := time.Now().Unix()
		if d.state.Paused || now < d.state.FeedRetryAt || (!d.state.FeedRequested && now < d.state.LastFeeds+int64(d.FeedHours)*3600) {
			d.mu.Unlock()
			if !sleepContext(d.ctx, time.Second) {
				return
			}
			continue
		}
		d.state.FeedRequested = true
		d.store.saveState(d.ctx, d.state)
		c, cancel := context.WithCancel(d.ctx)
		d.feedCancel = cancel
		d.mu.Unlock()
		e := d.syncFeeds(c)
		cancel()
		d.mu.Lock()
		d.feedCancel = nil
		d.feedCurrent = ""
		if e == nil {
			d.state.LastFeeds = time.Now().Unix()
			d.state.FeedRequested = false
			d.state.FeedRetryAt = 0
		} else if !errors.Is(e, context.Canceled) {
			d.state.Error = e.Error()
			d.state.FeedRetryAt = time.Now().Add(time.Hour).Unix()
		}
		d.store.saveState(d.ctx, d.state)
		d.mu.Unlock()
	}
}
func (d *JMComic) taskLoop() {
	defer d.wg.Done()
	for d.ctx.Err() == nil {
		var kind, key string
		d.mu.Lock()
		paused := d.state.Paused
		d.mu.Unlock()
		var manual bool
		e := d.store.db.QueryRowContext(d.ctx, `SELECT kind,key,manual FROM tasks WHERE retry_at<=? AND (?=0 OR manual=1) ORDER BY manual DESC,retry_at,kind,key LIMIT 1`, time.Now().Unix(), paused).Scan(&kind, &key, &manual)
		if e != nil {
			if !sleepContext(d.ctx, time.Second) {
				return
			}
			continue
		}
		d.mu.Lock()
		d.manualCurrent = kind + " " + key
		d.mu.Unlock()
		switch kind {
		case "album":
			var a cachedAlbum
			a, e = d.store.album(d.ctx, key)
			if e == nil {
				_, e = d.loadDetail(d.ctx, a, true, manual)
			}
		case "chapter":
			parts := strings.Split(key, "/")
			if len(parts) != 2 {
				e = errors.New("invalid JMComic chapter task")
				break
			}
			var a cachedAlbum
			a, e = d.store.album(d.ctx, parts[0])
			if e == nil {
				_, e = d.loadTree(d.ctx, a, parts[1], true, manual)
			}
		default:
			e = errors.New("unknown JMComic task")
		}
		if e == nil {
			d.store.db.ExecContext(d.ctx, `DELETE FROM tasks WHERE kind=? AND key=?`, kind, key)
		} else {
			d.recordFailure(d.ctx, kind, key, e)
		}
		d.mu.Lock()
		d.manualCurrent = ""
		d.mu.Unlock()
	}
}
func (d *JMComic) indexLoop() {
	defer d.wg.Done()
	for d.ctx.Err() == nil {
		var seq, revision int64
		var old, new string
		e := d.store.db.QueryRowContext(d.ctx, `SELECT id,old_path,new_path,revision FROM index_jobs ORDER BY id LIMIT 1`).Scan(&seq, &old, &new, &revision)
		if e == nil {
			c, cancel := context.WithTimeout(d.ctx, 30*time.Second)
			ok, err := op.HandleCachedTreeUpdate(driver.WithCachedTraversal(c), d, old, new)
			cancel()
			if ok && err == nil {
				d.store.db.ExecContext(d.ctx, `DELETE FROM index_jobs WHERE id=? AND revision=?`, seq, revision)
				continue
			}
		}
		if !sleepContext(d.ctx, 2*time.Second) {
			return
		}
	}
}
func (d *JMComic) JMComicStatus(ctx context.Context) (driver.JMComicStatus, error) {
	c, done, e := d.begin(ctx)
	if e != nil {
		return driver.JMComicStatus{}, e
	}
	defer done()
	d.mu.Lock()
	s := d.state
	r := driver.JMComicStatus{Phase: d.phase, Paused: s.Paused, Pages: d.pages, CurrentTask: strings.TrimSpace(d.current + " " + d.feedCurrent + " " + d.manualCurrent), LastSync: s.LastSync, LastFullSync: s.LastFull, LastFeeds: s.LastFeeds, NextSync: max(s.LastSync+int64(d.SyncHours)*3600, s.RetryAt), NextFullSync: max(s.LastFull+int64(d.FullSyncDays)*86400, s.RetryAt), NextFeeds: max(s.LastFeeds+int64(d.FeedHours)*3600, s.FeedRetryAt), LastError: s.Error}
	if s.LastSync == 0 {
		r.NextSync = max(time.Now().Unix(), s.RetryAt)
	}
	if s.LastFull == 0 {
		r.NextFullSync = max(time.Now().Unix(), s.RetryAt)
	}
	if s.LastFeeds == 0 {
		r.NextFeeds = max(time.Now().Unix(), s.FeedRetryAt)
	}
	d.mu.Unlock()
	for _, v := range []struct {
		p *int64
		q string
	}{{&r.Works, `SELECT COUNT(*) FROM albums WHERE active=1`}, {&r.Cached, `SELECT COUNT(*) FROM albums WHERE active=1 AND detail<>''`}, {&r.Chapters, `SELECT COUNT(*) FROM chapters`}, {&r.Pending, `SELECT COUNT(*) FROM tasks WHERE error=''`}, {&r.Failed, `SELECT COUNT(*) FROM tasks WHERE error<>''`}} {
		if e = d.store.db.QueryRowContext(c, v.q).Scan(v.p); e != nil {
			return r, e
		}
	}
	r.CacheBytes = d.store.cacheBytes()
	return r, nil
}
func (d *JMComic) JMComicAction(ctx context.Context, action, key string) error {
	c, done, e := d.begin(ctx)
	if e != nil {
		return e
	}
	defer done()
	d.mu.Lock()
	defer d.mu.Unlock()
	switch action {
	case "pause":
		d.state.Paused = true
		if d.backgroundCancel != nil {
			d.backgroundCancel()
		}
		if d.feedCancel != nil {
			d.feedCancel()
		}
	case "resume":
		d.state.Paused = false
	case "incremental", "full":
		if d.phase != action && d.state.Requested != "full" {
			d.state.Requested = action
		}
		if !d.state.FullCompleted {
			d.state.Requested = "full"
		}
		d.state.RetryAt = 0
	case "feeds":
		d.state.FeedRequested = true
		d.state.FeedRetryAt = 0
	case "retry":
		if _, e = d.store.db.ExecContext(c, `UPDATE tasks SET retry_at=0,manual=1`); e != nil {
			return e
		}
		d.state.RetryAt = 0
		d.state.FeedRetryAt = 0
	case "refresh":
		key = strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(key)), "JM")
		if !validID(key) {
			return errors.New("invalid JMComic work ID")
		}
		a, e := d.store.album(c, key)
		if e != nil || !a.Active {
			return errors.New("active JMComic work not found")
		}
		if e = d.store.queueManual(c, "album", key); e != nil {
			return e
		}
		if _, e = d.store.db.ExecContext(c, `UPDATE tasks SET retry_at=0 WHERE kind='album' AND key=?`, key); e != nil {
			return e
		}
	default:
		return errors.New("unsupported JMComic action")
	}
	return d.store.saveState(c, d.state)
}

var _ driver.JMComicManager = (*JMComic)(nil)
