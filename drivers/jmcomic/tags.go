package jmcomic

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type cachedTag struct {
	Name, Path string
	Count      int64
}

func tagPath(name string) string {
	name = strings.TrimSpace(name)
	clean := safeName(name)
	if clean != name {
		sum := sha256.Sum256([]byte(name))
		clean += fmt.Sprintf(" [%x]", sum[:4])
	}
	return path.Join("/标签", clean)
}
func tagQuery(name string) url.Values {
	return url.Values{"search_query": {name}, "main_tag": {"3"}, "o": {"mr"}, "t": {"a"}}
}
func (s *cacheStore) saveTagCatalog(ctx context.Context, names []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO tags(name,path) VALUES(?,?)`, name, tagPath(name)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Counts are aggregated from the existing detail JSON and feed memberships;
// neither flattened search records nor duplicate per-work tags are stored.
func (s *cacheStore) tags(ctx context.Context) ([]cachedTag, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name,COUNT(DISTINCT id) FROM (
	 SELECT name,NULL id FROM tags
	 UNION ALL SELECT trim(j.value),a.id FROM albums a,json_each(CASE WHEN a.detail='' THEN '{}' ELSE a.detail END,'$.tags') j WHERE a.active=1 AND j.type='text' AND trim(j.value)<>''
	 UNION ALL SELECT t.name,m.id FROM tags t JOIN feed_members m ON m.path=t.path JOIN albums a ON a.id=m.id WHERE a.active=1
	) GROUP BY name ORDER BY COUNT(DISTINCT id) DESC,name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []cachedTag
	for rows.Next() {
		var t cachedTag
		if err = rows.Scan(&t.Name, &t.Count); err != nil {
			return nil, err
		}
		t.Path = tagPath(t.Name)
		tags = append(tags, t)
	}
	return tags, rows.Err()
}
func (s *cacheStore) tag(ctx context.Context, p string) (cachedTag, error) {
	tags, err := s.tags(ctx)
	if err != nil {
		return cachedTag{}, err
	}
	for _, tag := range tags {
		if tag.Path == p {
			return tag, nil
		}
	}
	return cachedTag{}, sql.ErrNoRows
}
func (s *cacheStore) tagAlbums(ctx context.Context, t cachedTag) ([]cachedAlbum, error) {
	return s.albums(ctx, `SELECT `+albumColumns+` FROM albums WHERE active=1 AND (
	 id IN (SELECT id FROM feed_members WHERE path=?) OR
	 EXISTS(SELECT 1 FROM json_each(CASE WHEN detail='' THEN '{}' ELSE detail END,'$.tags') WHERE trim(value)=?)
	) ORDER BY updated DESC,CAST(id AS INTEGER) DESC`, t.Path, t.Name)
}
func (d *JMComic) syncTagCatalog(ctx context.Context) error {
	return d.fetchTagCatalog(ctx, false)
}
func (d *JMComic) fetchTagCatalog(ctx context.Context, high bool) error {
	var data struct {
		Blocks []struct {
			Content []string `json:"content"`
		} `json:"blocks"`
	}
	if err := d.json(ctx, "/categories", nil, high, &data); err != nil {
		return err
	}
	if data.Blocks == nil {
		return errors.New("JMComic categories response has no tag blocks")
	}
	var names []string
	for _, b := range data.Blocks {
		names = append(names, b.Content...)
	}
	if err := d.store.saveTagCatalog(ctx, names); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state.LastTags = time.Now().Unix()
	return d.store.saveState(ctx, d.state)
}
func (d *JMComic) loadTagCatalog(ctx context.Context) error {
	d.mu.Lock()
	ready := d.state.LastTags != 0
	d.mu.Unlock()
	if ready {
		return nil
	}
	var message string
	var retry int64
	if d.store.db.QueryRowContext(ctx, `SELECT error,retry_at FROM tasks WHERE kind='tag_catalog' AND key=''`).Scan(&message, &retry) == nil && retry > time.Now().Unix() {
		return errors.New(message)
	}
	ch := d.catalogGroup.DoChan("tags", func() (bool, error) {
		c, done, e := d.begin(d.ctx)
		if e != nil {
			return false, e
		}
		defer done()
		e = d.fetchTagCatalog(c, true)
		if e != nil {
			d.recordFailure(c, "tag_catalog", "", e)
		} else {
			d.store.db.ExecContext(c, `DELETE FROM tasks WHERE kind='tag_catalog' AND key=''`)
		}
		return e == nil, e
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		return r.Err
	}
}
func (d *JMComic) tagFolders(ctx context.Context) ([]model.Obj, error) {
	fetchError := d.loadTagCatalog(ctx)
	tags, err := d.store.tags(ctx)
	if err != nil {
		return nil, err
	}
	if fetchError != nil && len(tags) == 0 {
		return nil, fetchError
	}
	var out []model.Obj
	for _, t := range tags {
		o := folder(t.Path)
		o.tag = &model.TagInfo{Name: t.Name, Count: t.Count}
		out = append(out, o)
	}
	return out, nil
}
func (d *JMComic) ensureTagFeed(ctx context.Context, name string, force, high bool) error {
	if !force {
		var updated int64
		err := d.store.db.QueryRowContext(ctx, `SELECT updated FROM feeds WHERE path=?`, tagPath(name)).Scan(&updated)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	ch := d.tagGroup.DoChan(name, func() (bool, error) {
		c, done, err := d.begin(d.ctx)
		if err != nil {
			return false, err
		}
		defer done()
		p := tagPath(name)
		if !force {
			var updated int64
			err = d.store.db.QueryRowContext(c, `SELECT updated FROM feeds WHERE path=?`, p).Scan(&updated)
			if err == nil {
				return true, nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
			var message string
			var retry int64
			if d.store.db.QueryRowContext(c, `SELECT error,retry_at FROM tasks WHERE kind='tag' AND key=?`, name).Scan(&message, &retry) == nil && retry > time.Now().Unix() {
				return false, errors.New(message)
			}
		}
		items, err := d.pagedFeedPriority(c, "/search", tagQuery(name), high)
		if err == nil {
			err = d.store.saveTagCatalog(c, []string{name})
		}
		if err == nil {
			err = d.store.saveFeed(c, p, items, time.Now().Unix())
		}
		if err != nil {
			d.recordFailure(c, "tag", name, err)
			return false, err
		}
		d.store.db.ExecContext(c, `DELETE FROM tasks WHERE kind='tag' AND key=?`, name)
		return true, nil
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-ch:
		return result.Err
	}
}
func (s *cacheStore) visitedTags(ctx context.Context) ([]string, error) {
	tags, err := s.tags(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT path FROM feeds WHERE substr(path,1,length('/标签/'))='/标签/'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	visited := map[string]bool{}
	for rows.Next() {
		var p string
		if err = rows.Scan(&p); err != nil {
			return nil, err
		}
		visited[p] = true
	}
	var names []string
	for _, t := range tags {
		if visited[t.Path] {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names, rows.Err()
}
