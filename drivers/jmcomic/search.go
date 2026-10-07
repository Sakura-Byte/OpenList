package jmcomic

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func (d *JMComic) SearchCached(ctx context.Context, req model.SearchReq, emit func(model.SearchNode) error) error {
	ctx, done, err := d.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	req.Parent = path.Clean("/" + req.Parent)
	canonical := req.Parent == "/" || req.Parent == "/所有漫画" || strings.HasPrefix(req.Parent, "/所有漫画/")
	var tag *cachedTag
	if strings.HasPrefix(req.Parent, "/标签/") {
		parts := strings.Split(strings.TrimPrefix(req.Parent, "/标签/"), "/")
		v, e := d.store.tag(ctx, path.Join("/标签", parts[0]))
		if e == nil {
			tag = &v
		} else if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
	}
	add := func(p string, name string, dir bool, scopePath string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := model.SearchNode{Parent: p, Name: name, IsDir: dir}
		candidate := n
		if scopePath != "" {
			candidate.Parent = path.Dir(scopePath)
			n.AccessPath = scopePath
		}
		if driver.CachedSearchMatch(req, candidate) {
			return emit(n)
		}
		return nil
	}
	// Read in bounded batches so chapter lookups never hold the single SQLite
	// connection while album rows are open. Aliases select members, but emit only
	// canonical paths, avoiding duplicate works across recommendations and tags.
	last := ""
	dateDirs := map[string]bool{}
	for {
		// Reuse the existing ID primary-key index for keyset pagination. The
		// date index would sort all remaining albums again for every batch.
		query := `SELECT ` + albumColumns + ` FROM albums INDEXED BY sqlite_autoindex_albums_1 WHERE active=1 AND id>?`
		args := []any{last}
		if !canonical {
			query += ` AND (id IN (SELECT id FROM feed_members WHERE path=? OR substr(path,1,length(?))=? OR substr(?,1,length(path)+1)=path||'/')`
			prefix := strings.TrimRight(req.Parent, "/") + "/"
			args = append(args, req.Parent, prefix, prefix, req.Parent)
			if tag != nil {
				query += ` OR EXISTS(SELECT 1 FROM json_each(CASE WHEN detail='' THEN '{}' ELSE detail END,'$.tags') WHERE trim(value)=?)`
				args = append(args, tag.Name)
			}
			if req.Parent == "/标签" {
				query += ` OR EXISTS(SELECT 1 FROM json_each(CASE WHEN detail='' THEN '{}' ELSE detail END,'$.tags') WHERE type='text' AND trim(value)<>'')`
			}
			query += `)`
		}
		items, err := d.store.albums(ctx, query+` ORDER BY id LIMIT 100`, args...)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		for _, a := range items {
			last = string(a.Album.ID)
			p := a.Album.canonical()
			if canonical {
				for dir := path.Dir(p); dir != "/"; dir = path.Dir(dir) {
					if !dateDirs[dir] {
						dateDirs[dir] = true
						if err = add(path.Dir(dir), path.Base(dir), true, ""); err != nil {
							return err
						}
					}
				}
			}
			scopeRoot := ""
			if req.Parent == "/标签" && a.Detail != nil {
				for _, name := range a.Detail.Tags {
					if strings.TrimSpace(name) != "" {
						scopeRoot = path.Join(tagPath(name), a.Album.name())
						break
					}
				}
			}
			if tag != nil {
				scopeRoot = path.Join(tag.Path, a.Album.name())
				if req.Parent != tag.Path && req.Parent != scopeRoot && !strings.HasPrefix(req.Parent, scopeRoot+"/") {
					continue
				}
			} else if !canonical && scopeRoot == "" {
				// Find one matching alias for this ID, preserving the requested
				// album/chapter scope as well as membership in a feed group.
				rows, err := d.store.db.QueryContext(ctx, `SELECT path FROM feed_members WHERE id=? ORDER BY path`, last)
				if err != nil {
					return err
				}
				for rows.Next() {
					var feed string
					if err = rows.Scan(&feed); err != nil {
						rows.Close()
						return err
					}
					alias := path.Join(feed, a.Album.name())
					if req.Parent == feed || strings.HasPrefix(feed, req.Parent+"/") || req.Parent == alias || strings.HasPrefix(req.Parent, alias+"/") {
						scopeRoot = alias
						break
					}
				}
				err = rows.Err()
				rows.Close()
				if err != nil {
					return err
				}
				if scopeRoot == "" {
					continue
				}
			}
			if err = add(path.Dir(p), path.Base(p), true, scopeRoot); err != nil {
				return err
			}
			aliasChild := func(relative string) string {
				if scopeRoot == "" {
					return ""
				}
				return path.Join(scopeRoot, relative)
			}
			if err = add(p, "README.md", false, aliasChild("README.md")); err != nil {
				return err
			}
			if d.Mode == "server" && d.coverURL(a.Album) != "" {
				if err = add(p, "cover.jpg", false, aliasChild("cover.jpg")); err != nil {
					return err
				}
			}
			if a.Detail == nil {
				continue
			}
			for i, c := range a.Detail.Series {
				cp := path.Join(p, c.name(i))
				if err = add(p, c.name(i), true, aliasChild(c.name(i))); err != nil {
					return err
				}
				if err = add(cp, "README.md", false, aliasChild(path.Join(c.name(i), "README.md"))); err != nil {
					return err
				}
				if d.Mode != "server" {
					continue
				}
				tree, err := d.store.tree(ctx, string(c.ID))
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				for _, pg := range tree.Pages {
					if err = add(cp, pg.Name, false, aliasChild(path.Join(c.name(i), pg.Name))); err != nil {
						return err
					}
				}
			}
		}
	}
}

var _ driver.CachedSearcher = (*JMComic)(nil)
