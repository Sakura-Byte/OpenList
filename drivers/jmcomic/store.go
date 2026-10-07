package jmcomic

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type cacheStore struct {
	db       *sql.DB
	filename string
}

func openStore(filename string) (*cacheStore, error) {
	if e := os.MkdirAll(filepath.Dir(filename), 0700); e != nil {
		return nil, e
	}
	g, e := gorm.Open(openSQLite(filename), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		return nil, fmt.Errorf("open JMComic cache: %w", e)
	}
	db, e := g.DB()
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	s := &cacheStore{db: db, filename: filename}
	for _, q := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS albums (id TEXT PRIMARY KEY,data TEXT NOT NULL,path TEXT NOT NULL UNIQUE,date TEXT NOT NULL,updated INTEGER NOT NULL,detail TEXT NOT NULL DEFAULT '',detail_version INTEGER NOT NULL DEFAULT -1,active INTEGER NOT NULL DEFAULT 1,seen TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',retry_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE INDEX IF NOT EXISTS album_dates ON albums(active,date)`,
		`CREATE TABLE IF NOT EXISTS chapters (id TEXT PRIMARY KEY,album_id TEXT NOT NULL,data TEXT NOT NULL,version INTEGER NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS chapter_albums ON chapters(album_id)`,
		`CREATE TABLE IF NOT EXISTS feeds (path TEXT PRIMARY KEY,updated INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS feed_members (path TEXT NOT NULL,id TEXT NOT NULL,position INTEGER NOT NULL,PRIMARY KEY(path,id))`,
		`CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1),data TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS tasks (kind TEXT NOT NULL,key TEXT NOT NULL,retry_at INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',manual INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(kind,key))`,
		`CREATE TABLE IF NOT EXISTS index_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT,old_path TEXT NOT NULL,new_path TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,UNIQUE(old_path,new_path))`,
	} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, fmt.Errorf("initialize JMComic cache: %w", e)
		}
	}
	var hasManual int
	if e := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='manual'`).Scan(&hasManual); e != nil {
		db.Close()
		return nil, e
	}
	if hasManual == 0 {
		if _, e := db.Exec(`ALTER TABLE tasks ADD COLUMN manual INTEGER NOT NULL DEFAULT 0`); e != nil {
			db.Close()
			return nil, e
		}
	}
	return s, nil
}
func enqueueIndex(ctx context.Context, tx *sql.Tx, old, new string) error {
	_, e := tx.ExecContext(ctx, `INSERT INTO index_jobs(old_path,new_path) VALUES(?,?) ON CONFLICT(old_path,new_path) DO UPDATE SET id=(SELECT COALESCE(MAX(id),0)+1 FROM index_jobs),revision=index_jobs.revision+1`, old, new)
	return e
}

const albumColumns = `data,detail,detail_version,active,seen,error,retry_at`

func scanAlbum(r interface{ Scan(...any) error }) (cachedAlbum, error) {
	var a cachedAlbum
	var raw, dt string
	e := r.Scan(&raw, &dt, &a.DetailVersion, &a.Active, &a.Seen, &a.Error, &a.RetryAt)
	if e != nil {
		return a, e
	}
	if e = json.Unmarshal([]byte(raw), &a.Album); e != nil {
		return a, e
	}
	if dt != "" {
		a.Detail = &detail{}
		e = json.Unmarshal([]byte(dt), a.Detail)
	}
	return a, e
}
func (s *cacheStore) album(ctx context.Context, key string) (cachedAlbum, error) {
	return scanAlbum(s.db.QueryRowContext(ctx, `SELECT `+albumColumns+` FROM albums WHERE id=?`, key))
}
func (s *cacheStore) byPath(ctx context.Context, p string) (cachedAlbum, error) {
	return scanAlbum(s.db.QueryRowContext(ctx, `SELECT `+albumColumns+` FROM albums WHERE path=? AND active=1`, p))
}
func (s *cacheStore) albums(ctx context.Context, query string, args ...any) ([]cachedAlbum, error) {
	r, e := s.db.QueryContext(ctx, query, args...)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	var out []cachedAlbum
	for r.Next() {
		a, e := scanAlbum(r)
		if e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, r.Err()
}
func (s *cacheStore) upsert(ctx context.Context, items []album, generation string) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, a := range items {
		if !validID(string(a.ID)) || a.Name == "" {
			return errors.New("invalid JMComic album summary")
		}
		var raw, oldPath string
		var active bool
		e := tx.QueryRowContext(ctx, `SELECT data,path,active FROM albums WHERE id=?`, string(a.ID)).Scan(&raw, &oldPath, &active)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if raw != "" {
			var old album
			if e = json.Unmarshal([]byte(raw), &old); e != nil {
				return e
			}
			if a.Date == "" {
				a.Date = old.Date
			}
			if a.Updated == 0 {
				a.Updated = old.Updated
			}
			if a.Image == "" {
				a.Image = old.Image
			}
			if a.Author == "" {
				a.Author = old.Author
			}
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO albums(id,data,path,date,updated,seen) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data,path=excluded.path,date=excluded.date,updated=excluded.updated,active=1,seen=CASE WHEN excluded.seen='' THEN albums.seen ELSE excluded.seen END`, string(a.ID), encode(a), a.canonical(), a.Date, int64(a.Updated), generation); e != nil {
			return e
		}
		if oldPath != a.canonical() || raw != encode(a) || !active {
			if e = enqueueIndex(ctx, tx, oldPath, a.canonical()); e != nil {
				return e
			}
		}
	}
	return tx.Commit()
}
func (s *cacheStore) saveDetail(ctx context.Context, a cachedAlbum, dt detail) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, `UPDATE albums SET detail=?,detail_version=?,error='',retry_at=0 WHERE id=?`, encode(dt), int64(a.Album.Updated), string(a.Album.ID)); e != nil {
		return e
	}
	if e = enqueueCurrentIndex(ctx, tx, string(a.Album.ID)); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *cacheStore) tree(ctx context.Context, key string) (chapterTree, error) {
	var raw string
	var version int64
	e := s.db.QueryRowContext(ctx, `SELECT data,version FROM chapters WHERE id=?`, key).Scan(&raw, &version)
	var t chapterTree
	if e == nil {
		e = json.Unmarshal([]byte(raw), &t)
		t.Version = version
	}
	return t, e
}
func (s *cacheStore) saveTree(ctx context.Context, a album, t chapterTree) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, `INSERT INTO chapters(id,album_id,data,version) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET album_id=excluded.album_id,data=excluded.data,version=excluded.version`, t.ID, string(a.ID), encode(t), t.Version); e != nil {
		return e
	}
	if e = enqueueCurrentIndex(ctx, tx, string(a.ID)); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *cacheStore) hide(ctx context.Context, a album) error {
	return s.hideBatch(ctx, []album{a})
}
func (s *cacheStore) hideBatch(ctx context.Context, albums []album) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, a := range albums {
		if _, e = tx.ExecContext(ctx, `UPDATE albums SET active=0 WHERE id=?`, string(a.ID)); e != nil {
			return e
		}
		if e = enqueueCurrentIndex(ctx, tx, string(a.ID)); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *cacheStore) saveFeed(ctx context.Context, p string, items []album, now int64) error {
	if e := s.upsert(ctx, items, ""); e != nil {
		return e
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, `DELETE FROM feed_members WHERE path=?`, p); e != nil {
		return e
	}
	for i, a := range items {
		if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO feed_members(path,id,position) VALUES(?,?,?)`, p, string(a.ID), i); e != nil {
			return e
		}
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO feeds(path,updated) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET updated=excluded.updated`, p, now); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *cacheStore) loadState(ctx context.Context) (syncState, error) {
	var raw string
	var state syncState
	e := s.db.QueryRowContext(ctx, `SELECT data FROM state WHERE id=1`).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return state, nil
	}
	if e == nil {
		e = json.Unmarshal([]byte(raw), &state)
	}
	return state, e
}
func (s *cacheStore) saveState(ctx context.Context, state syncState) error {
	_, e := s.db.ExecContext(ctx, `INSERT INTO state(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data`, encode(state))
	return e
}
func (s *cacheStore) queue(ctx context.Context, kind, key string) error {
	_, e := s.db.ExecContext(ctx, `INSERT INTO tasks(kind,key) VALUES(?,?) ON CONFLICT(kind,key) DO NOTHING`, kind, key)
	return e
}
func (s *cacheStore) queueManual(ctx context.Context, kind, key string) error {
	_, e := s.db.ExecContext(ctx, `INSERT INTO tasks(kind,key,manual) VALUES(?,?,1) ON CONFLICT(kind,key) DO UPDATE SET manual=1,retry_at=0`, kind, key)
	return e
}
func enqueueCurrentIndex(ctx context.Context, tx *sql.Tx, key string) error {
	var p string
	var active bool
	if e := tx.QueryRowContext(ctx, `SELECT path,active FROM albums WHERE id=?`, key).Scan(&p, &active); e != nil {
		return e
	}
	next := p
	if !active {
		next = ""
	}
	return enqueueIndex(ctx, tx, p, next)
}
func (s *cacheStore) reindexCachedWorks(ctx context.Context) error {
	_, e := s.db.ExecContext(ctx, `INSERT INTO index_jobs(old_path,new_path) SELECT path,path FROM albums WHERE active=1 AND detail<>'' ON CONFLICT(old_path,new_path) DO UPDATE SET id=(SELECT COALESCE(MAX(id),0)+1 FROM index_jobs),revision=index_jobs.revision+1`)
	return e
}
func (s *cacheStore) cacheBytes() int64 {
	var n int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if st, e := os.Stat(s.filename + suffix); e == nil {
			n += st.Size()
		}
	}
	return n
}
