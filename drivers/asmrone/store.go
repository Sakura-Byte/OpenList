package asmrone

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
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return nil, err
	}
	gdb, err := gorm.Open(openSQLite(filename), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, fmt.Errorf("open ASMR.ONE cache: %w", err)
	}
	db, err := gdb.DB()
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &cacheStore{db: db, filename: filename}
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS works (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, data TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, tracks TEXT NOT NULL DEFAULT '', tracks_at INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', retry_at INTEGER NOT NULL DEFAULT 0, seen TEXT NOT NULL DEFAULT '', refresh_requested INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), data TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS index_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, old_path TEXT NOT NULL, new_path TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, UNIQUE(old_path,new_path))`,
		`CREATE INDEX IF NOT EXISTS works_pending ON works(active,retry_at,tracks_at)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize ASMR.ONE cache: %w", err)
		}
	}
	return s, nil
}

func (s *cacheStore) loadState(ctx context.Context) (syncState, error) {
	var state syncState
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT data FROM state WHERE id=1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal([]byte(raw), &state)
	return state, err
}
func (s *cacheStore) saveState(ctx context.Context, state syncState) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO state(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data`, encode(state))
	return err
}

func scanWork(row interface{ Scan(...any) error }) (cachedWork, error) {
	var w cachedWork
	var raw, tracks string
	err := row.Scan(&w.DirectoryName, &raw, &w.Active, &tracks, &w.TracksAt, &w.Error, &w.RetryAt)
	if err != nil {
		return w, err
	}
	if err = json.Unmarshal([]byte(raw), &w.work); err != nil {
		return w, err
	}
	w.HasTracks = tracks != ""
	if w.HasTracks {
		err = json.Unmarshal([]byte(tracks), &w.Tracks)
	}
	return w, err
}

const workColumns = `name,data,active,tracks,tracks_at,error,retry_at`

func (s *cacheStore) byID(ctx context.Context, id int64) (cachedWork, error) {
	return scanWork(s.db.QueryRowContext(ctx, `SELECT `+workColumns+` FROM works WHERE id=?`, id))
}
func (s *cacheStore) byName(ctx context.Context, name string) (cachedWork, error) {
	return scanWork(s.db.QueryRowContext(ctx, `SELECT `+workColumns+` FROM works WHERE name=? AND active=1`, name))
}
func (s *cacheStore) works(ctx context.Context) ([]cachedWork, error) {
	// Root listings never deserialize every track tree.
	rows, err := s.db.QueryContext(ctx, `SELECT name,data,active,'',tracks_at,error,retry_at FROM works WHERE active=1 ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []cachedWork
	for rows.Next() {
		w, err := scanWork(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, w)
	}
	return result, rows.Err()
}
func enqueueIndex(ctx context.Context, tx *sql.Tx, oldPath, newPath string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO index_jobs(old_path,new_path) VALUES(?,?) ON CONFLICT(old_path,new_path) DO UPDATE SET id=(SELECT COALESCE(MAX(id),0)+1 FROM index_jobs),revision=index_jobs.revision+1`, oldPath, newPath)
	return err
}
func (s *cacheStore) upsertPage(ctx context.Context, works []work, generation string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, w := range works {
		var oldName, oldData string
		var active bool
		err := tx.QueryRowContext(ctx, `SELECT name,data,active FROM works WHERE id=?`, int64(w.ID)).Scan(&oldName, &oldData, &active)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		name := w.name()
		// Multiple editions can share a source_id and title.
		for seq := 0; ; seq++ {
			var conflict int64
			e := tx.QueryRowContext(ctx, `SELECT id FROM works WHERE name=? AND id<>?`, name, int64(w.ID)).Scan(&conflict)
			if errors.Is(e, sql.ErrNoRows) {
				break
			}
			if e != nil {
				return e
			}
			name = fmt.Sprintf("%s [%d]", w.name(), w.ID)
			if seq > 0 {
				name += fmt.Sprintf(" (%d)", seq+1)
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO works(id,name,data,seen) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,data=excluded.data,active=1,seen=CASE WHEN excluded.seen='' THEN works.seen ELSE excluded.seen END`, int64(w.ID), name, encode(w), generation)
		if err != nil {
			return err
		}
		if oldData != encode(w) || oldName != name || !active {
			oldPath := ""
			if oldName != "" {
				oldPath = "/" + oldName
			}
			if err := enqueueIndex(ctx, tx, oldPath, "/"+name); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
func (s *cacheStore) finishFull(ctx context.Context, generation string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT name FROM works WHERE active=1 AND seen<>?`, generation)
	if err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		names = append(names, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := enqueueIndex(ctx, tx, "/"+name, ""); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE works SET active=0 WHERE seen<>?`, generation); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *cacheStore) saveTracks(ctx context.Context, w cachedWork, nodes []track, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE works SET tracks=?,tracks_at=?,error='',retry_at=0,refresh_requested=0 WHERE id=?`, encode(nodes), now, int64(w.ID)); err != nil {
		return err
	}
	// Use the latest title if a catalog sync renamed this work during the fetch.
	var name string
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT name,active FROM works WHERE id=?`, int64(w.ID)).Scan(&name, &active); err != nil {
		return err
	}
	if active {
		if err := enqueueIndex(ctx, tx, "/"+name, "/"+name); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *cacheStore) failTracks(ctx context.Context, id int64, message string, retryAt int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE works SET error=?,retry_at=? WHERE id=?`, message, retryAt, id)
	return err
}
func (s *cacheStore) nextWork(ctx context.Context, now int64) (cachedWork, error) {
	return scanWork(s.db.QueryRowContext(ctx, `SELECT `+workColumns+` FROM works WHERE active=1 AND tracks='' AND retry_at<=? ORDER BY id DESC LIMIT 1`, now))
}
func (s *cacheStore) counts(ctx context.Context) (works, cached, pending, failed int64, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(tracks<>''),0),COALESCE(SUM(tracks=''),0),COALESCE(SUM(error<>''),0) FROM works WHERE active=1`).Scan(&works, &cached, &pending, &failed)
	return
}
func (s *cacheStore) nextIndexJob(ctx context.Context) (indexJob, error) {
	var j indexJob
	err := s.db.QueryRowContext(ctx, `SELECT id,old_path,new_path,revision FROM index_jobs ORDER BY id LIMIT 1`).Scan(&j.ID, &j.OldPath, &j.NewPath, &j.Revision)
	return j, err
}
func (s *cacheStore) cacheBytes() int64 {
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if st, err := os.Stat(s.filename + suffix); err == nil {
			total += st.Size()
		}
	}
	return total
}
