package asmrone

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
		`CREATE TABLE IF NOT EXISTS works (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, data TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, tracks TEXT NOT NULL DEFAULT '', tracks_at INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', retry_at INTEGER NOT NULL DEFAULT 0, seen TEXT NOT NULL DEFAULT '', refresh_requested INTEGER NOT NULL DEFAULT 0, range_name TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), data TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS work_changes (work_id INTEGER PRIMARY KEY, modified_at INTEGER NOT NULL)`,
		`DROP TABLE IF EXISTS index_jobs`,
		`CREATE INDEX IF NOT EXISTS works_pending ON works(active,retry_at,tracks_at)`,
		`CREATE INDEX IF NOT EXISTS works_range ON works(active,range_name)`,
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
	err := row.Scan(&w.DirectoryName, &raw, &w.Active, &tracks, &w.TracksAt, &w.Error, &w.RetryAt, &w.ModifiedAt)
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

const workModifiedColumn = `COALESCE((SELECT modified_at FROM work_changes WHERE work_id=works.id),0)`
const workColumns = `name,data,active,tracks,tracks_at,error,retry_at,` + workModifiedColumn

func (s *cacheStore) byID(ctx context.Context, id int64) (cachedWork, error) {
	return scanWork(s.db.QueryRowContext(ctx, `SELECT `+workColumns+` FROM works WHERE id=?`, id))
}
func (s *cacheStore) byName(ctx context.Context, name string) (cachedWork, error) {
	return scanWork(s.db.QueryRowContext(ctx, `SELECT `+workColumns+` FROM works WHERE name=? AND active=1`, name))
}
func (s *cacheStore) works(ctx context.Context) ([]cachedWork, error) {
	return s.worksInRange(ctx, "")
}
func (s *cacheStore) worksInRange(ctx context.Context, rangeName string) ([]cachedWork, error) {
	// Root listings never deserialize every track tree.
	query := `SELECT name,data,active,'',tracks_at,error,retry_at,` + workModifiedColumn + ` FROM works WHERE active=1`
	var args []any
	if rangeName != "" {
		query += ` AND range_name=?`
		args = append(args, rangeName)
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY id DESC`, args...)
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
func (s *cacheStore) ranges(ctx context.Context) ([]rangeInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT range_name,MAX(MAX(`+workModifiedColumn+`,tracks_at*1000)) FROM works WHERE active=1 GROUP BY range_name ORDER BY length(range_name),range_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []rangeInfo
	for rows.Next() {
		var info rangeInfo
		if err := rows.Scan(&info.Name, &info.ModifiedAt); err != nil {
			return nil, err
		}
		result = append(result, info)
	}
	return result, rows.Err()
}
func (s *cacheStore) rangeByName(ctx context.Context, name string) (rangeInfo, error) {
	var info rangeInfo
	err := s.db.QueryRowContext(ctx, `SELECT range_name,MAX(MAX(`+workModifiedColumn+`,tracks_at*1000)) FROM works WHERE active=1 AND range_name=? GROUP BY range_name`, name).Scan(&info.Name, &info.ModifiedAt)
	return info, err
}
func saveModified(ctx context.Context, tx *sql.Tx, id, millis int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO work_changes(work_id,modified_at) VALUES(?,?) ON CONFLICT(work_id) DO UPDATE SET modified_at=MAX(work_changes.modified_at,excluded.modified_at)`, id, millis)
	return err
}
func (s *cacheStore) upsertPage(ctx context.Context, works []work, generation string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
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
		_, err = tx.ExecContext(ctx, `INSERT INTO works(id,name,data,seen,range_name) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,data=excluded.data,range_name=excluded.range_name,active=1,seen=CASE WHEN excluded.seen='' THEN works.seen ELSE excluded.seen END`, int64(w.ID), name, encode(w), generation, w.rangeName())
		if err != nil {
			return err
		}
		if oldData != encode(w) || oldName != name || !active {
			if err := saveModified(ctx, tx, int64(w.ID), now); err != nil {
				return err
			}
		} else {
			// Start tracking older cache entries on their next successful save.
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO work_changes(work_id,modified_at) VALUES(?,?)`, int64(w.ID), now); err != nil {
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
	if err := saveModified(ctx, tx, int64(w.ID), max(now*1000, time.Now().UnixMilli())); err != nil {
		return err
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
func (s *cacheStore) cacheBytes() int64 {
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if st, err := os.Stat(s.filename + suffix); err == nil {
			total += st.Size()
		}
	}
	return total
}
