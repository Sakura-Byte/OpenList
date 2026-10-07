package asmrone

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func retryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil {
		return max(time.Until(date), time.Duration(0))
	}
	return 0
}
func (d *ASMRONE) request(ctx context.Context, endpoint string, high bool, dst any) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		var body []byte
		var status int
		var delay time.Duration
		err := d.scheduler.do(ctx, high, func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.APIURL+endpoint, nil)
			if err != nil {
				return err
			}
			req.Header.Set("User-Agent", "OpenList ASMR.ONE Driver")
			req.Header.Set("Cookie", "locale=zh-cn")
			res, err := d.apiClient.Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			status = res.StatusCode
			delay = retryAfter(res.Header.Get("Retry-After"))
			if status == 429 {
				if delay == 0 {
					delay = time.Duration(1<<attempt) * time.Second
				}
				d.scheduler.cooldown(delay)
			}
			body, err = io.ReadAll(io.LimitReader(res.Body, 64<<20+1))
			if err == nil && len(body) > 64<<20 {
				return errors.New("ASMR.ONE metadata response too large")
			}
			return err
		})
		if err == nil && status == http.StatusOK {
			if err := json.Unmarshal(body, dst); err != nil {
				return fmt.Errorf("decode ASMR.ONE metadata: %w", err)
			}
			return nil
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("ASMR.ONE metadata HTTP %d", status)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if status != 0 && status != 429 && status < 500 {
			return last
		}
		if delay == 0 {
			delay = time.Duration(1<<attempt) * time.Second
		}
		if attempt == 2 {
			return last
		}
		if err := waitContext(ctx, delay); err != nil {
			return err
		}
	}
	return last
}

func (d *ASMRONE) loadTracks(ctx context.Context, id int64, force, high bool) ([]track, error) {
	key := strconv.FormatInt(id, 10)
	ch := d.tracksGroup.DoChan(key, func() ([]track, error) {
		d.mu.Lock()
		if d.closing {
			d.mu.Unlock()
			return nil, context.Canceled
		}
		d.wg.Add(1)
		lifetime := d.ctx
		d.mu.Unlock()
		defer d.wg.Done()
		requestCtx, cancel := context.WithCancel(lifetime)
		defer cancel()
		// Background cancellation stops prefetching. A browser cancellation does
		// not discard a shared result that another browser is still awaiting.
		if !high {
			stop := context.AfterFunc(ctx, cancel)
			defer stop()
		}
		w, err := d.store.byID(requestCtx, id)
		if err != nil {
			return nil, err
		}
		if w.HasTracks && !force {
			return w.Tracks, nil
		}
		if !force && w.RetryAt > time.Now().Unix() {
			return nil, fmt.Errorf("ASMR.ONE track fetch delayed: %s", w.Error)
		}
		var nodes []track
		err = d.request(requestCtx, "/api/tracks/"+key+"?v=2", high, &nodes)
		if err == nil && nodes == nil {
			err = errors.New("ASMR.ONE tracks response is null")
		}
		if err == nil {
			err = validateTracks(nodes)
		}
		if err != nil {
			if requestCtx.Err() == nil {
				_ = d.store.failTracks(requestCtx, id, err.Error(), time.Now().Add(time.Hour).Unix())
				d.setError(err)
			}
			return nil, err
		}
		if len(nodes) == 1 && nodes[0].isDir() && strings.EqualFold(strings.TrimSpace(nodes[0].Title), w.SourceID) {
			nodes = nodes[0].Children
		}
		if nodes == nil {
			nodes = []track{}
		}
		nodes = normalizeTracks(nodes)
		if err := d.store.saveTracks(requestCtx, w, nodes, time.Now().Unix()); err != nil {
			return nil, err
		}
		d.signal()
		return nodes, nil
	})
	select {
	case result := <-ch:
		return result.Val, result.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func validateTracks(nodes []track) error {
	for _, n := range nodes {
		if n.Type == "" {
			return errors.New("ASMR.ONE track has no type")
		}
		if n.Size < 0 {
			return errors.New("ASMR.ONE track has negative size")
		}
		if err := validateTracks(n.Children); err != nil {
			return err
		}
	}
	return nil
}

func (d *ASMRONE) setError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state.LastError = err.Error()
	_ = d.store.saveState(d.ctx, d.state)
}
func (d *ASMRONE) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
	select {
	case d.manualWake <- struct{}{}:
	default:
	}
}
func (d *ASMRONE) setPhase(phase, current string) {
	d.mu.Lock()
	d.phase = phase
	d.current = current
	d.mu.Unlock()
}
func (d *ASMRONE) syncCatalog(ctx context.Context, full bool) error {
	d.mu.Lock()
	watermark := d.state.Watermark
	d.pages = 0
	d.totalPages = 0
	d.mu.Unlock()
	generation := ""
	if full {
		generation = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	seen := map[int64]bool{}
	total := -1
	newest := watermark
	lastDate := ""
	for page := 1; ; page++ {
		d.setPhase(map[bool]string{true: "full", false: "incremental"}[full], fmt.Sprintf("catalog page %d", page))
		var response catalogPage
		if err := d.request(ctx, fmt.Sprintf("/api/works?order=create_date&sort=desc&page=%d&pageSize=100&subtitle=0", page), false, &response); err != nil {
			return err
		}
		p := response.Pagination
		if p.CurrentPage != page || p.PageSize <= 0 || p.PageSize > 100 || p.TotalCount < 0 || response.Works == nil || len(response.Works) > p.PageSize {
			return errors.New("invalid ASMR.ONE catalog pagination")
		}
		if total < 0 {
			total = p.TotalCount
		} else if p.TotalCount != total {
			return errors.New("ASMR.ONE catalog changed during sync; retry without removing works")
		}
		d.mu.Lock()
		d.pages = page
		d.totalPages = (total + p.PageSize - 1) / p.PageSize
		d.mu.Unlock()
		stop := false
		batch := make([]work, 0, len(response.Works))
		for _, w := range response.Works {
			if w.ID <= 0 || w.Created == "" {
				return errors.New("invalid ASMR.ONE catalog work")
			}
			if _, err := time.Parse("2006-01-02", w.Created); err != nil {
				return fmt.Errorf("invalid ASMR.ONE create_date: %w", err)
			}
			if lastDate != "" && w.Created > lastDate {
				return errors.New("ASMR.ONE catalog is not ordered by create_date")
			}
			lastDate = w.Created
			if seen[int64(w.ID)] {
				return errors.New("duplicate work in ASMR.ONE catalog; retry without removing works")
			}
			seen[int64(w.ID)] = true
			if !full && watermark != "" && w.Created < watermark {
				stop = true
				break
			}
			if w.Created > newest {
				newest = w.Created
			}
			batch = append(batch, w)
		}
		if err := d.store.upsertPage(ctx, batch, generation); err != nil {
			return err
		}
		if stop {
			break
		}
		if page*p.PageSize >= total {
			if len(seen) != total {
				return errors.New("incomplete ASMR.ONE catalog; retry without removing works")
			}
			break
		}
		if len(response.Works) != p.PageSize {
			return errors.New("truncated ASMR.ONE catalog; retry without removing works")
		}
	}
	if full {
		if err := d.store.finishFull(ctx, generation); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state.Watermark = newest
	d.state.LastSync = time.Now().Unix()
	d.state.RetryAt = 0
	d.state.LastError = ""
	if full {
		d.state.FullCompleted = true
		d.state.LastFullSync = d.state.LastSync
	}
	mode := "incremental"
	if full {
		mode = "full"
	}
	if d.state.Requested == mode {
		d.state.Requested = ""
	}
	return d.store.saveState(ctx, d.state)
}

func (d *ASMRONE) background() {
	defer d.wg.Done()
	for {
		if d.ctx.Err() != nil {
			return
		}
		d.mu.Lock()
		state := d.state
		mode := state.Requested
		if state.Paused || time.Now().Unix() < state.RetryAt {
			mode = ""
		}
		if mode == "" && !state.Paused && time.Now().Unix() >= state.RetryAt {
			if !state.FullCompleted || time.Now().Unix() >= state.LastFullSync+int64(d.FullSyncDays)*86400 {
				mode = "full"
			} else if time.Now().Unix() >= state.LastSync+int64(d.SyncHours)*3600 {
				mode = "incremental"
			}
		}
		jobCtx, cancel := context.WithCancel(d.ctx)
		d.backgroundCancel = cancel
		if mode != "" {
			if d.state.Requested == "" {
				d.state.Requested = mode
			}
			_ = d.store.saveState(d.ctx, d.state)
		}
		d.mu.Unlock()
		var err error
		if mode != "" {
			err = d.syncCatalog(jobCtx, mode == "full")
		} else {
			var w cachedWork
			err = sql.ErrNoRows
			if !state.Paused && state.FullCompleted {
				w, err = d.store.nextWork(jobCtx, time.Now().Unix())
			}
			if err == nil {
				d.setPhase("tracks", w.SourceID)
				_, err = d.loadTracks(jobCtx, int64(w.ID), false, false)
			}
		}
		cancel()
		d.mu.Lock()
		d.backgroundCancel = nil
		if mode != "" && errors.Is(err, context.Canceled) && d.state.Requested == "" {
			d.state.Requested = mode
			_ = d.store.saveState(d.ctx, d.state)
		}
		d.mu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled) {
			d.mu.Lock()
			d.state.LastError = err.Error()
			if mode != "" {
				d.state.RetryAt = time.Now().Add(time.Hour).Unix()
				if d.state.Requested == "" {
					d.state.Requested = mode
				}
			}
			_ = d.store.saveState(d.ctx, d.state)
			d.mu.Unlock()
		}
		d.setPhase("idle", "")
		if mode != "" || err == nil {
			continue
		}
		select {
		case <-d.ctx.Done():
			return
		case <-d.wake:
		case <-time.After(time.Second):
		}
	}
}

// Manual work jobs stay durable but dispatch independently of a multi-page
// catalog sync, using the same foreground-priority upstream queue.
func (d *ASMRONE) manualTasks() {
	defer d.wg.Done()
	for {
		if d.ctx.Err() != nil {
			return
		}
		w, err := scanWork(d.store.db.QueryRowContext(d.ctx, `SELECT `+workColumns+` FROM works WHERE active=1 AND refresh_requested=1 AND retry_at<=? ORDER BY id DESC LIMIT 1`, time.Now().Unix()))
		if err == nil {
			d.mu.Lock()
			d.manualCurrent = w.SourceID
			d.mu.Unlock()
			_, err = d.loadTracks(d.ctx, int64(w.ID), true, true)
			d.mu.Lock()
			d.manualCurrent = ""
			d.mu.Unlock()
			if err == nil {
				continue
			}
		}
		select {
		case <-d.ctx.Done():
			return
		case <-d.manualWake:
		case <-time.After(time.Second):
		}
	}
}

func (d *ASMRONE) ASMRONEStatus(ctx context.Context) (driver.ASMRONEStatus, error) {
	d.mu.Lock()
	if d.store == nil || d.cancel == nil || d.closing {
		d.mu.Unlock()
		return driver.ASMRONEStatus{}, errors.New("ASMR.ONE storage is not initialized")
	}
	s := d.state
	result := driver.ASMRONEStatus{Phase: d.phase, Paused: s.Paused, CurrentTask: d.current, Pages: d.pages, TotalPages: d.totalPages, LastSync: s.LastSync, LastFullSync: s.LastFullSync, LastError: s.LastError}
	if d.manualCurrent != "" {
		result.CurrentTask = strings.TrimSpace(result.CurrentTask + " | refresh " + d.manualCurrent)
		if result.Phase == "idle" {
			result.Phase = "tracks"
		}
	}
	d.mu.Unlock()
	result.NextSync = max(s.LastSync+int64(d.SyncHours)*3600, s.RetryAt)
	if s.LastSync == 0 {
		result.NextSync = max(time.Now().Unix(), s.RetryAt)
	}
	result.NextFullSync = max(s.LastFullSync+int64(d.FullSyncDays)*86400, s.RetryAt)
	if !s.FullCompleted {
		result.NextFullSync = max(time.Now().Unix(), s.RetryAt)
	}
	var err error
	result.Works, result.Cached, result.Pending, result.Failed, err = d.store.counts(ctx)
	result.CacheBytes = d.store.cacheBytes()
	return result, err
}
func (d *ASMRONE) ASMRONEAction(ctx context.Context, action, workID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.store == nil || d.cancel == nil {
		return errors.New("ASMR.ONE storage stopped")
	}
	switch action {
	case "pause":
		d.state.Paused = true
		if d.backgroundCancel != nil {
			d.backgroundCancel()
		}
	case "resume":
		d.state.Paused = false
	case "incremental", "full":
		if d.phase != action && d.state.Requested != "full" {
			d.state.Requested = action
		}
		if action == "incremental" && !d.state.FullCompleted {
			d.state.Requested = "full"
		}
		d.state.RetryAt = 0
	case "retry":
		if _, err := d.store.db.ExecContext(ctx, `UPDATE works SET refresh_requested=1,retry_at=0 WHERE active=1 AND error<>''`); err != nil {
			return err
		}
	case "refresh":
		var w cachedWork
		var err error
		if id, parseErr := strconv.ParseInt(workID, 10, 64); parseErr == nil && id > 0 {
			w, err = d.store.byID(ctx, id)
		} else {
			err = errors.New("work not found")
			rows, e := d.store.works(ctx)
			if e != nil {
				return e
			}
			for _, candidate := range rows {
				if strings.EqualFold(candidate.SourceID, strings.TrimSpace(workID)) {
					w = candidate
					err = nil
					break
				}
			}
		}
		if err != nil || !w.Active {
			return errors.New("active ASMR.ONE work not found")
		}
		if _, err := d.store.db.ExecContext(ctx, `UPDATE works SET refresh_requested=1,retry_at=0 WHERE id=?`, int64(w.ID)); err != nil {
			return err
		}
	default:
		return errors.New("unsupported ASMR.ONE action")
	}
	if err := d.store.saveState(ctx, d.state); err != nil {
		return err
	}
	d.signal()
	return nil
}
