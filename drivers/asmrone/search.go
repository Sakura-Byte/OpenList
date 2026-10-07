package asmrone

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// Media URLs and descriptions need not be allocated during a name search.
type searchTrack struct {
	Title    string        `json:"title"`
	Type     string        `json:"type"`
	Size     number        `json:"size"`
	Children []searchTrack `json:"children"`
}

const searchWorkBatchQuery = `SELECT id,name,range_name,tracks,data FROM works NOT INDEXED WHERE active=1 AND id<? ORDER BY id DESC LIMIT 100`

// Trees are written by encode (encoding/json). A literal keyword absent from
// that canonical JSON cannot occur in a file name. URLs can produce false
// positives; only the decoded name matcher is allowed to emit a result.
func treeMayMatch(raw string, words []string) bool {
	if raw == "" {
		return false
	}
	if len(words) == 0 {
		return true
	}
	lower := strings.ToLower(raw)
	for _, word := range words {
		if !strings.Contains(lower, word) {
			return false
		}
	}
	return true
}

func (d *ASMRONE) SearchCached(ctx context.Context, req model.SearchReq, emit func(model.SearchNode) error) error {
	d.mu.Lock()
	if d.closing || d.cancel == nil {
		d.mu.Unlock()
		return errors.New("ASMR.ONE storage stopped")
	}
	d.wg.Add(1)
	life := d.ctx
	d.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	defer func() { stop(); cancel(); d.wg.Done() }()

	add := func(parent, name string, dir bool, size int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := model.SearchNode{Parent: parent, Name: name, IsDir: dir, Size: size}
		if driver.CachedSearchMatch(req, n) {
			return emit(n)
		}
		return nil
	}
	ranges, err := d.store.ranges(ctx)
	if err != nil {
		return err
	}
	for _, r := range ranges {
		if err = add("/", r.Name, true, 0); err != nil {
			return err
		}
	}
	var walk func(string, []searchTrack) error
	walk = func(parent string, nodes []searchTrack) error {
		for _, n := range nodes {
			if err := add(parent, n.Title, strings.EqualFold(n.Type, "folder") || len(n.Children) > 0, int64(n.Size)); err != nil {
				return err
			}
			if err := walk(path.Join(parent, n.Title), n.Children); err != nil {
				return err
			}
		}
		return nil
	}
	// Bounded batches release SQLite between reads so searches do not monopolize
	// the cache connection needed by browsing and synchronization.
	type row struct {
		id                            int64
		name, rangeName, tracks, data string
	}
	last := int64(1<<63 - 1)
	var treeWords []string
	for _, word := range strings.Fields(strings.ToLower(req.Keywords)) {
		quoted := encode(word)
		treeWords = append(treeWords, quoted[1:len(quoted)-1])
	}
	query := searchWorkBatchQuery
	rangeName := ""
	if parent := path.Clean("/" + req.Parent); parent != "/" {
		rangeName = strings.Split(strings.TrimPrefix(parent, "/"), "/")[0]
		// Both range columns are fixed, so the existing range index can also
		// order by its implicit rowid without sorting or scanning other ranges.
		query = `SELECT id,name,range_name,tracks,data FROM works INDEXED BY works_range WHERE active=1 AND range_name=? AND id<? ORDER BY id DESC LIMIT 100`
	}
	for {
		// The pending-task index starts with active but cannot order by ID. Using
		// it here repeatedly sorts the entire remaining library for every batch.
		args := []any{last}
		if rangeName != "" {
			args = []any{rangeName, last}
		}
		rows, err := d.store.db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		batch := []row{}
		for rows.Next() {
			var v row
			if err = rows.Scan(&v.id, &v.name, &v.rangeName, &v.tracks, &v.data); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, v)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, v := range batch {
			last = v.id
			p := path.Join("/", v.rangeName, v.name)
			if err = add(path.Dir(p), v.name, true, 0); err != nil {
				return err
			}
			if !utils.IsSubPath(req.Parent, p) && !utils.IsSubPath(p, req.Parent) {
				continue
			}
			var nodes []searchTrack
			readmeCandidate := driver.CachedSearchMatch(req, model.SearchNode{Parent: p, Name: "README.md"})
			coverCandidate := driver.CachedSearchMatch(req, model.SearchNode{Parent: p, Name: "cover.jpg"})
			if v.tracks != "" && (readmeCandidate || coverCandidate || treeMayMatch(v.tracks, treeWords)) {
				if err = json.Unmarshal([]byte(v.tracks), &nodes); err != nil {
					return err
				}
				if err = walk(p, nodes); err != nil {
					return err
				}
			}
			names := map[string]bool{}
			for _, n := range nodes {
				names[strings.ToLower(n.Title)] = true
			}
			readme := !names["readme.md"] && readmeCandidate
			cover := !names["cover.jpg"] && coverCandidate
			if !readme && !cover {
				continue
			}
			var w work
			if err = json.Unmarshal([]byte(v.data), &w); err != nil {
				return err
			}
			if readme {
				if err = add(p, "README.md", false, int64(len(w.readme()))); err != nil {
					return err
				}
			}
			if cover && w.Cover != "" {
				if err = add(p, "cover.jpg", false, 0); err != nil {
					return err
				}
			}
		}
	}
}

var _ driver.CachedSearcher = (*ASMRONE)(nil)
