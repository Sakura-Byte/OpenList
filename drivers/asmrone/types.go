package asmrone

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ASMR.ONE returns numbers both as JSON numbers and strings.
type number int64

func (n *number) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" || s == "" {
		*n = 0
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*n = number(f)
	return nil
}

type work struct {
	ID        number `json:"id"`
	SourceID  string `json:"source_id"`
	Title     string `json:"title"`
	Created   string `json:"create_date"`
	Release   string `json:"release"`
	Cover     string `json:"mainCoverUrl"`
	Thumbnail string `json:"samCoverUrl"`
	Circle    struct {
		Name string `json:"name"`
	} `json:"circle"`
	Tags []struct {
		Name string `json:"name"`
	} `json:"tags"`
	VAs []struct {
		Name string `json:"name"`
	} `json:"vas"`
	Introduction string `json:"introduction"`
}

func (w work) name() string {
	id := strings.ToUpper(strings.TrimSpace(w.SourceID))
	if id == "" {
		id = fmt.Sprintf("RJ%06d", w.ID)
	}
	return safeName(id + " " + w.Title)
}
func (w work) rangeName() string {
	source := strings.ToUpper(strings.TrimSpace(w.SourceID))
	if source == "" {
		source = fmt.Sprintf("RJ%06d", w.ID)
	}
	if len(source) < 3 || (source[:2] != "RJ" && source[:2] != "VJ" && source[:2] != "BJ") {
		return "其他作品"
	}
	numberPart := source[2:]
	for _, r := range numberPart {
		if r < '0' || r > '9' {
			return "其他作品"
		}
	}
	n, err := strconv.ParseUint(numberPart, 10, 63)
	if err != nil {
		return "其他作品"
	}
	start := n / 10000 * 10000
	width := 6
	if n >= 1000000 {
		width = 8
	}
	return fmt.Sprintf("RJ%0*d~RJ%0*d", width, start, width, start+9999)
}
func (w work) modified() time.Time { t, _ := time.Parse("2006-01-02", w.Created); return t }
func (w work) readme() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", w.Title)
	cover := w.Cover
	if cover == "" {
		cover = w.Thumbnail
	}
	if cover != "" {
		// The frontend classifies the raw Markdown destination before parsing.
		// Angle wrappers look like local paths; encode delimiters instead.
		cover = strings.NewReplacer("<", "%3C", ">", "%3E", "(", "%28", ")", "%29", " ", "%20", "\r", "%0D", "\n", "%0A").Replace(cover)
		fmt.Fprintf(&b, "![Cover](%s)\n\n", cover)
	}
	fmt.Fprintf(&b, "%s\n\n- ID: %s\n- API ID: %d\n- Circle: %s\n- Release: %s\n", w.Introduction, w.SourceID, w.ID, w.Circle.Name, w.Release)
	b.WriteString("- Voice actors: ")
	for i, va := range w.VAs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(va.Name)
	}
	b.WriteString("\n- Tags: ")
	for i, tag := range w.Tags {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tag.Name)
	}
	b.WriteString("\n")
	return []byte(b.String())
}

type track struct {
	Type     string  `json:"type"`
	Title    string  `json:"title"`
	Hash     string  `json:"hash"`
	Stream   string  `json:"mediaStreamUrl"`
	Download string  `json:"mediaDownloadUrl"`
	Size     number  `json:"size"`
	Children []track `json:"children"`
}

func (t track) isDir() bool { return strings.EqualFold(t.Type, "folder") || len(t.Children) > 0 }
func safeName(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		switch r {
		case '/':
			return '／'
		case '\\':
			return '＼'
		case ':':
			return '：'
		case '*':
			return '＊'
		case '?':
			return '？'
		case '"':
			return '＂'
		case '<':
			return '＜'
		case '>':
			return '＞'
		case '|':
			return '｜'
		}
		return r
	}, strings.TrimSpace(s))
	s = strings.TrimRight(s, ". ")
	if s == "" {
		return "untitled"
	}
	return s
}

// Assign distinct sibling names once, preserving both duplicate files and their
// extensions. Reserved suffixes cannot collide with an original sibling name.
func normalizeTracks(nodes []track) []track {
	reserved := map[string]bool{}
	for _, n := range nodes {
		reserved[strings.ToLower(safeName(n.Title))] = true
	}
	used := map[string]bool{}
	for i := range nodes {
		name := safeName(nodes[i].Title)
		if used[strings.ToLower(name)] {
			base, ext := name, ""
			if j := strings.LastIndex(name, "."); j > 0 && !nodes[i].isDir() {
				base, ext = name[:j], name[j:]
			}
			for seq := 2; ; seq++ {
				candidate := fmt.Sprintf("%s (%d)%s", base, seq, ext)
				if !reserved[strings.ToLower(candidate)] && !used[strings.ToLower(candidate)] {
					name = candidate
					break
				}
			}
		}
		nodes[i].Title = name
		used[strings.ToLower(name)] = true
		nodes[i].Children = normalizeTracks(nodes[i].Children)
	}
	return nodes
}

type catalogPage struct {
	Works      []work `json:"works"`
	Pagination struct {
		CurrentPage int `json:"currentPage"`
		PageSize    int `json:"pageSize"`
		TotalCount  int `json:"totalCount"`
	} `json:"pagination"`
}

type cachedWork struct {
	work
	DirectoryName string
	Active        bool
	Tracks        []track
	HasTracks     bool
	TracksAt      int64
	ModifiedAt    int64
	Error         string
	RetryAt       int64
}

func (w cachedWork) name() string {
	if w.DirectoryName != "" {
		return w.DirectoryName
	}
	return w.work.name()
}
func (w cachedWork) path() string { return path.Join("/", w.rangeName(), w.name()) }
func (w cachedWork) modified() time.Time {
	if updated := max(w.ModifiedAt, w.TracksAt*1000); updated > 0 {
		return time.UnixMilli(updated)
	}
	return w.work.modified()
}

type rangeInfo struct {
	Name       string
	ModifiedAt int64
}

type syncState struct {
	Paused        bool
	FullCompleted bool
	Watermark     string
	LastSync      int64
	LastFullSync  int64
	RetryAt       int64
	Requested     string
	LastError     string
}

func encode(v any) string { b, _ := json.Marshal(v); return string(b) }
