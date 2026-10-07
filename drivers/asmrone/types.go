package asmrone

import (
	"encoding/json"
	"fmt"
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
func (w work) modified() time.Time { t, _ := time.Parse("2006-01-02", w.Created); return t }
func (w work) readme() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n%s\n\n- ID: %s\n- API ID: %d\n- Circle: %s\n- Release: %s\n", w.Title, w.Introduction, w.SourceID, w.ID, w.Circle.Name, w.Release)
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
	Error         string
	RetryAt       int64
}

func (w cachedWork) name() string {
	if w.DirectoryName != "" {
		return w.DirectoryName
	}
	return w.work.name()
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
type indexJob struct {
	ID               int64
	OldPath, NewPath string
	Revision         int64
}

func encode(v any) string { b, _ := json.Marshal(v); return string(b) }
