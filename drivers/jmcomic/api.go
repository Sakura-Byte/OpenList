package jmcomic

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Public mobile protocol constants, not account credentials. Protocol reference:
// https://github.com/hect0x7/JMComic-Crawler-Python (MIT; see NOTICE).
const appSecret = "185Hcomic3PAPP7R"
const templateSecret = "18comicAPPContent"
const appVersion = "2.1.7"
const mobileUA = "Mozilla/5.0 (Linux; Android 9; V1938CT Build/PQ3A.190705.11211812; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Safari/537.36"

type upstreamError struct {
	Status int
	Retry  time.Duration
}

func (e *upstreamError) Error() string { return fmt.Sprintf("JMComic metadata HTTP %d", e.Status) }
func unavailable(e error) bool {
	var u *upstreamError
	return errors.As(e, &u) && (u.Status == 404 || u.Status == 410)
}
func digest(s string) string { return fmt.Sprintf("%x", md5.Sum([]byte(s))) }
func decodeData(raw string, ts string) ([]byte, error) {
	b, e := base64.StdEncoding.DecodeString(raw)
	if e != nil {
		return nil, e
	}
	block, e := aes.NewCipher([]byte(digest(ts + appSecret)))
	if e != nil {
		return nil, e
	}
	if len(b) == 0 || len(b)%aes.BlockSize != 0 {
		return nil, errors.New("invalid encrypted JMComic payload")
	}
	for i := 0; i < len(b); i += aes.BlockSize {
		block.Decrypt(b[i:i+aes.BlockSize], b[i:i+aes.BlockSize])
	}
	n := int(b[len(b)-1])
	if n < 1 || n > aes.BlockSize || n > len(b) {
		return nil, errors.New("invalid JMComic padding")
	}
	for _, v := range b[len(b)-n:] {
		if int(v) != n {
			return nil, errors.New("invalid JMComic padding")
		}
	}
	return b[:len(b)-n], nil
}
func (d *JMComic) request(ctx context.Context, route string, query url.Values, high, html bool) ([]byte, error) {
	if route != "/setting" {
		if e := d.ensureSession(ctx, high); e != nil {
			return nil, e
		}
	}
	var out []byte
	for attempt := 0; attempt < 3; attempt++ {
		e := d.scheduler.do(ctx, high, func() error {
			ts := strconv.FormatInt(time.Now().Unix(), 10)
			secret := appSecret
			if html {
				secret = templateSecret
			}
			u := d.APIURL + route
			if len(query) > 0 {
				u += "?" + query.Encode()
			}
			req, e := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
			if e != nil {
				return e
			}
			req.Header.Set("User-Agent", mobileUA)
			req.Header.Set("token", digest(ts+secret))
			req.Header.Set("tokenparam", ts+","+appVersion)
			res, e := d.apiClient.Do(req)
			if e != nil {
				return e
			}
			defer res.Body.Close()
			if res.StatusCode != 200 {
				retry := time.Duration(1<<attempt) * time.Second
				if s := res.Header.Get("Retry-After"); s != "" {
					if sec, e := strconv.Atoi(s); e == nil {
						retry = time.Duration(sec) * time.Second
					} else if t, e := http.ParseTime(s); e == nil {
						retry = time.Until(t)
					}
				}
				retry = max(retry, time.Second)
				if res.StatusCode == 429 || res.StatusCode >= 500 {
					d.scheduler.cooldown(retry)
				}
				return &upstreamError{res.StatusCode, retry}
			}
			b, e := io.ReadAll(io.LimitReader(res.Body, 8<<20+1))
			if e != nil {
				return e
			}
			if len(b) > 8<<20 {
				return errors.New("JMComic metadata exceeds limit")
			}
			b = bytes.TrimPrefix(b, []byte{0xef, 0xbb, 0xbf})
			if html {
				out = b
				return nil
			}
			var envelope struct {
				Code number          `json:"code"`
				Data json.RawMessage `json:"data"`
			}
			if e = json.Unmarshal(b, &envelope); e != nil {
				return fmt.Errorf("decode JMComic envelope: %w", e)
			}
			if envelope.Code != 200 {
				retry := time.Duration(1<<attempt) * time.Second
				if s := res.Header.Get("Retry-After"); s != "" {
					if n, e := strconv.Atoi(s); e == nil {
						retry = time.Duration(n) * time.Second
					} else if until, e := http.ParseTime(s); e == nil {
						retry = time.Until(until)
					}
				}
				retry = max(retry, time.Second)
				if envelope.Code == 429 || envelope.Code >= 500 {
					d.scheduler.cooldown(retry)
				}
				return &upstreamError{Status: int(envelope.Code), Retry: retry}
			}
			var encoded string
			if e = json.Unmarshal(envelope.Data, &encoded); e != nil {
				return errors.New("unexpected JMComic metadata payload")
			}
			out, e = decodeData(encoded, ts)
			if e != nil {
				return fmt.Errorf("decode JMComic payload: %w", e)
			}
			return nil
		})
		if e == nil {
			return out, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var status *upstreamError
		if errors.As(e, &status) {
			if status.Status != 429 && status.Status < 500 {
				return nil, e
			}
		} else {
			var syntax *json.SyntaxError
			if errors.As(e, &syntax) {
				return nil, e
			}
			d.scheduler.cooldown(time.Duration(1<<attempt) * time.Second)
		}
		if attempt == 2 {
			return nil, e
		}
	}
	return nil, errors.New("JMComic request failed")
}
func (d *JMComic) ensureSession(ctx context.Context, high bool) error {
	d.mu.Lock()
	ready := d.sessionReady
	life := d.ctx
	d.mu.Unlock()
	if ready {
		return nil
	}
	ch := d.sessionGroup.DoChan("anonymous", func() (bool, error) {
		c, done, e := d.begin(life)
		if e != nil {
			return false, e
		}
		defer done()
		d.mu.Lock()
		ready := d.sessionReady
		d.mu.Unlock()
		if ready {
			return true, nil
		}
		_, e = d.request(c, "/setting", nil, high, false)
		if e != nil {
			return false, e
		}
		d.mu.Lock()
		d.sessionReady = true
		d.mu.Unlock()
		return true, nil
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		return r.Err
	}
}
func (d *JMComic) json(ctx context.Context, route string, q url.Values, high bool, out any) error {
	b, e := d.request(ctx, route, q, high, false)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(b, out); e != nil {
		return fmt.Errorf("parse JMComic %s: %w", route, e)
	}
	return nil
}
func (d *JMComic) catalogPage(ctx context.Context, n int, category, order string, high bool) ([]album, error) {
	var r struct {
		Content []album `json:"content"`
	}
	e := d.json(ctx, "/categories/filter", url.Values{"page": {strconv.Itoa(n)}, "order": {""}, "c": {category}, "o": {order}}, high, &r)
	if e != nil {
		return nil, e
	}
	if r.Content == nil {
		return nil, errors.New("JMComic category response has no content")
	}
	return r.Content, nil
}
func (d *JMComic) fetchDetail(ctx context.Context, key string, high bool) (detail, error) {
	var r detail
	e := d.json(ctx, "/album", url.Values{"id": {key}}, high, &r)
	if e != nil {
		return r, e
	}
	if string(r.ID) != key || r.Name == "" {
		return r, errors.New("invalid JMComic album detail")
	}
	seen := map[id]bool{}
	var chapters []chapter
	for _, c := range r.Series {
		if !validID(string(c.ID)) || seen[c.ID] {
			return r, errors.New("invalid or duplicate JMComic chapter")
		}
		seen[c.ID] = true
		chapters = append(chapters, c)
	}
	if len(chapters) == 0 {
		chapters = []chapter{{ID: id(key), Name: r.Name, Sort: "1"}}
	}
	sort.SliceStable(chapters, func(i, j int) bool {
		a, _ := strconv.Atoi(string(chapters[i].Sort))
		b, _ := strconv.Atoi(string(chapters[j].Sort))
		return a < b
	})
	r.Series = chapters
	return r, nil
}
func assignedObject(s, name string) (string, error) {
	r := regexp.MustCompile(`(?:const|var|let)\s+` + regexp.QuoteMeta(name) + `\s*=\s*`)
	loc := r.FindStringIndex(s)
	if loc == nil {
		return "", fmt.Errorf("JMComic template missing %s", name)
	}
	start := loc[1]
	for start < len(s) && s[start] != '{' {
		start++
	}
	depth := 0
	quote := byte(0)
	escape := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if escape {
				escape = false
			} else if c == '\\' {
				escape = true
			} else if c == quote {
				quote = 0
			}
			continue
		}
		if c == '"' || c == '\'' {
			quote = c
			continue
		}
		if c == '{' {
			depth++
		}
		if c == '}' {
			depth--
			if depth == 0 {
				return s[start : i+1], nil
			}
		}
	}
	return "", fmt.Errorf("JMComic template malformed %s", name)
}
func parseTemplate(b []byte, key, albumID string, version int64) (chapterTree, error) {
	t := chapterTree{ID: key, AlbumID: albumID, Version: version}
	s := string(b)
	conf, e := assignedObject(s, "config")
	if e != nil {
		return t, e
	}
	result, e := assignedObject(s, "result")
	if e != nil {
		return t, e
	}
	var c struct {
		Host  string `json:"imghost"`
		ID    id     `json:"jmid"`
		Cache string `json:"cache"`
	}
	var r struct {
		Images []string `json:"images"`
	}
	if e = json.Unmarshal([]byte(conf), &c); e != nil {
		return t, e
	}
	if e = json.Unmarshal([]byte(result), &r); e != nil {
		return t, e
	}
	if string(c.ID) != key || r.Images == nil {
		return t, errors.New("invalid JMComic chapter template")
	}
	u, e := url.Parse(c.Host)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return t, errors.New("invalid JMComic image host")
	}
	re := regexp.MustCompile(`(?:var|let|const)\s+scramble_id\s*=\s*(\d+)`)
	m := re.FindStringSubmatch(s)
	if len(m) != 2 {
		return t, errors.New("JMComic template missing scramble_id")
	}
	scramble, e := strconv.ParseInt(m[1], 10, 64)
	if e != nil {
		return t, e
	}
	chapterID, _ := strconv.ParseInt(key, 10, 64)
	for i, f := range r.Images {
		if f == "" || strings.ContainsAny(f, "/\\?#") || f == "." || f == ".." {
			return t, errors.New("unsafe JMComic image filename")
		}
		gif := strings.EqualFold(pathExtension(f), ".gif")
		name := fmt.Sprintf("%04d.png", i+1)
		if gif {
			name = fmt.Sprintf("%04d.gif", i+1)
		}
		segments := 0
		if !gif {
			segments = segmentCount(scramble, chapterID, strings.TrimSuffix(f, pathExtension(f)))
		}
		t.Pages = append(t.Pages, page{Name: name, URL: strings.TrimRight(c.Host, "/") + "/media/photos/" + key + "/" + url.PathEscape(f) + c.Cache, Segments: segments, GIF: gif})
	}
	if t.Pages == nil {
		t.Pages = []page{}
	}
	return t, nil
}
func pathExtension(s string) string {
	i := strings.LastIndexByte(s, '.')
	if i < 0 {
		return ""
	}
	return s[i:]
}
func segmentCount(scramble, key int64, name string) int {
	if key < scramble {
		return 0
	}
	if key < 268850 {
		return 10
	}
	mod := 8
	if key < 421926 {
		mod = 10
	}
	h := digest(strconv.FormatInt(key, 10) + name)
	return int(h[len(h)-1])%mod*2 + 2
}
