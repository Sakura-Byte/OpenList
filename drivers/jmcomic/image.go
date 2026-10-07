package jmcomic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"net/http"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	_ "golang.org/x/image/webp"
)

// Count live outputs as well as active decoders: a slow client must not leave
// unbounded converted images resident while more conversions are admitted.
var conversions = struct {
	sync.Mutex
	owners  map[*JMComic]int
	active  int
	changed chan struct{}
}{owners: map[*JMComic]int{}, changed: make(chan struct{})}

func conversionChanged() { close(conversions.changed); conversions.changed = make(chan struct{}) }
func acquireConversionOwner(d *JMComic, n int) {
	conversions.Lock()
	defer conversions.Unlock()
	conversions.owners[d] = n
	conversionChanged()
}
func releaseConversionOwner(d *JMComic) {
	conversions.Lock()
	defer conversions.Unlock()
	delete(conversions.owners, d)
	conversionChanged()
}
func acquireConversion(ctx context.Context) (func(), error) {
	for {
		conversions.Lock()
		limit := 1
		first := true
		for _, n := range conversions.owners {
			if first || n < limit {
				limit = n
				first = false
			}
		}
		if conversions.active < limit {
			conversions.active++
			conversions.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { conversions.Lock(); conversions.active--; conversionChanged(); conversions.Unlock() })
			}, nil
		}
		wake := conversions.changed
		conversions.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

type boundedWriter struct {
	ctx    context.Context
	buffer bytes.Buffer
	limit  int64
}

func (w *boundedWriter) Write(b []byte) (int, error) {
	if e := w.ctx.Err(); e != nil {
		return 0, e
	}
	if int64(w.buffer.Len())+int64(len(b)) > w.limit {
		return 0, errors.New("JMComic converted image exceeds output limit")
	}
	return w.buffer.Write(b)
}

// A row view reuses the decoded image. PNG's scanline encoder needs no second
// full raster and can preserve alpha without a JPEG round trip.
type rowView struct {
	image.Image
	rows []int
}

func (r rowView) At(x, y int) color.Color {
	return r.Image.At(x, r.rows[y-r.Bounds().Min.Y]+r.Bounds().Min.Y)
}
func restoreRows(height, segments int) []int {
	rows := make([]int, height)
	for i := range rows {
		rows[i] = i
	}
	if segments <= 1 {
		return rows
	}
	base, rem := height/segments, height%segments
	for i := 0; i < segments; i++ {
		n := base
		target := base * i
		source := height - base*(i+1) - rem
		if i == 0 {
			n += rem
		} else {
			target += rem
		}
		for y := 0; y < n; y++ {
			rows[target+y] = source + y
		}
	}
	return rows
}
func convertImage(ctx context.Context, b []byte, pg page, maxPixels, outputLimit int64) ([]byte, string, error) {
	if e := ctx.Err(); e != nil {
		return nil, "", e
	}
	cfg, format, e := image.DecodeConfig(bytes.NewReader(b))
	if e != nil {
		return nil, "", fmt.Errorf("decode JMComic image header: %w", e)
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width) > maxPixels/int64(cfg.Height) {
		return nil, "", errors.New("JMComic decoded image exceeds pixel limit")
	}
	if pg.GIF {
		if format != "gif" {
			return nil, "", errors.New("JMComic GIF response has wrong format")
		}
		if int64(len(b)) > outputLimit {
			return nil, "", errors.New("JMComic GIF exceeds output limit")
		}
		return b, "image/gif", nil
	}
	img, _, e := image.Decode(bytes.NewReader(b))
	if e != nil {
		return nil, "", fmt.Errorf("decode JMComic image: %w", e)
	}
	if img.Bounds().Dx() != cfg.Width || img.Bounds().Dy() != cfg.Height {
		return nil, "", errors.New("JMComic image dimensions changed while decoding")
	}
	w := &boundedWriter{ctx: ctx, limit: outputLimit}
	enc := png.Encoder{CompressionLevel: png.BestSpeed}
	if e = enc.Encode(w, rowView{Image: img, rows: restoreRows(cfg.Height, pg.Segments)}); e != nil {
		return nil, "", e
	}
	return w.buffer.Bytes(), "image/png", nil
}

type memoryBuffer struct {
	data    []byte
	release func()
	once    sync.Once
}

func (m *memoryBuffer) Close() error {
	m.once.Do(func() {
		m.data = nil
		if m.release != nil {
			m.release()
		}
	})
	return nil
}
func (m *memoryBuffer) RangeRead(ctx context.Context, r http_range.Range) (io.ReadCloser, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	size := int64(len(m.data))
	if r.Start < 0 || r.Start > size {
		return nil, errors.New("invalid JMComic byte range")
	}
	end := size
	if r.Length >= 0 && r.Length < size-r.Start {
		end = r.Start + r.Length
	}
	return io.NopCloser(bytes.NewReader(m.data[r.Start:end])), nil
}
func (d *JMComic) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	c, done, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	o, ok := file.(*object)
	if !ok {
		return nil, errs.ObjectNotFound
	}
	if o.content != nil {
		m := &memoryBuffer{data: o.content}
		return &model.Link{RangeReader: m, ContentLength: int64(len(m.data)), SyncClosers: utils.NewSyncClosers(m), RequireReference: true}, nil
	}
	if d.Mode == "client" {
		return nil, fmt.Errorf("%w: JMComic client mode reads images in the browser only", errs.NotSupport)
	}
	if o.page == nil {
		return nil, errs.ObjectNotFound
	}
	release, e := acquireConversion(c)
	if e != nil {
		return nil, e
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	req, e := http.NewRequestWithContext(c, http.MethodGet, o.page.URL, nil)
	if e != nil {
		return nil, e
	}
	req.Header.Set("User-Agent", mobileUA)
	req.Header.Set("Referer", d.APIURL+"/")
	req.Header.Set("X-Requested-With", "com.JMComic3.app")
	res, e := base.TransferClientFor(d).Do(req)
	if e != nil {
		return nil, e
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JMComic image HTTP %d", res.StatusCode)
	}
	limit := int64(d.InputMiB) << 20
	if res.ContentLength > limit {
		return nil, errors.New("JMComic source image exceeds input limit")
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if e != nil {
		return nil, e
	}
	if int64(len(b)) > limit {
		return nil, errors.New("JMComic source image exceeds input limit")
	}
	contentType := res.Header.Get("Content-Type")
	if o.chapterID != "" {
		b, contentType, e = convertImage(c, b, *o.page, d.MaxPixels, int64(d.OutputMiB)<<20)
		if e != nil {
			return nil, e
		}
	} else {
		if _, _, e = image.DecodeConfig(bytes.NewReader(b)); e != nil {
			return nil, errors.New("invalid JMComic cover image")
		}
	}
	m := &memoryBuffer{data: b, release: release}
	success = true
	return &model.Link{RangeReader: m, ContentLength: int64(len(b)), Header: http.Header{"Content-Type": {contentType}}, SyncClosers: utils.NewSyncClosers(m), RequireReference: true}, nil
}
