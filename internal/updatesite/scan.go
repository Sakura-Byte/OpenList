package updatesite

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/google/uuid"
)

const (
	defaultScanChunkLimit  = 32
	maxScanChunkLimit      = 256
	scanCursorTTL          = 10 * time.Minute
	defaultChunkBufferSize = 64
)

var (
	walkPublicScan func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error

	ErrScanCursorExpired  = errors.New("update-site scan cursor expired")
	ErrScanCursorMismatch = errors.New("update-site scan cursor does not match request")
	ErrScanNotConfigured  = errors.New("update-site scan dependencies are not configured")

	scanSessions = newScanSessionStore()
)

type ScanPageRequest struct {
	Path         string
	MaxDepth     int
	IncludeThumb bool
	ChunkLimit   int
	Cursor       string
}

type ScanPageChunk struct {
	ParentPath string
	Nodes      []model.Obj
	ParentDone bool
}

type ScanPageStats struct {
	PageCount     int `json:"page_count"`
	ChunkCount    int `json:"chunk_count"`
	DirCount      int `json:"dir_count"`
	EntryCount    int `json:"entry_count"`
	PendingChunks int `json:"pending_chunks"`
}

type ScanPageMeta struct {
	IncludeThumb bool `json:"include_thumb"`
	ChunkLimit   int  `json:"chunk_limit"`
}

type ScanPageResponse struct {
	Chunks []ScanPageChunk `json:"chunks"`
	Cursor string          `json:"cursor,omitempty"`
	Done   bool            `json:"done"`
	Stats  ScanPageStats   `json:"stats"`
	Meta   ScanPageMeta    `json:"meta"`
}

type scanSession struct {
	cursor       string
	path         string
	maxDepth     int
	includeThumb bool

	ctx    context.Context
	cancel context.CancelFunc

	pageMu sync.Mutex

	mu         sync.Mutex
	stats      ScanPageStats
	done       bool
	err        error
	timer      *time.Timer
	chunks     chan ScanPageChunk
	lastAccess time.Time
}

type scanSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*scanSession
}

func newScanSessionStore() *scanSessionStore {
	return &scanSessionStore{
		sessions: make(map[string]*scanSession),
	}
}

func SetPublicScanDeps(walk func(ctx context.Context, rawPath string, maxDepth int, onChunk driver.UpdateSiteChunkCallback) error) {
	walkPublicScan = walk
}

func ScanPublicPathPage(ctx context.Context, req ScanPageRequest) (ScanPageResponse, error) {
	if walkPublicScan == nil {
		return ScanPageResponse{}, ErrScanNotConfigured
	}

	req.Path = utils.FixAndCleanPath(req.Path)
	if req.Path == "" {
		req.Path = "/"
	}
	if req.MaxDepth == 0 {
		req.MaxDepth = -1
	}
	req.ChunkLimit = normalizeScanChunkLimit(req.ChunkLimit)

	cursor, session, err := scanSessions.loadOrCreate(req)
	if err != nil {
		return ScanPageResponse{}, err
	}

	resp, err := session.nextPage(ctx, req.ChunkLimit)
	if err != nil {
		scanSessions.delete(cursor)
		return ScanPageResponse{}, err
	}
	resp.Meta.IncludeThumb = session.includeThumb
	resp.Meta.ChunkLimit = req.ChunkLimit
	if resp.Done {
		resp.Cursor = ""
		scanSessions.delete(cursor)
		return resp, nil
	}
	resp.Cursor = cursor
	return resp, nil
}

func normalizeScanChunkLimit(limit int) int {
	switch {
	case limit <= 0:
		return defaultScanChunkLimit
	case limit > maxScanChunkLimit:
		return maxScanChunkLimit
	default:
		return limit
	}
}

func (s *scanSessionStore) loadOrCreate(req ScanPageRequest) (string, *scanSession, error) {
	if req.Cursor == "" {
		cursor := uuid.NewString()
		session := newScanSession(cursor, req)
		s.mu.Lock()
		s.sessions[cursor] = session
		s.mu.Unlock()
		session.start()
		return cursor, session, nil
	}

	s.mu.Lock()
	session, ok := s.sessions[req.Cursor]
	s.mu.Unlock()
	if !ok {
		return "", nil, ErrScanCursorExpired
	}
	if session.path != req.Path || session.maxDepth != req.MaxDepth || session.includeThumb != req.IncludeThumb {
		return "", nil, ErrScanCursorMismatch
	}
	session.touch()
	return req.Cursor, session, nil
}

func (s *scanSessionStore) delete(cursor string) {
	s.mu.Lock()
	session, ok := s.sessions[cursor]
	if ok {
		delete(s.sessions, cursor)
	}
	s.mu.Unlock()
	if ok {
		session.close()
	}
}

func (s *scanSessionStore) expire(cursor string) {
	s.delete(cursor)
}

func newScanSession(cursor string, req ScanPageRequest) *scanSession {
	ctx, cancel := context.WithCancel(context.Background())
	session := &scanSession{
		cursor:       cursor,
		path:         req.Path,
		maxDepth:     req.MaxDepth,
		includeThumb: req.IncludeThumb,
		ctx:          ctx,
		cancel:       cancel,
		chunks:       make(chan ScanPageChunk, defaultChunkBufferSize),
		lastAccess:   time.Now(),
	}
	session.timer = time.AfterFunc(scanCursorTTL, func() {
		scanSessions.expire(cursor)
	})
	return session
}

func (s *scanSession) start() {
	go func() {
		err := walkPublicScan(s.ctx, s.path, s.maxDepth, func(chunk driver.UpdateSiteChunk) error {
			return s.emitChunk(chunk)
		})
		s.finish(err)
	}()
}

func (s *scanSession) emitChunk(chunk driver.UpdateSiteChunk) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	pageChunk := ScanPageChunk{
		ParentPath: utils.FixAndCleanPath(chunk.Parent),
		Nodes:      chunk.Entries,
		ParentDone: chunk.ParentDone,
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.chunks <- pageChunk:
	}
	s.mu.Lock()
	s.stats.ChunkCount++
	s.stats.EntryCount += len(pageChunk.Nodes)
	if pageChunk.ParentDone {
		s.stats.DirCount++
	}
	s.mu.Unlock()
	return nil
}

func (s *scanSession) finish(err error) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	if errors.Is(err, context.Canceled) && s.ctx.Err() != nil {
		err = nil
	}
	s.done = true
	s.err = err
	close(s.chunks)
	s.mu.Unlock()
}

func (s *scanSession) close() {
	s.cancel()
	s.mu.Lock()
	if s.timer != nil {
		s.timer.Stop()
	}
	s.mu.Unlock()
}

func (s *scanSession) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAccess = time.Now()
	if s.timer != nil {
		s.timer.Reset(scanCursorTTL)
	}
}

func (s *scanSession) nextPage(ctx context.Context, chunkLimit int) (ScanPageResponse, error) {
	s.pageMu.Lock()
	defer s.pageMu.Unlock()

	s.touch()
	resp := ScanPageResponse{
		Chunks: make([]ScanPageChunk, 0, chunkLimit),
	}

	for len(resp.Chunks) < chunkLimit {
		if len(resp.Chunks) > 0 {
			select {
			case chunk, ok := <-s.chunks:
				if !ok {
					goto finalize
				}
				resp.Chunks = append(resp.Chunks, chunk)
				continue
			default:
				goto finalize
			}
		}

		select {
		case <-ctx.Done():
			return ScanPageResponse{}, ctx.Err()
		case chunk, ok := <-s.chunks:
			if !ok {
				goto finalize
			}
			resp.Chunks = append(resp.Chunks, chunk)
		}
	}

finalize:
	s.mu.Lock()
	s.stats.PageCount++
	resp.Stats = s.stats
	resp.Stats.PendingChunks = len(s.chunks)
	done := s.done && len(s.chunks) == 0
	err := s.err
	s.mu.Unlock()

	if err != nil && len(resp.Chunks) == 0 {
		return ScanPageResponse{}, err
	}
	resp.Done = done
	return resp, nil
}
