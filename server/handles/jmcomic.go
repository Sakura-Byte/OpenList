package handles

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

func jmcomicManager(id uint) (driver.JMComicManager, error) {
	s, e := db.GetStorageById(id)
	if e != nil {
		return nil, e
	}
	if s.Driver != "JMComic" || s.Disabled {
		return nil, errors.New("active JMComic storage required")
	}
	d, e := op.GetStorageByMountPath(s.MountPath)
	if e != nil {
		return nil, e
	}
	if d.GetStorage().Status != op.WORK {
		return nil, errors.New("JMComic storage is not initialized")
	}
	m, ok := d.(driver.JMComicManager)
	if !ok {
		return nil, errors.New("JMComic management unavailable")
	}
	return m, nil
}
func JMComicStatus(c *gin.Context) {
	id, e := strconv.ParseUint(c.Query("id"), 10, 32)
	if e != nil || id == 0 {
		common.ErrorStrResp(c, "invalid storage id", 400)
		return
	}
	m, e := jmcomicManager(uint(id))
	if e != nil {
		common.ErrorResp(c, e, 400)
		return
	}
	s, e := m.JMComicStatus(c.Request.Context())
	if e != nil {
		common.ErrorResp(c, e, 500)
		return
	}
	common.SuccessResp(c, s)
}
func JMComicAction(c *gin.Context) {
	var r struct {
		ID     uint   `json:"id"`
		Action string `json:"action"`
		WorkID string `json:"work_id"`
	}
	if e := c.ShouldBindJSON(&r); e != nil || r.ID == 0 {
		common.ErrorStrResp(c, "invalid JMComic action", 400)
		return
	}
	m, e := jmcomicManager(r.ID)
	if e != nil {
		common.ErrorResp(c, e, 400)
		return
	}
	if e = m.JMComicAction(c.Request.Context(), r.Action, r.WorkID); e != nil {
		common.ErrorResp(c, e, 400)
		return
	}
	common.SuccessResp(c, nil)
}

// Keep driver paths relative to storage; only this authenticated handler turns
// them into user-visible navigation paths and signed content links.
func prepareJMReader(c *gin.Context, user *model.User, requestPath, password string, m *driver.JMReaderManifest) error {
	d, e := fs.GetStorage(requestPath, &fs.GetStoragesArgs{})
	if e != nil {
		return e
	}
	mount := d.GetStorage().MountPath
	base := strings.TrimRight(path.Clean("/"+user.BasePath), "/")
	full := func(p string) string { return utils.GetFullPath(mount, p) }
	visible := func(p string) (string, error) {
		p = full(p)
		if base != "" && p != base && !strings.HasPrefix(p, base+"/") {
			return "", errors.New("reader path outside user base")
		}
		v := strings.TrimPrefix(p, base)
		if v == "" {
			v = "/"
		}
		return v, nil
	}
	allowed := func(p string) bool {
		meta, e := op.GetNearestMeta(p)
		if e != nil && !errors.Is(e, errs.MetaNotFound) {
			return false
		}
		return common.CanAccess(user, meta, p, password)
	}
	albumFull := full(m.AlbumPath)
	if !allowed(albumFull) {
		return errors.New("reader work is not accessible")
	}
	m.AlbumPath, e = visible(m.AlbumPath)
	if e != nil {
		return e
	}
	chapters := make([]driver.JMReaderChapter, 0, len(m.Chapters))
	var currentFull string
	for _, ch := range m.Chapters {
		p := full(ch.Path)
		if !allowed(p) {
			continue
		}
		ch.Path, e = visible(ch.Path)
		if e != nil {
			continue
		}
		if ch.ID == m.ChapterID {
			currentFull = p
		}
		chapters = append(chapters, ch)
	}
	m.Chapters = chapters
	if m.ChapterID != "" && currentFull == "" {
		return errors.New("reader chapter is not accessible")
	}
	pages := make([]driver.JMReaderPage, 0, len(m.Pages))
	ip := utils.ClientIP(c.Request)
	for _, pg := range m.Pages {
		p := path.Join(currentFull, pg.Name)
		if !allowed(p) {
			continue
		}
		if m.Mode == "server" {
			pg.URL = fmt.Sprintf("%s/d%s?sign=%s", common.GetApiUrl(c), utils.EncodePath(p, true), sign.SignDownload(user, p, ip))
			pg.Segments = 0
		}
		pages = append(pages, pg)
	}
	m.Pages = pages
	if m.Mode == "server" && m.Cover != "" {
		p := path.Join(albumFull, "cover.jpg")
		if allowed(p) {
			m.Cover = fmt.Sprintf("%s/d%s?sign=%s", common.GetApiUrl(c), utils.EncodePath(p, true), sign.SignDownload(user, p, ip))
		} else {
			m.Cover = ""
		}
	}
	return nil
}
