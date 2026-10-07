package handles

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type readerFixture struct {
	model.Storage
	driver.RootPath
}

func (d *readerFixture) Config() driver.Config {
	return driver.Config{Name: "JMReaderFixture", DefaultRoot: "/", NoCache: true, NoUpload: true}
}
func (d *readerFixture) GetAddition() driver.Additional { return &d.RootPath }
func (d *readerFixture) Init(context.Context) error     { return nil }
func (d *readerFixture) Drop(context.Context) error     { return nil }
func (d *readerFixture) List(context.Context, model.Obj, model.ListArgs) ([]model.Obj, error) {
	return []model.Obj{}, nil
}
func (d *readerFixture) Link(context.Context, model.Obj, model.LinkArgs) (*model.Link, error) {
	return nil, fmt.Errorf("fixture has no content")
}
func TestReaderManifestBasePathsPasswordsAndHiddenChapters(t *testing.T) {
	setupProxyTest(t)
	op.RegisterDriver(func() driver.Driver { return &readerFixture{} })
	database, e := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())), &gorm.Config{})
	require.NoError(t, e)
	previousDB := db.GetDb()
	db.Init(database)
	id, e := op.CreateStorage(context.Background(), model.Storage{Driver: "JMReaderFixture", MountPath: "/fixture", Addition: `{"root_folder_path":"/"}`})
	require.NoError(t, e)
	t.Cleanup(func() {
		_ = op.DeleteStorageById(context.Background(), id)
		if previousDB != nil {
			db.Init(previousDB)
		}
		c, _ := database.DB()
		_ = c.Close()
	})
	require.NoError(t, db.CreateMeta(&model.Meta{Path: "/fixture/work", Password: "secret", PSub: true, Hide: "^hidden$", HSub: true}))
	require.NoError(t, db.CreateMeta(&model.Meta{Path: "/fixture/标签", Hide: "^Hidden$", HSub: true}))
	user := &model.User{ID: 12, Role: model.GENERAL, BasePath: "/fixture"}
	out := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(out)
	c.Request = httptest.NewRequest("POST", "http://openlist.example/api/fs/other", nil)
	manifest := func(mode string) *driver.JMReaderManifest {
		return &driver.JMReaderManifest{Mode: mode, AlbumID: "42", ChapterID: "42", AlbumPath: "/work", Chapters: []driver.JMReaderChapter{{ID: "42", Name: "allowed", Path: "/work/allowed"}, {ID: "43", Name: "hidden", Path: "/work/hidden"}}, Pages: []driver.JMReaderPage{{Name: "0001.png", URL: "https://images.example/page.png", Segments: 4}}}
	}
	require.ErrorContains(t, prepareJMReader(c, user, "/fixture/work/allowed", "wrong", manifest("client")), "not accessible")
	m := manifest("client")
	m.TagLinks = []driver.JMReaderTag{{Name: "Visible", Path: "/标签/Visible"}, {Name: "Hidden", Path: "/标签/Hidden"}}
	require.NoError(t, prepareJMReader(c, user, "/fixture/work/allowed", "secret", m))
	require.Equal(t, "/work", m.AlbumPath)
	require.Len(t, m.Chapters, 1)
	require.Equal(t, "/work/allowed", m.Chapters[0].Path)
	require.Equal(t, "https://images.example/page.png", m.Pages[0].URL)
	require.Equal(t, []driver.JMReaderTag{{Name: "Visible", Path: "/标签/Visible"}}, m.TagLinks)
	aliasUser := *user
	aliasUser.BasePath = "/fixture/work"
	restricted := manifest("client")
	restricted.TagLinks = []driver.JMReaderTag{{Name: "Visible", Path: "/标签/Visible"}}
	require.NoError(t, prepareJMReader(c, &aliasUser, "/fixture/work/allowed", "secret", restricted))
	require.Empty(t, restricted.TagLinks)
	m = manifest("server")
	require.NoError(t, prepareJMReader(c, user, "/fixture/work/allowed", "secret", m))
	require.Contains(t, m.Pages[0].URL, "/d/fixture/work/allowed/0001.png?sign=")
	require.Zero(t, m.Pages[0].Segments)
	user.BasePath = "/fixture/work/allowed"
	require.ErrorContains(t, prepareJMReader(c, user, "/fixture/work/allowed", "secret", manifest("client")), "outside user base")
	_, e = jmcomicManager(id)
	require.ErrorContains(t, e, "active JMComic storage required")
}

type readerInfoObj struct {
	model.Object
	info *model.ReaderInfo
}

func (o *readerInfoObj) GetReaderInfo() *model.ReaderInfo { return o.info }
func TestObjectResponseAdvertisesReaderWithoutNameInference(t *testing.T) {
	setupProxyTest(t)
	user := &model.User{ID: 1, Role: model.ADMIN}
	info := &model.ReaderInfo{Kind: "album", Mode: "client", AlbumID: "42"}
	objects := []model.Obj{&readerInfoObj{Object: model.Object{Name: "Opaque name", IsFolder: true}, info: info}, &model.Object{Name: "JM42 Title.jpg"}}
	resp := toObjsResp(objects, "/fixture", false, user, "")
	require.Equal(t, info, resp[0].Reader)
	require.Nil(t, resp[1].Reader)
	require.False(t, strings.Contains(resp[0].Name, "JM"))
}
