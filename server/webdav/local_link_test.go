package webdav

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestRedirectPolicyServesLocalReadme(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "README.md"), []byte("# Readme\n"), 0600))
	conf.Conf = conf.DefaultConfig(t.TempDir())
	op.Cache.ClearAll()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	addition, _ := json.Marshal(map[string]string{"root_folder_path": root})
	id, err := op.CreateStorage(context.Background(), model.Storage{Driver: "Local", MountPath: "/readme-test", Addition: string(addition)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = op.DeleteStorageById(context.Background(), id); conn, _ := database.DB(); conn.Close() })
	user := &model.User{ID: 11, Username: "admin", Role: model.ADMIN, BasePath: "/readme-test"}
	storage, err := op.GetStorageByMountPath("/readme-test")
	require.NoError(t, err)
	storage.GetStorage().WebdavPolicy = "302_redirect"
	handler := &Handler{Prefix: "/dav", LockSystem: NewMemLS()}
	for _, method := range []string{"GET", "HEAD"} {
		request := httptest.NewRequest(method, "http://example.com/dav/README.md", nil)
		request.Header.Set("Range", "bytes=2-5")
		request = request.WithContext(context.WithValue(request.Context(), conf.UserKey, user))
		response := httptest.NewRecorder()
		status, err := handler.handleGetHeadPost(response, request)
		require.NoError(t, err)
		require.Zero(t, status)
		require.Empty(t, response.Header().Get("Location"))
		if method == "GET" {
			require.Equal(t, 206, response.Code)
			require.Equal(t, "Read", response.Body.String())
		} else {
			require.Contains(t, []int{200, 206}, response.Code)
			require.Empty(t, response.Body.String())
		}
	}
}
