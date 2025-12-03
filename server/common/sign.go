package common

import (
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
)

func Sign(obj model.Obj, parent string, encrypt bool) string {
	if obj.IsDir() || (!encrypt && !setting.GetBool(conf.SignAll)) {
		return ""
	}
	return sign.Sign(stdpath.Join(parent, obj.GetName()))
}

// SignDownload generates a user-bound download token.
func SignDownload(obj model.Obj, parent string, encrypt bool, user *model.User) string {
	if obj.IsDir() || (!encrypt && !setting.GetBool(conf.SignAll)) {
		return ""
	}
	return sign.SignDownload(user, stdpath.Join(parent, obj.GetName()))
}
