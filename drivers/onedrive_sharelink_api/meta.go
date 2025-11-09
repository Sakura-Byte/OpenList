package onedrive_sharelink_api

import (
	"net/http"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootPath
	RedirectUrl       string
	ChunkSize         int64  `json:"chunk_size" type:"number" default:"5"`
	ShareLinkURL      string `json:"url" required:"true"`
	ShareLinkPassword string `json:"password"`
	UseSharelinkRoot  bool   `json:"use_sharelink_root"`
	Headers           http.Header
	BaseUrl           string
	SharelinkRootPath string
}

var config = driver.Config{
	Name:        "Onedrive Sharelink API",
	LocalSort:   true,
	DefaultRoot: "/",
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &OnedriveSharelinkAPI{}
	})
}
