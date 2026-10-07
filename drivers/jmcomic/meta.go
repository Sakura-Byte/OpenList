package jmcomic

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootPath
	APIURL                string  `json:"api_url" default:"https://www.cdnhjk.net" required:"true"`
	Mode                  string  `json:"mode" type:"select" options:"client,server" default:"client" required:"true"`
	CacheRoot             string  `json:"cache_root"`
	Tags                  string  `json:"tags" type:"text"`
	RequestsPerSecond     float64 `json:"requests_per_second" type:"float" default:"1" required:"true"`
	SyncHours             int     `json:"sync_hours" type:"number" default:"24"`
	FullSyncDays          int     `json:"full_sync_days" type:"number" default:"7"`
	FeedHours             int     `json:"feed_hours" type:"number" default:"6"`
	FeedLimit             int     `json:"feed_limit" type:"number" default:"200"`
	InputMiB              int     `json:"input_mib" type:"number" default:"16"`
	MaxPixels             int64   `json:"max_pixels" type:"number" default:"16000000"`
	OutputMiB             int     `json:"output_mib" type:"number" default:"32"`
	ConversionConcurrency int     `json:"conversion_concurrency" type:"number" default:"1"`
}

var config = driver.Config{Name: "JMComic", DefaultRoot: "/", NoUpload: true, NoCache: true, CheckStatus: true, OnlyProxy: true, NoLinkURL: true}

func init() { op.RegisterDriver(func() driver.Driver { return &JMComic{} }) }
