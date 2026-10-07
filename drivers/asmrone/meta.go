package asmrone

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootPath
	APIURL            string  `json:"api_url" default:"https://api.asmr.one" required:"true"`
	CacheRoot         string  `json:"cache_root"`
	RequestsPerSecond float64 `json:"requests_per_second" type:"float" default:"1" required:"true"`
	SyncHours         int     `json:"sync_hours" type:"number" default:"24" required:"true"`
	FullSyncDays      int     `json:"full_sync_days" type:"number" default:"7" required:"true"`
}

var config = driver.Config{Name: "ASMR.ONE", DefaultRoot: "/", LocalSort: true, NoUpload: true, NoCache: true, CheckStatus: true, ProxyRangeOption: true}

func init() { op.RegisterDriver(func() driver.Driver { return &ASMRONE{} }) }
