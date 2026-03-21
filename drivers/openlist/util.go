package openlist

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	stdpath "path"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

const upstreamUpdateSiteProbeTTL = 5 * time.Minute

type upstreamUpdateSiteCapability struct {
	Supported bool
	ExpiresAt time.Time
}

var upstreamUpdateSiteCapabilityCache sync.Map

func (d *OpenList) login(ctx context.Context) error {
	if d.Username == "" {
		return nil
	}
	var resp common.Resp[LoginResp]
	_, _, err := d.request(ctx, "/auth/login", http.MethodPost, func(req *resty.Request) {
		req.SetResult(&resp).SetBody(base.Json{
			"username": d.Username,
			"password": d.Password,
		})
	})
	if err != nil {
		return err
	}
	d.Token = resp.Data.Token
	op.MustSaveDriverStorage(d)
	return nil
}

func (d *OpenList) request(ctx context.Context, api, method string, callback base.ReqCallback, retry ...bool) ([]byte, int, error) {
	url := d.Address + "/api" + api
	if err := d.WaitLimit(ctx); err != nil {
		return nil, 0, err
	}
	req := base.RestyClient.R().SetContext(ctx)
	req.SetHeader("Authorization", d.Token)
	if callback != nil {
		callback(req)
	}
	res, err := req.Execute(method, url)
	if err != nil {
		code := 0
		if res != nil {
			code = res.StatusCode()
		}
		return nil, code, err
	}
	log.Debugf("[openlist] response body: %s", res.String())
	if res.StatusCode() >= 400 {
		return nil, res.StatusCode(), fmt.Errorf("request failed, status: %s", res.Status())
	}
	code := utils.Json.Get(res.Body(), "code").ToInt()
	if code != 200 {
		if (code == 401 || code == 403) && !utils.IsBool(retry...) {
			err = d.login(ctx)
			if err != nil {
				return nil, code, err
			}
			return d.request(ctx, api, method, callback, true)
		}
		return nil, code, fmt.Errorf("request failed,code: %d, message: %s", code, utils.Json.Get(res.Body(), "message").ToString())
	}
	return res.Body(), 200, nil
}

func (d *OpenList) ListRForUpdateSite(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	_ = args
	if maxDepth == 0 || callback == nil {
		return nil
	}
	if d.supportsUpstreamUpdateSiteScan(ctx) {
		return d.proxyUpdateSiteScan(ctx, dir.GetPath(), maxDepth, callback)
	}
	return d.scanUpdateSiteByList(ctx, dir, maxDepth, callback)
}

func (d *OpenList) supportsUpstreamUpdateSiteScan(ctx context.Context) bool {
	key := d.Address
	if cached, ok := upstreamUpdateSiteCapabilityCache.Load(key); ok {
		entry := cached.(upstreamUpdateSiteCapability)
		if time.Now().Before(entry.ExpiresAt) {
			return entry.Supported
		}
	}
	supported := d.probeUpstreamUpdateSiteScan(ctx)
	upstreamUpdateSiteCapabilityCache.Store(key, upstreamUpdateSiteCapability{
		Supported: supported,
		ExpiresAt: time.Now().Add(upstreamUpdateSiteProbeTTL),
	})
	return supported
}

func (d *OpenList) probeUpstreamUpdateSiteScan(ctx context.Context) bool {
	var raw map[string]any
	body, _, err := d.request(ctx, "/admin/update_site/list_recursive", http.MethodPost, func(req *resty.Request) {
		req.SetBody(UpdateSiteRecursiveListReq{
			Path:         "/",
			MaxDepth:     1,
			IncludeThumb: false,
			ChunkLimit:   1,
			Cursor:       "",
		})
	})
	if err != nil {
		return false
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	data, ok := raw["data"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = data["entries"]
	return ok
}

func (d *OpenList) proxyUpdateSiteScan(ctx context.Context, reqPath string, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	cursor := ""
	for {
		var resp UpdateSiteRecursiveListResp
		_, _, err := d.request(ctx, "/admin/update_site/list_recursive", http.MethodPost, func(req *resty.Request) {
			req.SetResult(&resp).SetBody(UpdateSiteRecursiveListReq{
				Path:         reqPath,
				MaxDepth:     maxDepth,
				IncludeThumb: true,
				ChunkLimit:   64,
				Cursor:       cursor,
			})
		})
		if err != nil {
			return err
		}
		if resp.Code != 200 {
			return fmt.Errorf("upstream update-site scan failed: %s", resp.Message)
		}
		if len(resp.Data.Entries) > 0 {
			entries := make([]driver.UpdateSiteEntry, len(resp.Data.Entries))
			for i, entry := range resp.Data.Entries {
				entries[i] = entry
				entries[i].Debug = &driver.UpdateSiteEntryDebug{
					Engine:        "upstream_update_site_scan",
					StorageMount:  d.GetStorage().MountPath,
					StorageDriver: d.Config().Name,
				}
			}
			if err := callback(driver.UpdateSiteChunk{Entries: entries}); err != nil {
				return err
			}
		}
		if resp.Data.Done {
			return nil
		}
		cursor = strings.TrimSpace(resp.Data.Cursor)
		if cursor == "" {
			return fmt.Errorf("upstream update-site scan returned no cursor before completion")
		}
	}
}

func (d *OpenList) scanUpdateSiteByList(ctx context.Context, dir model.Obj, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	type task struct {
		obj   model.Obj
		depth int
	}
	queue := []task{{obj: dir, depth: 0}}
	for len(queue) > 0 {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		current := queue[0]
		queue = queue[1:]
		parentPath := utils.FixAndCleanPath(current.obj.GetPath())
		if parentPath == "" {
			parentPath = "/"
		}
		objs, err := d.List(ctx, current.obj, model.ListArgs{})
		if err != nil {
			return err
		}
		entries := make([]driver.UpdateSiteEntry, 0, len(objs))
		for _, obj := range objs {
			thumb, _ := model.GetThumb(obj)
			entries = append(entries, driver.UpdateSiteEntry{
				VisiblePath: stdpath.Join(parentPath, obj.GetName()),
				ParentPath:  parentPath,
				Name:        obj.GetName(),
				IsDir:       obj.IsDir(),
				Size:        obj.GetSize(),
				Modified:    obj.ModTime(),
				Thumb:       thumb,
				Debug: &driver.UpdateSiteEntryDebug{
					Engine:        "list",
					StorageMount:  d.GetStorage().MountPath,
					StorageDriver: d.Config().Name,
				},
			})
			if obj.IsDir() && (maxDepth < 0 || current.depth+1 < maxDepth) {
				queue = append(queue, task{
					obj: &model.Object{
						Name:     obj.GetName(),
						Path:     stdpath.Join(parentPath, obj.GetName()),
						Modified: obj.ModTime(),
						Size:     obj.GetSize(),
						IsFolder: true,
					},
					depth: current.depth + 1,
				})
			}
		}
		if len(entries) > 0 {
			if err := callback(driver.UpdateSiteChunk{Entries: entries}); err != nil {
				return err
			}
		}
	}
	return nil
}
