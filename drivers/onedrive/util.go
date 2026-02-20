package onedrive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	stdpath "path"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	streamPkg "github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/avast/retry-go"
	"github.com/go-resty/resty/v2"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

var onedriveHostMap = map[string]Host{
	"global": {
		Oauth: "https://login.microsoftonline.com",
		Api:   "https://graph.microsoft.com",
	},
	"cn": {
		Oauth: "https://login.chinacloudapi.cn",
		Api:   "https://microsoftgraph.chinacloudapi.cn",
	},
	"us": {
		Oauth: "https://login.microsoftonline.us",
		Api:   "https://graph.microsoft.us",
	},
	"de": {
		Oauth: "https://login.microsoftonline.de",
		Api:   "https://graph.microsoft.de",
	},
}

func (d *Onedrive) GetMetaUrl(auth bool, path string) string {
	host, _ := onedriveHostMap[d.Region]
	path = utils.EncodePath(path, true)
	if auth {
		return host.Oauth
	}
	if d.IsSharepoint {
		if path == "/" || path == "\\" {
			return fmt.Sprintf("%s/v1.0/sites/%s/drive/root", host.Api, d.SiteId)
		} else {
			return fmt.Sprintf("%s/v1.0/sites/%s/drive/root:%s:", host.Api, d.SiteId, path)
		}
	} else {
		if path == "/" || path == "\\" {
			return fmt.Sprintf("%s/v1.0/me/drive/root", host.Api)
		} else {
			return fmt.Sprintf("%s/v1.0/me/drive/root:%s:", host.Api, path)
		}
	}
}

func (d *Onedrive) refreshToken() error {
	var err error
	for i := 0; i < 3; i++ {
		err = d._refreshToken()
		if err == nil {
			break
		}
	}
	return err
}

func (d *Onedrive) _refreshToken() error {
	// 使用在线API刷新Token，无需ClientID和ClientSecret
	if d.UseOnlineAPI && len(d.APIAddress) > 0 {
		u := d.APIAddress
		var resp struct {
			RefreshToken string `json:"refresh_token"`
			AccessToken  string `json:"access_token"`
			ErrorMessage string `json:"text"`
		}
		_, err := base.RestyClient.R().
			SetResult(&resp).
			SetQueryParams(map[string]string{
				"refresh_ui": d.RefreshToken,
				"server_use": "true",
				"driver_txt": "onedrive_pr",
			}).
			Get(u)
		if err != nil {
			return err
		}
		if resp.RefreshToken == "" || resp.AccessToken == "" {
			if resp.ErrorMessage != "" {
				return fmt.Errorf("failed to refresh token: %s", resp.ErrorMessage)
			}
			return fmt.Errorf("empty token returned from official API, a wrong refresh token may have been used")
		}
		d.AccessToken = resp.AccessToken
		d.RefreshToken = resp.RefreshToken
		op.MustSaveDriverStorage(d)
		return nil
	}
	// 使用本地客户端的情况下检查是否为空
	if d.ClientID == "" || d.ClientSecret == "" {
		return fmt.Errorf("empty ClientID or ClientSecret")
	}
	// 走原有的刷新逻辑
	url := d.GetMetaUrl(true, "") + "/common/oauth2/v2.0/token"
	var resp base.TokenResp
	var e TokenErr
	_, err := base.RestyClient.R().SetResult(&resp).SetError(&e).SetFormData(map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     d.ClientID,
		"client_secret": d.ClientSecret,
		"redirect_uri":  d.RedirectUri,
		"refresh_token": d.RefreshToken,
	}).Post(url)
	if err != nil {
		return err
	}
	if e.Error != "" {
		return fmt.Errorf("%s", e.ErrorDescription)
	}
	if resp.RefreshToken == "" {
		return errs.EmptyToken
	}
	d.RefreshToken, d.AccessToken = resp.RefreshToken, resp.AccessToken
	op.MustSaveDriverStorage(d)
	return nil
}

func (d *Onedrive) Request(url string, method string, callback base.ReqCallback, resp interface{}, noRetry ...bool) ([]byte, error) {
	if d.ref != nil {
		return d.ref.Request(url, method, callback, resp)
	}
	req := base.RestyClient.R()
	req.SetHeader("Authorization", "Bearer "+d.AccessToken)
	if callback != nil {
		callback(req)
	}
	if resp != nil {
		req.SetResult(resp)
	}
	var e RespErr
	req.SetError(&e)
	res, err := req.Execute(method, url)
	if err != nil {
		return nil, err
	}
	if e.Error.Code != "" {
		if e.Error.Code == "InvalidAuthenticationToken" && !utils.IsBool(noRetry...) {
			err = d.refreshToken()
			if err != nil {
				return nil, err
			}
			return d.Request(url, method, callback, resp)
		}
		return nil, errors.New(e.Error.Message)
	}
	return res.Body(), nil
}

func (d *Onedrive) getFiles(path string) ([]File, error) {
	var res []File
	nextLink := d.GetMetaUrl(false, path) + "/children?$top=1000&$expand=thumbnails($select=medium)&$select=id,name,size,fileSystemInfo,content.downloadUrl,file,parentReference"
	for nextLink != "" {
		var files Files
		_, err := d.Request(nextLink, http.MethodGet, nil, &files)
		if err != nil {
			return nil, err
		}
		res = append(res, files.Value...)
		nextLink = files.NextLink
	}
	return res, nil
}

func onedriveListRRequestAttempts() int {
	if conf.Conf != nil && conf.Conf.IndexListR.RequestMaxAttempts > 0 {
		return conf.Conf.IndexListR.RequestMaxAttempts
	}
	return 3
}

func onedriveListRRetryDelay() time.Duration {
	if conf.Conf != nil && conf.Conf.IndexListR.RequestRetryDelayMs > 0 {
		return time.Duration(conf.Conf.IndexListR.RequestRetryDelayMs) * time.Millisecond
	}
	return 300 * time.Millisecond
}

func (d *Onedrive) requestDeltaPageWithRetry(ctx context.Context, nextLink string) (Files, error) {
	attempts := onedriveListRRequestAttempts()
	retryDelay := onedriveListRRetryDelay()
	var (
		files   Files
		lastErr error
	)
	for attempt := 1; attempt <= attempts; attempt++ {
		if utils.IsCanceled(ctx) {
			return Files{}, ctx.Err()
		}
		files = Files{}
		_, err := d.Request(nextLink, http.MethodGet, nil, &files)
		if err == nil {
			return files, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		log.Warnf("onedrive ListR delta request failed, retrying (%d/%d), err=%v",
			attempt, attempts, err)
		backoff := time.Duration(attempt) * retryDelay
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Files{}, ctx.Err()
		case <-timer.C:
		}
	}
	return Files{}, lastErr
}

func (d *Onedrive) ListR(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	_ = args
	if maxDepth == 0 {
		return nil
	}
	startPath := utils.FixAndCleanPath(dir.GetPath())
	if startPath == "" {
		startPath = "/"
	}
	directoryID := dir.GetID()
	if directoryID == "" {
		root, err := d.GetFile(startPath)
		if err != nil {
			return err
		}
		directoryID = root.Id
	}
	if directoryID == "" {
		return fmt.Errorf("onedrive ListR: empty directory id")
	}

	nextLink := d.GetMetaUrl(false, "/") + "/delta?$top=1000&$select=id,name,size,fileSystemInfo,file,parentReference,deleted,remoteItem"
	items := make([]File, 0)
	for nextLink != "" {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		files, err := d.requestDeltaPageWithRetry(ctx, nextLink)
		if err != nil {
			return err
		}
		items = append(items, files.Value...)
		nextLink = files.NextLink
	}

	entriesByParent, sharedDirs := resolveOnedriveListRItems(startPath, directoryID, items, maxDepth)
	if startPath != "/" && len(entriesByParent) == 0 {
		return d.listRByList(ctx, dir, maxDepth, callback)
	}

	if callback == nil {
		return nil
	}
	parentPaths := make([]string, 0, len(entriesByParent))
	for parentPath := range entriesByParent {
		parentPaths = append(parentPaths, parentPath)
	}
	sort.Strings(parentPaths)
	for i := range parentPaths {
		if err := callback(parentPaths[i], entriesByParent[parentPaths[i]]); err != nil {
			return err
		}
	}
	for i := range sharedDirs {
		remainingDepth := -1
		if maxDepth >= 0 {
			depth := listRPathDepth(startPath, sharedDirs[i].GetPath())
			remainingDepth = maxDepth - depth
			if remainingDepth <= 0 {
				continue
			}
		}
		if err := d.listRByList(ctx, sharedDirs[i], remainingDepth, callback); err != nil {
			return err
		}
	}
	return nil
}

func (d *Onedrive) listRByList(ctx context.Context, dir model.Obj, maxDepth int, callback driver.ListRCallback) error {
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
		for i := range objs {
			raw := model.UnwrapObj(objs[i])
			if raw.GetPath() != "" {
				continue
			}
			if setter, ok := raw.(model.SetPath); ok {
				setter.SetPath(stdpath.Join(parentPath, raw.GetName()))
			}
		}
		if callback != nil && len(objs) > 0 {
			if err := callback(parentPath, objs); err != nil {
				return err
			}
		}
		nextDepth := current.depth + 1
		if maxDepth >= 0 && nextDepth >= maxDepth {
			continue
		}
		for i := range objs {
			if objs[i].IsDir() {
				queue = append(queue, task{obj: objs[i], depth: nextDepth})
			}
		}
	}
	return nil
}

func listRPathDepth(basePath, fullPath string) int {
	relPath := utils.RelativePath(basePath, fullPath)
	if relPath == "" {
		return 0
	}
	return strings.Count(relPath, "/") + 1
}

func resolveOnedriveListRItems(startPath, directoryID string, items []File, maxDepth int) (map[string][]model.Obj, []model.Obj) {
	entriesByParent := make(map[string][]model.Obj)
	handledItemIDs := make(map[string]struct{})
	items = compactOnedriveDeltaItemsFirstSeen(items)
	sharedDirs := make([]model.Obj, 0)
	seenSharedDirIDs := make(map[string]struct{})
	idToPath := map[string]string{
		directoryID: startPath,
	}
	unresolved := append([]File(nil), items...)

	for len(unresolved) > 0 {
		progressed := false
		nextUnresolved := make([]File, 0)
		for i := range unresolved {
			item := unresolved[i]
			if item.Id == "" {
				continue
			}
			if item.Id == directoryID {
				continue
			}
			if _, ok := handledItemIDs[item.Id]; ok {
				continue
			}
			if item.Deleted != nil {
				handledItemIDs[item.Id] = struct{}{}
				continue
			}
			parentPath, ok := idToPath[item.ParentReference.Id]
			if !ok {
				if parsedPath, parsed := parseOnedriveParentPath(item.ParentReference.Path); parsed &&
					isWithinOnedriveListRSubtree(startPath, parsedPath) {
					parentPath = parsedPath
					idToPath[item.ParentReference.Id] = parsedPath
					ok = true
				}
			}
			if !ok {
				nextUnresolved = append(nextUnresolved, item)
				continue
			}

			fullPath := stdpath.Join(parentPath, item.Name)
			depth := listRPathDepth(startPath, fullPath)
			if maxDepth >= 0 && depth > maxDepth {
				handledItemIDs[item.Id] = struct{}{}
				continue
			}

			obj := fileToObj(item, item.ParentReference.Id)
			obj.Path = fullPath
			entriesByParent[parentPath] = append(entriesByParent[parentPath], obj)
			handledItemIDs[item.Id] = struct{}{}
			if item.RemoteItem != nil && item.RemoteItem.Folder != nil && obj.IsDir() {
				if _, seen := seenSharedDirIDs[obj.GetID()]; !seen {
					seenSharedDirIDs[obj.GetID()] = struct{}{}
					sharedDirs = append(sharedDirs, obj)
				}
			}

			if obj.IsDir() && (maxDepth < 0 || depth < maxDepth) {
				idToPath[item.Id] = fullPath
			}
			progressed = true
		}
		if !progressed {
			break
		}
		unresolved = nextUnresolved
	}
	return entriesByParent, sharedDirs
}

func compactOnedriveDeltaItemsFirstSeen(items []File) []File {
	compacted := make([]File, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for i := range items {
		if items[i].Id == "" {
			continue
		}
		if _, ok := seen[items[i].Id]; !ok {
			seen[items[i].Id] = struct{}{}
			compacted = append(compacted, items[i])
		}
	}
	return compacted
}

func parseOnedriveParentPath(parentPath string) (string, bool) {
	parentPath = strings.TrimSpace(parentPath)
	if parentPath == "" {
		return "", false
	}
	if strings.HasSuffix(parentPath, "/root") {
		return "/", true
	}
	idx := strings.Index(parentPath, ":/")
	if idx < 0 {
		return "", false
	}
	return utils.FixAndCleanPath(parentPath[idx+1:]), true
}

func isWithinOnedriveListRSubtree(rootPath, path string) bool {
	rootPath = utils.FixAndCleanPath(rootPath)
	path = utils.FixAndCleanPath(path)
	if rootPath == "/" {
		return true
	}
	return path == rootPath || strings.HasPrefix(path, utils.PathAddSeparatorSuffix(rootPath))
}

func (d *Onedrive) GetFile(path string) (*File, error) {
	var file File
	u := d.GetMetaUrl(false, path)
	_, err := d.Request(u, http.MethodGet, nil, &file)
	return &file, err
}

func (d *Onedrive) upSmall(ctx context.Context, dstDir model.Obj, stream model.FileStreamer) error {
	filepath := stdpath.Join(dstDir.GetPath(), stream.GetName())
	// 1. upload new file
	// ApiDoc: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content?view=odsp-graph-online
	url := d.GetMetaUrl(false, filepath) + "/content"
	_, err := d.Request(url, http.MethodPut, func(req *resty.Request) {
		req.SetBody(driver.NewLimitedUploadStream(ctx, stream)).SetContext(ctx)
	}, nil)
	if err != nil {
		return fmt.Errorf("onedrive: Failed to upload new file(path=%v): %w", filepath, err)
	}

	// 2. update metadata
	err = d.updateMetadata(ctx, stream, filepath)
	if err != nil {
		return fmt.Errorf("onedrive: Failed to update file(path=%v) metadata: %w", filepath, err)
	}
	return nil
}

func (d *Onedrive) updateMetadata(ctx context.Context, stream model.FileStreamer, filepath string) error {
	url := d.GetMetaUrl(false, filepath)
	metadata := toAPIMetadata(stream)
	// ApiDoc: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_update?view=odsp-graph-online
	_, err := d.Request(url, http.MethodPatch, func(req *resty.Request) {
		req.SetBody(metadata).SetContext(ctx)
	}, nil)
	return err
}

func toAPIMetadata(stream model.FileStreamer) Metadata {
	metadata := Metadata{
		FileSystemInfo: &FileSystemInfoFacet{},
	}
	if !stream.ModTime().IsZero() {
		metadata.FileSystemInfo.LastModifiedDateTime = stream.ModTime()
	}
	if !stream.CreateTime().IsZero() {
		metadata.FileSystemInfo.CreatedDateTime = stream.CreateTime()
	}
	if stream.CreateTime().IsZero() && !stream.ModTime().IsZero() {
		metadata.FileSystemInfo.CreatedDateTime = stream.CreateTime()
	}
	return metadata
}

func (d *Onedrive) upBig(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	url := d.GetMetaUrl(false, stdpath.Join(dstDir.GetPath(), stream.GetName())) + "/createUploadSession"
	metadata := map[string]any{"item": toAPIMetadata(stream)}
	res, err := d.Request(url, http.MethodPost, func(req *resty.Request) {
		req.SetBody(metadata).SetContext(ctx)
	}, nil)
	if err != nil {
		return err
	}
	DEFAULT := d.ChunkSize * 1024 * 1024
	ss, err := streamPkg.NewStreamSectionReader(stream, int(DEFAULT), &up)
	if err != nil {
		return err
	}

	uploadUrl := jsoniter.Get(res, "uploadUrl").ToString()
	var finish int64 = 0
	for finish < stream.GetSize() {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		left := stream.GetSize() - finish
		byteSize := min(left, DEFAULT)
		utils.Log.Debugf("[Onedrive] upload range: %d-%d/%d", finish, finish+byteSize-1, stream.GetSize())
		rd, err := ss.GetSectionReader(finish, byteSize)
		if err != nil {
			return err
		}
		err = retry.Do(
			func() error {
				rd.Seek(0, io.SeekStart)
				req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadUrl, driver.NewLimitedUploadStream(ctx, rd))
				if err != nil {
					return err
				}
				req.ContentLength = byteSize
				req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", finish, finish+byteSize-1, stream.GetSize()))
				res, err := base.HttpClient.Do(req)
				if err != nil {
					return err
				}
				defer res.Body.Close()
				// https://learn.microsoft.com/zh-cn/onedrive/developer/rest-api/api/driveitem_createuploadsession
				switch {
				case res.StatusCode >= 500 && res.StatusCode <= 504:
					return fmt.Errorf("server error: %d", res.StatusCode)
				case res.StatusCode != 201 && res.StatusCode != 202 && res.StatusCode != 200:
					data, _ := io.ReadAll(res.Body)
					return errors.New(string(data))
				default:
					return nil
				}
			},
			retry.Context(ctx),
			retry.Attempts(3),
			retry.DelayType(retry.BackOffDelay),
			retry.Delay(time.Second),
		)
		ss.FreeSectionReader(rd)
		if err != nil {
			return err
		}
		finish += byteSize
		up(float64(finish) * 100 / float64(stream.GetSize()))
	}
	return nil
}

func (d *Onedrive) getDrive(ctx context.Context) (*DriveResp, error) {
	var api string
	host, _ := onedriveHostMap[d.Region]
	if d.IsSharepoint {
		api = fmt.Sprintf("%s/v1.0/sites/%s/drive", host.Api, d.SiteId)
	} else {
		api = fmt.Sprintf("%s/v1.0/me/drive", host.Api)
	}
	var resp DriveResp
	_, err := d.Request(api, http.MethodGet, func(req *resty.Request) {
		req.SetContext(ctx)
	}, &resp, true)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (d *Onedrive) getDirectUploadInfo(ctx context.Context, path string) (*model.HttpDirectUploadInfo, error) {
	// Create upload session
	url := d.GetMetaUrl(false, path) + "/createUploadSession"
	metadata := map[string]any{
		"item": map[string]any{
			"@microsoft.graph.conflictBehavior": "rename",
		},
	}

	res, err := d.Request(url, http.MethodPost, func(req *resty.Request) {
		req.SetBody(metadata).SetContext(ctx)
	}, nil)
	if err != nil {
		return nil, err
	}

	uploadUrl := jsoniter.Get(res, "uploadUrl").ToString()
	if uploadUrl == "" {
		return nil, fmt.Errorf("failed to get upload URL from response")
	}
	return &model.HttpDirectUploadInfo{
		UploadURL: uploadUrl,
		ChunkSize: d.ChunkSize * 1024 * 1024, // Convert MB to bytes
		Method:    "PUT",
	}, nil
}
