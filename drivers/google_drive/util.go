package google_drive

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	stdpath "path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/avast/retry-go"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/golang-jwt/jwt/v4"
	log "github.com/sirupsen/logrus"
)

// do others that not defined in Driver interface

// Google Drive API field constants
const (
	// File list query fields
	FilesListFields = "files(id,name,mimeType,parents,size,modifiedTime,createdTime,thumbnailLink,shortcutDetails,md5Checksum,sha1Checksum,sha256Checksum),nextPageToken"
	// Single file query fields
	FileInfoFields = "id,name,mimeType,size,md5Checksum,sha1Checksum,sha256Checksum"
	// Number of parent IDs queried in one ListR batch.
	listRGrouping = 50
	// Default number of concurrent workers for GoogleDrive ListR.
	defaultListRCheckers = 8
	// Input queue size for GoogleDrive ListR workers.
	listRInputBuffer = 1000
	// Default max attempts for a single ListR listing request.
	defaultListRRequestAttempts = 3
	// Default retry delay for a single ListR listing request.
	defaultListRRequestRetryDelayMs = 300
)

type googleDriveServiceAccount struct {
	// Type                    string `json:"type"`
	// ProjectID               string `json:"project_id"`
	// PrivateKeyID            string `json:"private_key_id"`
	PrivateKey  string `json:"private_key"`
	ClientEMail string `json:"client_email"`
	// ClientID                string `json:"client_id"`
	// AuthURI                 string `json:"auth_uri"`
	TokenURI string `json:"token_uri"`
	// AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	// ClientX509CertURL       string `json:"client_x509_cert_url"`
}

func (d *GoogleDrive) refreshToken() error {
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
				"driver_txt": "googleui_go",
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

	// googleDriveServiceAccountFile gdsaFile
	gdsaFile, gdsaFileErr := os.Stat(d.RefreshToken)
	if gdsaFileErr == nil {
		gdsaFileThis := d.RefreshToken
		if gdsaFile.IsDir() {
			if len(d.ServiceAccountFileList) <= 0 {
				gdsaReadDir, gdsaDirErr := os.ReadDir(d.RefreshToken)
				if gdsaDirErr != nil {
					log.Error("read dir fail")
					return gdsaDirErr
				}
				var gdsaFileList []string
				for _, fi := range gdsaReadDir {
					if !fi.IsDir() {
						match, _ := regexp.MatchString("^.*\\.json$", fi.Name())
						if !match {
							continue
						}
						gdsaDirText := d.RefreshToken
						if d.RefreshToken[len(d.RefreshToken)-1:] != "/" {
							gdsaDirText = d.RefreshToken + "/"
						}
						gdsaFileList = append(gdsaFileList, gdsaDirText+fi.Name())
					}
				}
				d.ServiceAccountFileList = gdsaFileList
				gdsaFileThis = d.ServiceAccountFileList[d.ServiceAccountFile]
				d.ServiceAccountFile++
			} else {
				if d.ServiceAccountFile < len(d.ServiceAccountFileList) {
					d.ServiceAccountFile++
				} else {
					d.ServiceAccountFile = 0
				}
				gdsaFileThis = d.ServiceAccountFileList[d.ServiceAccountFile]
			}
		}

		gdsaFileThisContent, err := os.ReadFile(gdsaFileThis)
		if err != nil {
			return err
		}

		// Now let's unmarshal the data into `payload`
		var jsonData googleDriveServiceAccount
		err = utils.Json.Unmarshal(gdsaFileThisContent, &jsonData)
		if err != nil {
			return err
		}

		gdsaScope := "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/drive.appdata https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/drive.metadata https://www.googleapis.com/auth/drive.metadata.readonly https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.scripts"

		timeNow := time.Now()
		var timeStart int64 = timeNow.Unix()
		var timeEnd int64 = timeNow.Add(time.Minute * 60).Unix()

		// load private key from string
		privateKeyPem, _ := pem.Decode([]byte(jsonData.PrivateKey))
		privateKey, _ := x509.ParsePKCS8PrivateKey(privateKeyPem.Bytes)

		jwtToken := jwt.NewWithClaims(jwt.SigningMethodRS256,
			jwt.MapClaims{
				"iss":   jsonData.ClientEMail,
				"scope": gdsaScope,
				"aud":   jsonData.TokenURI,
				"exp":   timeEnd,
				"iat":   timeStart,
			})
		assertion, err := jwtToken.SignedString(privateKey)
		if err != nil {
			return err
		}

		var resp base.TokenResp
		var e TokenError
		res, err := base.RestyClient.R().SetResult(&resp).SetError(&e).
			SetFormData(map[string]string{
				"assertion":  assertion,
				"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
			}).Post(jsonData.TokenURI)
		if err != nil {
			return err
		}
		log.Debug(res.String())
		if e.Error != "" {
			return errors.New(e.Error)
		}
		d.AccessToken = resp.AccessToken
		return nil
	} else if os.IsExist(gdsaFileErr) {
		return gdsaFileErr
	}
	url := "https://www.googleapis.com/oauth2/v4/token"
	var resp base.TokenResp
	var e TokenError
	res, err := base.RestyClient.R().SetResult(&resp).SetError(&e).
		SetFormData(map[string]string{
			"client_id":     d.ClientID,
			"client_secret": d.ClientSecret,
			"refresh_token": d.RefreshToken,
			"grant_type":    "refresh_token",
		}).Post(url)
	if err != nil {
		return err
	}
	log.Debug(res.String())
	if e.Error != "" {
		return errors.New(e.Error)
	}
	d.AccessToken = resp.AccessToken
	return nil
}

func (d *GoogleDrive) request(url string, method string, callback base.ReqCallback, resp interface{}) ([]byte, error) {
	req := base.RestyClient.R()
	req.SetHeader("Authorization", "Bearer "+d.AccessToken)
	req.SetQueryParam("includeItemsFromAllDrives", "true")
	req.SetQueryParam("supportsAllDrives", "true")
	if callback != nil {
		callback(req)
	}
	if resp != nil {
		req.SetResult(resp)
	}
	var e Error
	req.SetError(&e)
	res, err := req.Execute(method, url)
	if err != nil {
		return nil, err
	}
	if e.Error.Code != 0 {
		if e.Error.Code == 401 {
			err = d.refreshToken()
			if err != nil {
				return nil, err
			}
			return d.request(url, method, callback, resp)
		}
		return nil, fmt.Errorf("%s: %v", e.Error.Message, e.Error.Errors)
	}
	return res.Body(), nil
}

func (d *GoogleDrive) getFiles(id string) ([]File, error) {
	orderBy := "folder,name,modifiedTime desc"
	if d.OrderBy != "" {
		orderBy = d.OrderBy + " " + d.OrderDirection
	}
	return d.getFilesByQuery(fmt.Sprintf("'%s' in parents and trashed = false", id), orderBy)
}

func (d *GoogleDrive) getFilesByParentIDs(parentIDs []string) ([]File, error) {
	if len(parentIDs) == 0 {
		return nil, nil
	}
	return d.getFilesByQuery(buildListRParentsQuery(parentIDs), "")
}

func googleDriveListRCheckers() int {
	if conf.Conf != nil && conf.Conf.IndexListR.Checkers > 0 {
		return conf.Conf.IndexListR.Checkers
	}
	return defaultListRCheckers
}

func googleDriveListRRequestAttempts() int {
	if conf.Conf != nil && conf.Conf.IndexListR.RequestMaxAttempts > 0 {
		return conf.Conf.IndexListR.RequestMaxAttempts
	}
	return defaultListRRequestAttempts
}

func googleDriveListRRetryDelay() time.Duration {
	if conf.Conf != nil && conf.Conf.IndexListR.RequestRetryDelayMs > 0 {
		return time.Duration(conf.Conf.IndexListR.RequestRetryDelayMs) * time.Millisecond
	}
	return time.Duration(defaultListRRequestRetryDelayMs) * time.Millisecond
}

func (d *GoogleDrive) getFilesByParentIDsWithRetry(ctx context.Context, parentIDs []string) ([]File, error) {
	attempts := googleDriveListRRequestAttempts()
	retryDelay := googleDriveListRRetryDelay()
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if utils.IsCanceled(ctx) {
			return nil, ctx.Err()
		}
		files, err := d.getFilesByParentIDs(parentIDs)
		if err == nil {
			return files, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		log.Warnf("google drive ListR request failed, retrying (%d/%d), parents=%d, err=%v",
			attempt, attempts, len(parentIDs), err)
		backoff := time.Duration(attempt) * retryDelay
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func buildListRParentsQuery(parentIDs []string) string {
	parentsExpr := make([]string, 0, len(parentIDs))
	for i := range parentIDs {
		parentsExpr = append(parentsExpr, fmt.Sprintf("'%s' in parents", parentIDs[i]))
	}
	return fmt.Sprintf("(%s) and trashed = false", strings.Join(parentsExpr, " or "))
}

func (d *GoogleDrive) getFilesByQuery(query, orderBy string) ([]File, error) {
	pageToken := ""
	res := make([]File, 0)
	for {
		var resp Files
		queryParams := map[string]string{
			"fields":   FilesListFields,
			"pageSize": "1000",
			"q":        query,
		}
		if pageToken != "" {
			queryParams["pageToken"] = pageToken
		}
		if orderBy != "" {
			queryParams["orderBy"] = orderBy
		}
		_, err := d.request("https://www.googleapis.com/drive/v3/files", http.MethodGet, func(req *resty.Request) {
			req.SetQueryParams(queryParams)
		}, &resp)
		if err != nil {
			return nil, err
		}
		d.fillShortcutFileInfo(&resp)
		res = append(res, resp.Files...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return res, nil
}

func (d *GoogleDrive) fillShortcutFileInfo(resp *Files) {
	shortcutTargetIds := make([]string, 0)
	shortcutIndices := make([]int, 0)
	for i := range resp.Files {
		if resp.Files[i].MimeType == "application/vnd.google-apps.shortcut" &&
			resp.Files[i].ShortcutDetails.TargetId != "" &&
			resp.Files[i].ShortcutDetails.TargetMimeType != "application/vnd.google-apps.folder" {
			shortcutTargetIds = append(shortcutTargetIds, resp.Files[i].ShortcutDetails.TargetId)
			shortcutIndices = append(shortcutIndices, i)
		}
	}
	if len(shortcutTargetIds) == 0 {
		return
	}
	targetFiles := d.batchGetTargetFilesInfo(shortcutTargetIds)
	for i, targetID := range shortcutTargetIds {
		targetFile, ok := targetFiles[targetID]
		if !ok {
			continue
		}
		fileIndex := shortcutIndices[i]
		if targetFile.Size != "" {
			resp.Files[fileIndex].Size = targetFile.Size
		}
		if targetFile.MD5Checksum != "" {
			resp.Files[fileIndex].MD5Checksum = targetFile.MD5Checksum
		}
		if targetFile.SHA1Checksum != "" {
			resp.Files[fileIndex].SHA1Checksum = targetFile.SHA1Checksum
		}
		if targetFile.SHA256Checksum != "" {
			resp.Files[fileIndex].SHA256Checksum = targetFile.SHA256Checksum
		}
	}
}

type googleDriveListRTask struct {
	id    string
	path  string
	depth int
}

func (d *GoogleDrive) ensureListRStateLocked() {
	if d.listRGrouping <= 0 {
		d.listRGrouping = listRGrouping
	}
	if d.listREmpties == nil {
		d.listREmpties = make(map[string]struct{})
	}
}

func (d *GoogleDrive) getListRGrouping() int {
	d.listRMu.Lock()
	defer d.listRMu.Unlock()
	d.ensureListRStateLocked()
	return d.listRGrouping
}

func (d *GoogleDrive) disableListRGrouping() bool {
	d.listRMu.Lock()
	defer d.listRMu.Unlock()
	d.ensureListRStateLocked()
	if d.listRGrouping == 1 {
		return false
	}
	d.listRGrouping = 1
	return true
}

func (d *GoogleDrive) markListREmpties(parentIDs []string) {
	d.listRMu.Lock()
	defer d.listRMu.Unlock()
	d.ensureListRStateLocked()
	for i := range parentIDs {
		d.listREmpties[parentIDs[i]] = struct{}{}
	}
}

func (d *GoogleDrive) maybeReenableListRGrouping(parentID string) bool {
	d.listRMu.Lock()
	defer d.listRMu.Unlock()
	d.ensureListRStateLocked()
	if _, ok := d.listREmpties[parentID]; !ok {
		return false
	}
	delete(d.listREmpties, parentID)
	if len(d.listREmpties) != 0 {
		return false
	}
	if d.listRGrouping == listRGrouping {
		return false
	}
	d.listRGrouping = listRGrouping
	return true
}

func (d *GoogleDrive) ListR(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	_ = args
	if maxDepth == 0 {
		return nil
	}
	rootID := dir.GetID()
	if rootID == "" {
		return fmt.Errorf("google drive ListR: empty root id")
	}
	rootPath := utils.FixAndCleanPath(dir.GetPath())
	if rootPath == "" {
		rootPath = "/"
	}

	type sendJobFunc func(job googleDriveListRTask)
	checkers := googleDriveListRCheckers()
	var (
		mu       sync.Mutex // protects in and overflow
		wg       sync.WaitGroup
		workerWg sync.WaitGroup
		in       = make(chan googleDriveListRTask, listRInputBuffer)
		out      = make(chan error, checkers)
		overflow = make([]googleDriveListRTask, 0)

		visitedMu sync.Mutex
		visited   = map[string]struct{}{rootID: {}}

		stopped atomic.Bool
		errOnce sync.Once
		retErr  error
	)

	fail := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			retErr = err
			stopped.Store(true)
			mu.Lock()
			if in != nil {
				close(in)
				in = nil
			}
			overflow = nil
			mu.Unlock()
		})
	}

	sendJob := func(job googleDriveListRTask) {
		if stopped.Load() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if in == nil || stopped.Load() {
			return
		}
		wg.Add(1)
		select {
		case in <- job:
		default:
			overflow = append(overflow, job)
			wg.Done()
		}
	}

	processChunk := func(chunk []googleDriveListRTask, grouping int, send sendJobFunc) error {
		parentIDs := make([]string, 0, len(chunk))
		taskByParentID := make(map[string]googleDriveListRTask, len(chunk))
		for i := range chunk {
			parentIDs = append(parentIDs, chunk[i].id)
			taskByParentID[chunk[i].id] = chunk[i]
		}
		files, err := d.getFilesByParentIDsWithRetry(ctx, parentIDs)
		if err != nil {
			return err
		}

		visitedMu.Lock()
		entriesByParent, nextTasks := mapGoogleDriveListRFiles(files, parentIDs, taskByParentID, maxDepth, visited)
		visitedMu.Unlock()

		foundItems := len(entriesByParent) > 0
		if len(parentIDs) > 1 && !foundItems && grouping > 1 {
			if d.disableListRGrouping() {
				log.Debugf("google drive ListR disabled grouping to work around grouped-empty query bug")
			}
			d.markListREmpties(parentIDs)
			for i := range chunk {
				send(chunk[i])
			}
			return nil
		}
		if grouping == 1 && len(parentIDs) == 1 && !foundItems {
			if d.maybeReenableListRGrouping(parentIDs[0]) {
				log.Debugf("google drive ListR re-enabled grouping after grouped-empty false detection")
			}
		}

		parentPaths := make([]string, 0, len(entriesByParent))
		for parent := range entriesByParent {
			parentPaths = append(parentPaths, parent)
		}
		sort.Strings(parentPaths)
		if callback != nil {
			for i := range parentPaths {
				if err := callback(parentPaths[i], entriesByParent[parentPaths[i]]); err != nil {
					return err
				}
			}
		}
		for i := range nextTasks {
			send(nextTasks[i])
		}
		return nil
	}

	worker := func() {
		defer workerWg.Done()
		for task := range in {
			chunk := []googleDriveListRTask{task}
			grouping := d.getListRGrouping()
			if grouping <= 0 {
				grouping = listRGrouping
			}
		waitloop:
			for i := 1; i < grouping; i++ {
				select {
				case t, ok := <-in:
					if !ok {
						break waitloop
					}
					chunk = append(chunk, t)
				default:
				}
			}

			if stopped.Load() {
				for range chunk {
					wg.Done()
				}
				continue
			}
			if utils.IsCanceled(ctx) {
				for range chunk {
					wg.Done()
				}
				fail(ctx.Err())
				out <- ctx.Err()
				return
			}
			err := processChunk(chunk, grouping, sendJob)
			for range chunk {
				wg.Done()
			}
			if err != nil {
				fail(err)
				out <- err
				return
			}
		}
		out <- nil
	}

	wg.Add(1)
	in <- googleDriveListRTask{id: rootID, path: rootPath, depth: 0}
	for i := 0; i < checkers; i++ {
		workerWg.Add(1)
		go worker()
	}

	go func() {
		wg.Wait()
		for {
			mu.Lock()
			if in == nil {
				mu.Unlock()
				return
			}
			if len(overflow) == 0 {
				close(in)
				in = nil
				mu.Unlock()
				return
			}
			l := min(len(overflow), listRInputBuffer/2)
			batch := append([]googleDriveListRTask(nil), overflow[:l]...)
			wg.Add(l)
			for _, job := range batch {
				in <- job
			}
			overflow = overflow[l:]
			mu.Unlock()
			wg.Wait()
		}
	}()

	for i := 0; i < checkers; i++ {
		if e := <-out; e != nil {
			fail(e)
		}
	}
	workerWg.Wait()
	close(out)
	return retErr
}

func mapGoogleDriveListRFiles(files []File, parentIDs []string, taskByParentID map[string]googleDriveListRTask,
	maxDepth int, visitedDirIDs map[string]struct{}) (map[string][]model.Obj, []googleDriveListRTask) {
	entriesByParent := make(map[string][]model.Obj)
	nextTasks := make([]googleDriveListRTask, 0)
	for i := range files {
		file := files[i]
		if len(file.Parents) == 0 {
			if len(parentIDs) != 1 {
				continue
			}
			rootParentTask, ok := taskByParentID[parentIDs[0]]
			if !ok || rootParentTask.path != "/" {
				continue
			}
			file.Parents = []string{parentIDs[0]}
		}
		for _, parentID := range file.Parents {
			parentTask, ok := taskByParentID[parentID]
			if !ok {
				continue
			}
			obj := fileToObj(file)
			entriesByParent[parentTask.path] = append(entriesByParent[parentTask.path], obj)
			if !obj.IsDir() {
				continue
			}
			nextDepth := parentTask.depth + 1
			if maxDepth >= 0 && nextDepth >= maxDepth {
				continue
			}
			if _, seen := visitedDirIDs[obj.GetID()]; seen {
				continue
			}
			visitedDirIDs[obj.GetID()] = struct{}{}
			nextTasks = append(nextTasks, googleDriveListRTask{
				id:    obj.GetID(),
				path:  stdpath.Join(parentTask.path, obj.GetName()),
				depth: nextDepth,
			})
		}
	}
	return entriesByParent, nextTasks
}

// getTargetFileInfo gets target file details for shortcuts
func (d *GoogleDrive) getTargetFileInfo(targetId string) (File, error) {
	var targetFile File
	url := fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s", targetId)
	query := map[string]string{
		"fields": FileInfoFields,
	}
	_, err := d.request(url, http.MethodGet, func(req *resty.Request) {
		req.SetQueryParams(query)
	}, &targetFile)
	if err != nil {
		return File{}, err
	}
	return targetFile, nil
}

// batchGetTargetFilesInfo batch gets target file info, sequential processing to avoid concurrency complexity
func (d *GoogleDrive) batchGetTargetFilesInfo(targetIds []string) map[string]File {
	if len(targetIds) == 0 {
		return make(map[string]File)
	}

	result := make(map[string]File)
	// Sequential processing to avoid concurrency complexity
	for _, targetId := range targetIds {
		file, err := d.getTargetFileInfo(targetId)
		if err == nil {
			result[targetId] = file
		}
	}
	return result
}

func (d *GoogleDrive) chunkUpload(ctx context.Context, file model.FileStreamer, url string, up driver.UpdateProgress) error {
	defaultChunkSize := d.ChunkSize * 1024 * 1024
	ss, err := stream.NewStreamSectionReader(file, int(defaultChunkSize), &up)
	if err != nil {
		return err
	}

	var offset int64 = 0
	url += "?includeItemsFromAllDrives=true&supportsAllDrives=true"
	for offset < file.GetSize() {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		chunkSize := min(file.GetSize()-offset, defaultChunkSize)
		reader, err := ss.GetSectionReader(offset, chunkSize)
		if err != nil {
			return err
		}
		limitedReader := driver.NewLimitedUploadStream(ctx, reader)
		err = retry.Do(func() error {
			reader.Seek(0, io.SeekStart)
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, limitedReader)
			if err != nil {
				return err
			}
			req.Header = map[string][]string{
				"Authorization":  {"Bearer " + d.AccessToken},
				"Content-Length": {strconv.FormatInt(chunkSize, 10)},
				"Content-Range":  {fmt.Sprintf("bytes %d-%d/%d", offset, offset+chunkSize-1, file.GetSize())},
			}
			res, err := base.HttpClient.Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			bytes, _ := io.ReadAll(res.Body)
			var e Error
			utils.Json.Unmarshal(bytes, &e)
			if e.Error.Code != 0 {
				if e.Error.Code == 401 {
					err = d.refreshToken()
					if err != nil {
						return err
					}
				}
				return fmt.Errorf("%s: %v", e.Error.Message, e.Error.Errors)
			}
			up(float64(offset+chunkSize) / float64(file.GetSize()) * 100)
			return nil
		},
			retry.Context(ctx),
			retry.Attempts(3),
			retry.DelayType(retry.BackOffDelay),
			retry.Delay(time.Second))
		ss.FreeSectionReader(reader)
		if err != nil {
			return err
		}
		offset += chunkSize
	}
	return nil
}

func (d *GoogleDrive) getAbout(ctx context.Context) (*AboutResp, error) {
	query := map[string]string{
		"fields": "storageQuota",
	}
	var resp AboutResp
	_, err := d.request("https://www.googleapis.com/drive/v3/about", http.MethodGet, func(req *resty.Request) {
		req.SetQueryParams(query)
		req.SetContext(ctx)
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
