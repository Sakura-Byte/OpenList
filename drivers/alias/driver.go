package alias

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	stdpath "path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	log "github.com/sirupsen/logrus"
)

type Alias struct {
	model.Storage
	Addition
	rootOrder   []string
	pathMap     map[string][]string
	autoFlatten bool
	oneKey      string
}

func (d *Alias) Config() driver.Config {
	return config
}

func (d *Alias) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Alias) Init(ctx context.Context) error {
	if d.Paths == "" {
		return errors.New("paths is required")
	}
	paths := strings.Split(d.Paths, "\n")
	d.rootOrder = make([]string, 0, len(paths))
	d.pathMap = make(map[string][]string)
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		k, v := getPair(path)
		if _, ok := d.pathMap[k]; !ok {
			d.rootOrder = append(d.rootOrder, k)
		}
		d.pathMap[k] = append(d.pathMap[k], v)
	}
	if len(d.pathMap) == 1 {
		for k := range d.pathMap {
			d.oneKey = k
		}
		d.autoFlatten = true
	} else {
		d.oneKey = ""
		d.autoFlatten = false
	}
	return nil
}

func (d *Alias) Drop(ctx context.Context) error {
	d.rootOrder = nil
	d.pathMap = nil
	return nil
}

func (d *Alias) Get(ctx context.Context, path string) (model.Obj, error) {
	if utils.PathEqual(path, "/") {
		return &model.Object{
			Name:     "Root",
			IsFolder: true,
			Path:     "/",
		}, nil
	}
	root, sub := d.getRootAndPath(path)
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	var ret *model.Object
	provider := ""
	var lastErr error
	for _, dst := range dsts {
		rawPath := stdpath.Join(dst, sub)
		obj, err := fs.Get(ctx, rawPath, &fs.GetArgs{NoLog: true})
		if err != nil {
			if !errs.IsObjectNotFound(err) {
				lastErr = err
			}
			continue
		}
		storage, err := fs.GetStorage(rawPath, &fs.GetStoragesArgs{})
		if ret == nil {
			ret = &model.Object{
				Path:     path,
				Name:     obj.GetName(),
				Size:     obj.GetSize(),
				Modified: obj.ModTime(),
				IsFolder: obj.IsDir(),
				HashInfo: obj.GetHash(),
			}
			if !d.ProviderPassThrough || err != nil {
				break
			}
			provider = storage.Config().Name
		} else if err != nil || provider != storage.GetStorage().Driver {
			provider = ""
			break
		}
	}
	if ret == nil {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, errs.ObjectNotFound
	}
	if provider != "" {
		return &model.ObjectProvider{
			Object: *ret,
			Provider: model.Provider{
				Provider: provider,
			},
		}, nil
	}
	return ret, nil
}

func (d *Alias) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	path := dir.GetPath()
	if utils.PathEqual(path, "/") && !d.autoFlatten {
		return d.listRoot(ctx, args.WithStorageDetails && d.DetailsPassThrough, args.Refresh), nil
	}
	root, sub := d.getRootAndPath(path)
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	var objs []model.Obj
	var lastErr error
	anySuccess := false
	for _, dst := range dsts {
		tmp, err := fs.List(ctx, stdpath.Join(dst, sub), &fs.ListArgs{
			NoLog:              true,
			Refresh:            args.Refresh,
			WithStorageDetails: args.WithStorageDetails && d.DetailsPassThrough,
		})
		if err != nil {
			if !errs.IsObjectNotFound(err) {
				lastErr = err
			}
			continue
		}
		anySuccess = true
		tmp, err = utils.SliceConvert(tmp, func(obj model.Obj) (model.Obj, error) {
			objRes := model.Object{
				Name:     obj.GetName(),
				Size:     obj.GetSize(),
				Modified: obj.ModTime(),
				IsFolder: obj.IsDir(),
			}
			if thumb, ok := model.GetThumb(obj); ok {
				return &model.ObjThumb{
					Object: objRes,
					Thumbnail: model.Thumbnail{
						Thumbnail: thumb,
					},
				}, nil
			}
			if details, ok := model.GetStorageDetails(obj); ok {
				return &model.ObjStorageDetails{
					Obj:                    &objRes,
					StorageDetailsWithName: *details,
				}, nil
			}
			return &objRes, nil
		})
		if err == nil {
			objs = append(objs, tmp...)
		}
	}
	if !anySuccess && lastErr != nil {
		return nil, lastErr
	}
	if lastErr != nil {
		log.Warnf("alias List partial failure (some backends returned errors): %v", lastErr)
	}
	return objs, nil
}

func (d *Alias) ListR(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	path := dir.GetPath()
	if utils.PathEqual(path, "/") && !d.autoFlatten {
		// Non-autoFlatten root: emit root virtual folder entries first, then recurse
		rootEntries := make([]model.Obj, 0, len(d.rootOrder))
		for _, k := range d.rootOrder {
			rootEntries = append(rootEntries, &model.Object{
				Name:     k,
				IsFolder: true,
				Modified: d.Modified,
			})
		}
		if len(rootEntries) > 0 {
			if err := callback("/", rootEntries); err != nil {
				return err
			}
		}
		if maxDepth == 1 {
			return nil
		}
		nextDepth := maxDepth
		if maxDepth > 0 {
			nextDepth = maxDepth - 1
		}
		for _, k := range d.rootOrder {
			dsts := d.pathMap[k]
			if err := d.listRForDsts(ctx, dsts, "/"+k, "", args, nextDepth, callback); err != nil {
				if errors.Is(err, context.Canceled) || utils.IsCanceled(ctx) {
					return err
				}
				log.Warnf("alias ListR for root key %s failed: %v", k, err)
				// Continue with other keys — don't let one failing key block the rest
			}
		}
		return nil
	}
	root, sub := d.getRootAndPath(path)
	dsts, ok := d.pathMap[root]
	if !ok {
		return errs.ObjectNotFound
	}
	var aliasPrefix string
	if d.autoFlatten {
		aliasPrefix = ""
	} else {
		aliasPrefix = "/" + root
	}
	return d.listRForDsts(ctx, dsts, aliasPrefix, sub, args, maxDepth, callback)
}

func (d *Alias) listRForDsts(ctx context.Context, dsts []string, aliasPrefix, sub string,
	args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	var lastErr error
	// Track whether ANY callback was invoked (even from a dst that later failed).
	// This is critical: if we already sent entries via callback, returning an error
	// would cause listr_walk.go to fallback and duplicate those entries.
	anyCallbackSent := false
	wrappedCallback := func(parent string, objs []model.Obj) error {
		anyCallbackSent = true
		return callback(parent, objs)
	}
	for _, dst := range dsts {
		rawPath := stdpath.Join(dst, sub)
		storage, actualPath, err := op.GetStorageAndActualPath(rawPath)
		if err != nil {
			log.Warnf("alias ListR: failed to resolve storage for %s: %v", rawPath, err)
			lastErr = err
			continue
		}
		mountPath := storage.GetStorage().MountPath
		err = op.WalkStorageRecursive(ctx, storage, actualPath, maxDepth, args,
			true, nil, func(parent string, objs []model.Obj) error {
				// Transform parent from underlying storage space to alias path space
				fullParent := utils.GetFullPath(mountPath, parent)
				subParent := strings.TrimPrefix(fullParent, dst)
				if subParent == "" {
					subParent = "/"
				}
				aliasParent := stdpath.Join(aliasPrefix, subParent)
				if aliasParent == "" {
					aliasParent = "/"
				}
				// Convert objs to plain model.Object to avoid double-wrapping
				convertedObjs := make([]model.Obj, len(objs))
				for i, obj := range objs {
					convertedObjs[i] = &model.Object{
						Name:     obj.GetName(),
						Size:     obj.GetSize(),
						Modified: obj.ModTime(),
						IsFolder: obj.IsDir(),
					}
				}
				return wrappedCallback(aliasParent, convertedObjs)
			})
		if err != nil {
			if errors.Is(err, context.Canceled) || utils.IsCanceled(ctx) {
				return err
			}
			log.Warnf("alias ListR for %s failed: %v", rawPath, err)
			lastErr = err
		}
	}
	if lastErr != nil {
		if anyCallbackSent {
			// Some data was already sent via callback. Returning error would cause
			// listr_walk.go to fallback to recursive List, duplicating entries.
			// Accept partial results and log the failure.
			log.Warnf("alias ListR partial failure (some data already sent, skipping fallback): %v", lastErr)
			return nil
		}
		// No data was sent — safe to return error and trigger fallback
		return lastErr
	}
	return nil
}

func (d *Alias) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	root, sub := d.getRootAndPath(file.GetPath())
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	// proxy || ftp,s3
	if common.GetApiUrl(ctx) == "" {
		args.Redirect = false
	}
	for _, dst := range dsts {
		reqPath := stdpath.Join(dst, sub)
		link, fi, err := d.link(ctx, reqPath, args)
		if err != nil {
			continue
		}
		if link == nil {
			// 重定向且需要通过代理
			return &model.Link{
				URL: fmt.Sprintf("%s/p%s?sign=%s",
					common.GetApiUrl(ctx),
					utils.EncodePath(reqPath, true),
					sign.Sign(reqPath)),
			}, nil
		}

		resultLink := *link
		resultLink.SyncClosers = utils.NewSyncClosers(link)
		if args.Redirect {
			return &resultLink, nil
		}

		if resultLink.ContentLength == 0 {
			resultLink.ContentLength = fi.GetSize()
		}
		if d.DownloadConcurrency > 0 {
			resultLink.Concurrency = d.DownloadConcurrency
		}
		if d.DownloadPartSize > 0 {
			resultLink.PartSize = d.DownloadPartSize * utils.KB
		}
		return &resultLink, nil
	}
	return nil, errs.ObjectNotFound
}

func (d *Alias) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
	root, sub := d.getRootAndPath(args.Obj.GetPath())
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	for _, dst := range dsts {
		rawPath := stdpath.Join(dst, sub)
		storage, actualPath, err := op.GetStorageAndActualPath(rawPath)
		if err != nil {
			continue
		}
		return op.Other(ctx, storage, model.FsOtherArgs{
			Path:   actualPath,
			Method: args.Method,
			Data:   args.Data,
		})
	}
	return nil, errs.NotImplement
}

func (d *Alias) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	reqPath, err := d.getReqPath(ctx, parentDir, true)
	if err == nil {
		for _, path := range reqPath {
			err = errors.Join(err, fs.MakeDir(ctx, stdpath.Join(*path, dirName)))
		}
		return err
	}
	if errs.IsNotImplementError(err) {
		return errors.New("same-name dirs cannot make sub-dir")
	}
	return err
}

func (d *Alias) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	srcPath, err := d.getReqPath(ctx, srcObj, false)
	if errs.IsNotImplementError(err) {
		return errors.New("same-name files cannot be moved")
	}
	if err != nil {
		return err
	}
	dstPath, err := d.getReqPath(ctx, dstDir, true)
	if errs.IsNotImplementError(err) {
		return errors.New("same-name dirs cannot be moved to")
	}
	if err != nil {
		return err
	}
	if len(srcPath) == len(dstPath) {
		for i := range srcPath {
			_, e := fs.Move(ctx, *srcPath[i], *dstPath[i])
			err = errors.Join(err, e)
		}
		return err
	} else {
		return errors.New("parallel paths mismatch")
	}
}

func (d *Alias) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	reqPath, err := d.getReqPath(ctx, srcObj, false)
	if err == nil {
		for _, path := range reqPath {
			err = errors.Join(err, fs.Rename(ctx, *path, newName))
		}
		return err
	}
	if errs.IsNotImplementError(err) {
		return errors.New("same-name files cannot be Rename")
	}
	return err
}

func (d *Alias) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	srcPath, err := d.getReqPath(ctx, srcObj, false)
	if errs.IsNotImplementError(err) {
		return errors.New("same-name files cannot be copied")
	}
	if err != nil {
		return err
	}
	dstPath, err := d.getReqPath(ctx, dstDir, true)
	if errs.IsNotImplementError(err) {
		return errors.New("same-name dirs cannot be copied to")
	}
	if err != nil {
		return err
	}
	if len(srcPath) == len(dstPath) {
		for i := range srcPath {
			_, e := fs.Copy(ctx, *srcPath[i], *dstPath[i])
			err = errors.Join(err, e)
		}
		return err
	} else if len(srcPath) == 1 || !d.ProtectSameName {
		for _, path := range dstPath {
			_, e := fs.Copy(ctx, *srcPath[0], *path)
			err = errors.Join(err, e)
		}
		return err
	} else {
		return errors.New("parallel paths mismatch")
	}
}

func (d *Alias) Remove(ctx context.Context, obj model.Obj) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	reqPath, err := d.getReqPath(ctx, obj, false)
	if err == nil {
		for _, path := range reqPath {
			err = errors.Join(err, fs.Remove(ctx, *path))
		}
		return err
	}
	if errs.IsNotImplementError(err) {
		return errors.New("same-name files cannot be Delete")
	}
	return err
}

func (d *Alias) Put(ctx context.Context, dstDir model.Obj, s model.FileStreamer, up driver.UpdateProgress) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	reqPath, err := d.getReqPath(ctx, dstDir, true)
	if err == nil {
		if len(reqPath) == 1 {
			storage, reqActualPath, err := op.GetStorageAndActualPath(*reqPath[0])
			if err != nil {
				return err
			}
			return op.Put(ctx, storage, reqActualPath, &stream.FileStream{
				Obj:      s,
				Mimetype: s.GetMimetype(),
				Reader:   s,
			}, up)
		} else {
			file, err := s.CacheFullAndWriter(nil, nil)
			if err != nil {
				return err
			}
			count := float64(len(reqPath) + 1)
			up(100 / count)
			for i, path := range reqPath {
				err = errors.Join(err, fs.PutDirectly(ctx, *path, &stream.FileStream{
					Obj:      s,
					Mimetype: s.GetMimetype(),
					Reader:   file,
				}))
				up(float64(i+2) / float64(count) * 100)
				_, e := file.Seek(0, io.SeekStart)
				if e != nil {
					return errors.Join(err, e)
				}
			}
			return err
		}
	}
	if errs.IsNotImplementError(err) {
		return errors.New("same-name dirs cannot be Put")
	}
	return err
}

func (d *Alias) PutURL(ctx context.Context, dstDir model.Obj, name, url string) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	reqPath, err := d.getReqPath(ctx, dstDir, true)
	if err == nil {
		for _, path := range reqPath {
			err = errors.Join(err, fs.PutURL(ctx, *path, name, url))
		}
		return err
	}
	if errs.IsNotImplementError(err) {
		return errors.New("same-name files cannot offline download")
	}
	return err
}

func (d *Alias) GetArchiveMeta(ctx context.Context, obj model.Obj, args model.ArchiveArgs) (model.ArchiveMeta, error) {
	root, sub := d.getRootAndPath(obj.GetPath())
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	for _, dst := range dsts {
		meta, err := d.getArchiveMeta(ctx, dst, sub, args)
		if err == nil {
			return meta, nil
		}
	}
	return nil, errs.NotImplement
}

func (d *Alias) ListArchive(ctx context.Context, obj model.Obj, args model.ArchiveInnerArgs) ([]model.Obj, error) {
	root, sub := d.getRootAndPath(obj.GetPath())
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	for _, dst := range dsts {
		l, err := d.listArchive(ctx, dst, sub, args)
		if err == nil {
			return l, nil
		}
	}
	return nil, errs.NotImplement
}

func (d *Alias) Extract(ctx context.Context, obj model.Obj, args model.ArchiveInnerArgs) (*model.Link, error) {
	// alias的两个驱动，一个支持驱动提取，一个不支持，如何兼容？
	// 如果访问的是不支持驱动提取的驱动内的压缩文件，GetArchiveMeta就会返回errs.NotImplement，提取URL前缀就会是/ae，Extract就不会被调用
	// 如果访问的是支持驱动提取的驱动内的压缩文件，GetArchiveMeta就会返回有效值，提取URL前缀就会是/ad，Extract就会被调用
	root, sub := d.getRootAndPath(obj.GetPath())
	dsts, ok := d.pathMap[root]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	for _, dst := range dsts {
		reqPath := stdpath.Join(dst, sub)
		link, err := d.extract(ctx, reqPath, args)
		if err != nil {
			continue
		}
		if link == nil {
			return &model.Link{
				URL: fmt.Sprintf("%s/ap%s?inner=%s&pass=%s&sign=%s",
					common.GetApiUrl(ctx),
					utils.EncodePath(reqPath, true),
					utils.EncodePath(args.InnerPath, true),
					url.QueryEscape(args.Password),
					sign.SignArchive(reqPath)),
			}, nil
		}
		resultLink := *link
		resultLink.SyncClosers = utils.NewSyncClosers(link)
		return &resultLink, nil
	}
	return nil, errs.NotImplement
}

func (d *Alias) ArchiveDecompress(ctx context.Context, srcObj, dstDir model.Obj, args model.ArchiveDecompressArgs) error {
	if !d.Writable {
		return errs.PermissionDenied
	}
	srcPath, err := d.getReqPath(ctx, srcObj, false)
	if errs.IsNotImplementError(err) {
		return errors.New("same-name files cannot be decompressed")
	}
	if err != nil {
		return err
	}
	dstPath, err := d.getReqPath(ctx, dstDir, true)
	if errs.IsNotImplementError(err) {
		return errors.New("same-name dirs cannot be decompressed to")
	}
	if err != nil {
		return err
	}
	if len(srcPath) == len(dstPath) {
		for i := range srcPath {
			_, e := fs.ArchiveDecompress(ctx, *srcPath[i], *dstPath[i], args)
			err = errors.Join(err, e)
		}
		return err
	} else if len(srcPath) == 1 || !d.ProtectSameName {
		for _, path := range dstPath {
			_, e := fs.ArchiveDecompress(ctx, *srcPath[0], *path, args)
			err = errors.Join(err, e)
		}
		return err
	} else {
		return errors.New("parallel paths mismatch")
	}
}

func (d *Alias) ResolveLinkCacheMode(path string) driver.LinkCacheMode {
	root, sub := d.getRootAndPath(path)
	dsts, ok := d.pathMap[root]
	if !ok {
		return 0
	}
	for _, dst := range dsts {
		storage, actualPath, err := op.GetStorageAndActualPath(stdpath.Join(dst, sub))
		if err != nil {
			continue
		}
		if storage.Config().CheckStatus && storage.GetStorage().Status != op.WORK {
			continue
		}
		mode := storage.Config().LinkCacheMode
		if mode == -1 {
			return storage.(driver.LinkCacheModeResolver).ResolveLinkCacheMode(actualPath)
		} else {
			return mode
		}
	}
	return 0
}

var _ driver.Driver = (*Alias)(nil)
var _ driver.ListRer = (*Alias)(nil)
