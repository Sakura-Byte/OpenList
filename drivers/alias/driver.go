package alias

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	log "github.com/sirupsen/logrus"
)

type Alias struct {
	model.Storage
	Addition
	rootOrder []string
	pathMap   map[string][]string
	root      model.Obj
}

func (d *Alias) Config() driver.Config {
	return config
}

func (d *Alias) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Alias) Init(ctx context.Context) error {
	paths := strings.Split(d.Paths, "\n")
	d.rootOrder = make([]string, 0, len(paths))
	d.pathMap = make(map[string][]string)
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		k, v := getPair(path)
		temp, ok := d.pathMap[k]
		if !ok {
			d.rootOrder = append(d.rootOrder, k)
		}
		d.pathMap[k] = append(temp, v)
	}

	switch len(d.rootOrder) {
	case 0:
		return errors.New("paths is required")
	case 1:
		paths := d.pathMap[d.rootOrder[0]]
		roots := make(BalancedObjs, 0, len(paths))
		roots = append(roots, &model.Object{
			Name:     "root",
			Path:     paths[0],
			IsFolder: true,
			Modified: d.Modified,
			Mask:     model.Locked,
		})
		for _, path := range paths[1:] {
			roots = append(roots, &model.Object{
				Path: path,
			})
		}
		d.root = roots
	default:
		d.root = &model.Object{
			Name:     "root",
			Path:     "/",
			IsFolder: true,
			Modified: d.Modified,
			Mask:     model.ReadOnly,
		}
	}

	if !utils.SliceContains(ValidReadConflictPolicy, d.ReadConflictPolicy) {
		d.ReadConflictPolicy = FirstRWP
	}
	if !utils.SliceContains(ValidWriteConflictPolicy, d.WriteConflictPolicy) {
		d.WriteConflictPolicy = DisabledWP
	}
	if !utils.SliceContains(ValidPutConflictPolicy, d.PutConflictPolicy) {
		d.PutConflictPolicy = DisabledWP
	}
	return nil
}

func (d *Alias) Drop(ctx context.Context) error {
	d.rootOrder = nil
	d.pathMap = nil
	d.root = nil
	return nil
}

func (d *Alias) GetRoot(ctx context.Context) (model.Obj, error) {
	if d.root == nil {
		return nil, errs.StorageNotInit
	}
	return d.root, nil
}

// 通过op.Get调用的话，path一定是子路径(/开头)
func (d *Alias) Get(ctx context.Context, path string) (model.Obj, error) {
	roots, sub := d.getRootsAndPath(path)
	if len(roots) == 0 {
		return nil, errs.ObjectNotFound
	}
	for idx, root := range roots {
		rawPath := stdpath.Join(root, sub)
		obj, err := fs.Get(ctx, rawPath, &fs.GetArgs{NoLog: true})
		if err != nil {
			continue
		}
		mask := model.GetObjMask(obj) &^ model.Temp
		if sub == "" {
			// 根目录
			mask |= model.Locked | model.Virtual
		}
		obj = wrapAliasViewObj(obj, rawPath, d.rawPathToAliasPath(rawPath), mask)
		if d.ProviderPassThrough && !obj.IsDir() {
			if storage, err := fs.GetStorage(rawPath, &fs.GetStoragesArgs{}); err == nil {
				obj = &providerOverlay{Obj: obj, provider: storage.Config().Name}
			}
		}

		remainingRoots := roots[idx+1:]
		remainingObjs := make([]model.Obj, 0, len(remainingRoots))
		needThumb := !obj.IsDir() && !hasThumb(obj)
		for _, nextRoot := range remainingRoots {
			nextRawPath := stdpath.Join(nextRoot, sub)
			if needThumb {
				nextObj, err := fs.Get(ctx, nextRawPath, &fs.GetArgs{NoLog: true})
				if err == nil {
					nextMask := model.GetObjMask(nextObj) &^ model.Temp
					if sub == "" {
						nextMask |= model.Locked | model.Virtual
					}
					nextObj = wrapAliasViewObj(nextObj, nextRawPath, d.rawPathToAliasPath(nextRawPath), nextMask)
					if d.ProviderPassThrough && !nextObj.IsDir() {
						if storage, err := fs.GetStorage(nextRawPath, &fs.GetStoragesArgs{}); err == nil {
							nextObj = &providerOverlay{Obj: nextObj, provider: storage.Config().Name}
						}
					}
					remainingObjs = append(remainingObjs, nextObj)
					obj = borrowThumb(obj, nextObj)
					needThumb = !hasThumb(obj)
					continue
				}
			}
			remainingObjs = append(remainingObjs, &tempObj{model.Object{Path: nextRawPath}})
		}

		var objs BalancedObjs
		objs = make(BalancedObjs, 0, len(remainingObjs)+2)
		objs = append(objs, obj)
		if idx > 0 {
			objs = append(objs, nil)
		}
		objs = append(objs, remainingObjs...)
		return objs, nil
	}
	return nil, errs.ObjectNotFound
}

func (d *Alias) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	dirs, ok := dir.(BalancedObjs)
	if !ok {
		return d.listRoot(ctx, args.WithStorageDetails && d.DetailsPassThrough, args.Refresh), nil
	}

	// 因为alias是NoCache且Get方法不会返回NotSupport或NotImplement错误
	// 所以这里对象不会传回到alias，也就不需要返回BalancedObjs了
	objMap := make(map[string]model.Obj)
	for _, dir := range dirs {
		if dir == nil {
			continue
		}
		dirPath := getRawPath(dir)
		tmp, err := fs.List(ctx, dirPath, &fs.ListArgs{
			NoLog:              true,
			Refresh:            args.Refresh,
			WithStorageDetails: args.WithStorageDetails && d.DetailsPassThrough,
		})
		if err != nil {
			continue
		}
		for _, obj := range tmp {
			name := obj.GetName()
			mask := model.GetObjMask(obj) &^ model.Temp
			rawPath := stdpath.Join(dirPath, name)
			objRet := wrapAliasViewObj(obj, rawPath, d.rawPathToAliasPath(rawPath), mask)
			if existing, exists := objMap[name]; exists {
				objMap[name] = borrowThumb(existing, objRet)
				continue
			}
			objMap[name] = objRet
		}
	}
	objs := make([]model.Obj, 0, len(objMap))
	for _, obj := range objMap {
		objs = append(objs, obj)
	}
	if d.OrderBy == "" {
		sort := getAllSort(dirs)
		if sort.OrderBy != "" {
			model.SortFiles(objs, sort.OrderBy, sort.OrderDirection)
		}
		if d.ExtractFolder == "" && sort.ExtractFolder != "" {
			model.ExtractFolder(objs, sort.ExtractFolder)
		}
	}
	return objs, nil
}

func (d *Alias) rawPathToAliasPath(rawPath string) string {
	rawPath = utils.FixAndCleanPath(rawPath)
	if rawPath == "" {
		return "/"
	}
	singleRoot := len(d.rootOrder) == 1
	for _, key := range d.rootOrder {
		aliasPrefix := "/" + key
		if singleRoot {
			aliasPrefix = ""
		}
		for _, dst := range d.pathMap[key] {
			dst = utils.FixAndCleanPath(dst)
			if !utils.PathEqual(rawPath, dst) && !strings.HasPrefix(rawPath, utils.PathAddSeparatorSuffix(dst)) {
				continue
			}
			rel := strings.TrimPrefix(rawPath, dst)
			if rel == "" {
				if aliasPrefix == "" {
					return "/"
				}
				return aliasPrefix
			}
			mapped := stdpath.Join(aliasPrefix, rel)
			if mapped == "" {
				return "/"
			}
			return mapped
		}
	}
	return rawPath
}

func (d *Alias) ResolvePublicPath(rawPath string) (string, bool) {
	rawPath = utils.FixAndCleanPath(rawPath)
	relPath := d.rawPathToAliasPath(rawPath)
	if relPath == rawPath {
		return "", false
	}
	mountPath := utils.GetActualMountPath(d.GetStorage().MountPath)
	if relPath == "/" {
		return mountPath, true
	}
	return utils.FixAndCleanPath(stdpath.Join(mountPath, relPath)), true
}

func (d *Alias) ListR(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	if maxDepth == 0 || callback == nil {
		return nil
	}
	path := d.rawPathToAliasPath(dir.GetPath())
	if path == "" {
		path = "/"
	}
	if utils.PathEqual(path, "/") && len(d.rootOrder) > 1 {
		// Multi-root: emit virtual root keys first, then recurse each key.
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
			}
		}
		return nil
	}
	roots, sub := d.getRootsAndPath(path)
	if len(roots) == 0 {
		return errs.ObjectNotFound
	}
	aliasPrefix := ""
	if len(d.rootOrder) > 1 {
		trimmed := strings.TrimPrefix(path, "/")
		root, _, _ := strings.Cut(trimmed, "/")
		if root == "" {
			return errs.ObjectNotFound
		}
		aliasPrefix = "/" + root
	}
	return d.listRForDsts(ctx, roots, aliasPrefix, sub, args, maxDepth, callback)
}

func (d *Alias) ListRForUpdateSite(ctx context.Context, dir model.Obj, args model.ListArgs, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	if maxDepth == 0 || callback == nil {
		return nil
	}
	path := d.rawPathToAliasPath(dir.GetPath())
	if path == "" {
		path = "/"
	}
	if utils.PathEqual(path, "/") && len(d.rootOrder) > 1 {
		rootEntries := make([]model.Obj, 0, len(d.rootOrder))
		for _, k := range d.rootOrder {
			rootEntries = append(rootEntries, &model.Object{
				Name:     k,
				IsFolder: true,
				Modified: d.Modified,
			})
		}
		if err := callback(driver.UpdateSiteChunk{
			Parent:     "/",
			Entries:    rootEntries,
			ParentDone: true,
		}); err != nil {
			return err
		}
		if maxDepth == 1 {
			return nil
		}
		nextDepth := maxDepth
		if maxDepth > 0 {
			nextDepth = maxDepth - 1
		}
		for _, k := range d.rootOrder {
			if err := d.listRForUpdateSiteDsts(ctx, d.pathMap[k], "/"+k, "", args, nextDepth, callback); err != nil {
				if errors.Is(err, context.Canceled) || utils.IsCanceled(ctx) {
					return err
				}
				log.Warnf("alias update-site ListR for root key %s failed: %v", k, err)
			}
		}
		return nil
	}
	roots, sub := d.getRootsAndPath(path)
	if len(roots) == 0 {
		return errs.ObjectNotFound
	}
	aliasPrefix := ""
	if len(d.rootOrder) > 1 {
		trimmed := strings.TrimPrefix(path, "/")
		root, _, _ := strings.Cut(trimmed, "/")
		if root == "" {
			return errs.ObjectNotFound
		}
		aliasPrefix = "/" + root
	}
	return d.listRForUpdateSiteDsts(ctx, roots, aliasPrefix, sub, args, maxDepth, callback)
}

func (d *Alias) listRForUpdateSiteDsts(ctx context.Context, dsts []string, aliasPrefix, sub string,
	args model.ListArgs, maxDepth int, callback driver.UpdateSiteChunkCallback) error {
	var lastErr error
	anyChunkSent := false
	for _, dst := range dsts {
		rawPath := stdpath.Join(dst, sub)
		storage, actualPath, err := op.GetStorageAndActualPath(rawPath)
		if err != nil {
			log.Warnf("alias update-site ListR: failed to resolve storage for %s: %v", rawPath, err)
			lastErr = err
			continue
		}
		err = op.WalkUpdateSiteStorageChunks(ctx, storage, actualPath, maxDepth, args, func(chunk driver.UpdateSiteChunk) error {
			anyChunkSent = true
			fullParent := utils.GetFullPath(utils.GetActualMountPath(storage.GetStorage().MountPath), chunk.Parent)
			subParent := strings.TrimPrefix(fullParent, dst)
			if subParent == "" {
				subParent = "/"
			}
			aliasParent := stdpath.Join(aliasPrefix, subParent)
			if aliasParent == "" {
				aliasParent = "/"
			}
			converted := make([]model.Obj, len(chunk.Entries))
			for i, obj := range chunk.Entries {
				converted[i] = &model.Object{
					Name:     obj.GetName(),
					Size:     obj.GetSize(),
					Modified: obj.ModTime(),
					IsFolder: obj.IsDir(),
				}
			}
			return callback(driver.UpdateSiteChunk{
				Parent:     aliasParent,
				Entries:    converted,
				ParentDone: chunk.ParentDone,
			})
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || utils.IsCanceled(ctx) {
				return err
			}
			log.Warnf("alias update-site ListR for %s failed: %v", rawPath, err)
			lastErr = err
		}
	}
	if lastErr != nil && !anyChunkSent {
		return lastErr
	}
	if lastErr != nil {
		log.Warnf("alias update-site ListR partial failure (some data already sent, skipping fallback): %v", lastErr)
	}
	return nil
}

func (d *Alias) listRForDsts(ctx context.Context, dsts []string, aliasPrefix, sub string,
	args model.ListArgs, maxDepth int, callback driver.ListRCallback) error {
	var lastErr error
	// Track whether ANY callback was invoked (even from a dst that later failed).
	// If data was already emitted, returning error would trigger fallback and duplicate data.
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
				fullParent := utils.GetFullPath(mountPath, parent)
				subParent := strings.TrimPrefix(fullParent, dst)
				if subParent == "" {
					subParent = "/"
				}
				aliasParent := stdpath.Join(aliasPrefix, subParent)
				if aliasParent == "" {
					aliasParent = "/"
				}
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
			log.Warnf("alias ListR partial failure (some data already sent, skipping fallback): %v", lastErr)
			return nil
		}
		return lastErr
	}
	return nil
}

func (d *Alias) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if d.ReadConflictPolicy == AllRWP && !args.Redirect {
		files, err := d.getAllObjs(ctx, file, getWriteAndPutFilterFunc(AllRWP))
		if err != nil {
			return nil, err
		}
		linkClosers := make([]io.Closer, 0, len(files))
		rrf := make([]model.RangeReaderIF, 0, len(files))
		for _, f := range files {
			link, fi, err := d.link(ctx, getRawPath(f), args)
			if err != nil {
				continue
			}
			if fi.GetSize() != files.GetSize() {
				_ = link.Close()
				continue
			}
			l := *link // 复制一份，避免修改到原始link
			if l.ContentLength == 0 {
				l.ContentLength = fi.GetSize()
			}
			if d.DownloadConcurrency > 0 {
				l.Concurrency = d.DownloadConcurrency
			}
			if d.DownloadPartSize > 0 {
				l.PartSize = d.DownloadPartSize * utils.KB
			}
			rr, err := stream.GetRangeReaderFromLink(l.ContentLength, &l)
			if err != nil {
				_ = link.Close()
				continue
			}
			linkClosers = append(linkClosers, link)
			rrf = append(rrf, rr)
		}
		rr := func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
			return rrf[rand.Intn(len(rrf))].RangeRead(ctx, httpRange)
		}
		return &model.Link{
			RangeReader: stream.RangeReaderFunc(rr),
			SyncClosers: utils.NewSyncClosers(linkClosers...),
		}, nil
	}

	var link *model.Link
	var fi model.Obj
	var err error
	files := file.(BalancedObjs)
	if d.ReadConflictPolicy == RandomBalancedRP || d.ReadConflictPolicy == AllRWP {
		rand.Shuffle(len(files), func(i, j int) {
			files[i], files[j] = files[j], files[i]
		})
	}
	for _, f := range files {
		if f == nil {
			continue
		}
		rawPath := getRawPath(f)
		link, fi, err = d.link(ctx, rawPath, args)
		if err == nil {
			if link == nil {
				// 重定向且需要通过代理
				return &model.Link{
					URL: fmt.Sprintf("%s/p%s?sign=%s",
						common.GetApiUrl(ctx),
						utils.EncodePath(rawPath, true),
						sign.Sign(rawPath)),
				}, nil
			}
			break
		}
	}
	if err != nil {
		return nil, err
	}
	resultLink := *link // 复制一份，避免修改到原始link
	resultLink.Expiration = nil
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

func (d *Alias) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
	// Other 不应负载均衡，这是因为前端是否调用 /fs/other 的判断条件是返回的 provider 的值
	// 而 ProviderPassThrough 开启时，返回的 provider 固定为第一个 obj 的后端驱动
	storage, actualPath, err := op.GetStorageAndActualPath(getRawPath(args.Obj))
	if err != nil {
		return nil, err
	}
	return op.Other(ctx, storage, model.FsOtherArgs{
		Path:   actualPath,
		Method: args.Method,
		Data:   args.Data,
	})
}

func (d *Alias) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	objs, err := d.getWriteObjs(ctx, parentDir)
	if err == nil {
		for _, obj := range objs {
			err = errors.Join(err, fs.MakeDir(ctx, stdpath.Join(getRawPath(obj), dirName)))
		}
	}
	return err
}

func (d *Alias) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	srcs, dsts, err := d.getMoveObjs(ctx, srcObj, dstDir)
	if err == nil {
		for i, dst := range dsts {
			src := srcs[i]
			_, e := fs.Move(ctx, getRawPath(src), getRawPath(dst))
			err = errors.Join(err, e)
		}
		srcs = srcs[len(dsts):]
		for _, src := range srcs {
			e := fs.Remove(ctx, getRawPath(src))
			err = errors.Join(err, e)
		}
	}
	return err
}

func (d *Alias) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	objs, err := d.getWriteObjs(ctx, srcObj)
	if err == nil {
		for _, obj := range objs {
			err = errors.Join(err, fs.Rename(ctx, getRawPath(obj), newName))
		}
	}
	return err
}

func (d *Alias) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	srcs, dsts, err := d.getCopyObjs(ctx, srcObj, dstDir)
	if err == nil {
		for i, src := range srcs {
			dst := dsts[i]
			_, e := fs.Copy(ctx, getRawPath(src), getRawPath(dst))
			err = errors.Join(err, e)
		}
	}
	return err
}

func (d *Alias) Remove(ctx context.Context, obj model.Obj) error {
	objs, err := d.getWriteObjs(ctx, obj)
	if err == nil {
		for _, obj := range objs {
			err = errors.Join(err, fs.Remove(ctx, getRawPath(obj)))
		}
	}
	return err
}

func (d *Alias) Put(ctx context.Context, dstDir model.Obj, s model.FileStreamer, up driver.UpdateProgress) error {
	objs, err := d.getPutObjs(ctx, dstDir)
	if err == nil {
		if len(objs) == 1 {
			storage, reqActualPath, err := op.GetStorageAndActualPath(getRawPath(objs))
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
			count := float64(len(objs) + 1)
			up(100 / count)
			for i, obj := range objs {
				err = errors.Join(err, fs.PutDirectly(ctx, getRawPath(obj), &stream.FileStream{
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
	return err
}

func (d *Alias) PutURL(ctx context.Context, dstDir model.Obj, name, url string) error {
	objs, err := d.getPutObjs(ctx, dstDir)
	if err == nil {
		for _, obj := range objs {
			err = errors.Join(err, fs.PutURL(ctx, getRawPath(obj), name, url))
		}
		return err
	}
	return err
}

func (d *Alias) GetArchiveMeta(ctx context.Context, obj model.Obj, args model.ArchiveArgs) (model.ArchiveMeta, error) {
	reqPath := d.getBalancedPath(ctx, obj)
	if reqPath == "" {
		return nil, errs.NotFile
	}
	meta, err := d.getArchiveMeta(ctx, reqPath, args)
	if err == nil {
		return meta, nil
	}
	return nil, errs.NotImplement
}

func (d *Alias) ListArchive(ctx context.Context, obj model.Obj, args model.ArchiveInnerArgs) ([]model.Obj, error) {
	reqPath := d.getBalancedPath(ctx, obj)
	if reqPath == "" {
		return nil, errs.NotFile
	}
	l, err := d.listArchive(ctx, reqPath, args)
	if err == nil {
		return l, nil
	}
	return nil, errs.NotImplement
}

func (d *Alias) Extract(ctx context.Context, obj model.Obj, args model.ArchiveInnerArgs) (*model.Link, error) {
	// alias的两个驱动，一个支持驱动提取，一个不支持，如何兼容？
	// 如果访问的是不支持驱动提取的驱动内的压缩文件，GetArchiveMeta就会返回errs.NotImplement，提取URL前缀就会是/ae，Extract就不会被调用
	// 如果访问的是支持驱动提取的驱动内的压缩文件，GetArchiveMeta就会返回有效值，提取URL前缀就会是/ad，Extract就会被调用
	reqPath := d.getBalancedPath(ctx, obj)
	if reqPath == "" {
		return nil, errs.NotFile
	}
	link, err := d.extract(ctx, reqPath, args)
	if err != nil {
		return nil, errs.NotImplement
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

func (d *Alias) ArchiveDecompress(ctx context.Context, srcObj, dstDir model.Obj, args model.ArchiveDecompressArgs) error {
	srcs, dsts, err := d.getCopyObjs(ctx, srcObj, dstDir)
	if err == nil {
		for i, src := range srcs {
			dst := dsts[i]
			_, e := fs.ArchiveDecompress(ctx, getRawPath(src), getRawPath(dst), args)
			err = errors.Join(err, e)
		}
	}
	return err
}

func (d *Alias) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	if !d.DetailsPassThrough {
		return nil, errs.NotImplement
	}
	if len(d.rootOrder) != 1 {
		return nil, errs.NotImplement
	}
	backends := d.pathMap[d.rootOrder[0]]
	var storage driver.Driver
	for _, backend := range backends {
		s, err := fs.GetStorage(backend, &fs.GetStoragesArgs{})
		if err != nil {
			return nil, errs.NotImplement
		}
		if storage == nil {
			storage = s
		} else if storage.GetStorage().MountPath != s.GetStorage().MountPath {
			return nil, errs.NotImplement
		}
	}
	if storage == nil { // should never access
		return nil, errs.NotImplement
	}
	return op.GetStorageDetails(ctx, storage)
}

func (d *Alias) ResolveLinkCacheMode(path string) driver.LinkCacheMode {
	roots, sub := d.getRootsAndPath(path)
	if len(roots) == 0 {
		return 0
	}
	for _, root := range roots {
		storage, actualPath, err := op.GetStorageAndActualPath(stdpath.Join(root, sub))
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
var _ driver.UpdateSiteListRer = (*Alias)(nil)
