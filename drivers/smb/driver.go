package smb

import (
	"context"
	"errors"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"

	"github.com/cloudsoda/go-smb2"
)

type SMB struct {
	lastConnTime  atomic.Int64
	connMu        sync.Mutex
	connection    *smbConnection
	newConnection func(context.Context) (*smbConnection, error)
	activeLeases  int
	dropped       bool
	model.Storage
	Addition
}

func (d *SMB) Config() driver.Config {
	return config
}

func (d *SMB) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *SMB) Init(ctx context.Context) error {
	if !strings.Contains(d.Addition.Address, ":") {
		d.Addition.Address = d.Addition.Address + ":445"
	}
	return d.initFS(ctx)
}

func (d *SMB) Drop(ctx context.Context) error {
	return d.dropFS()
}

func (d *SMB) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	fullPath := dir.GetPath()
	rawFiles, err := fs.ReadDir(fullPath)
	if err != nil {
		d.cleanLastConnTime()
		return nil, err
	}
	d.updateLastConnTime()
	files := make([]model.Obj, 0, len(rawFiles))
	for _, f := range rawFiles {
		file := model.Object{
			Path:     path.Join(fullPath, f.Name()),
			Name:     f.Name(),
			Modified: f.ModTime(),
			Size:     f.Size(),
			IsFolder: f.IsDir(),
			Ctime:    f.(*smb2.FileStat).CreationTime,
		}

		files = append(files, &file)
	}
	return files, nil
}

func (d *SMB) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return nil, err
	}
	fullPath := file.GetPath()
	remoteFile, err := fs.Open(fullPath)
	if err != nil {
		d.cleanLastConnTime()
		release()
		return nil, err
	}
	d.updateLastConnTime()
	mFile := &stream.RateLimitFile{
		File:    remoteFile,
		Limiter: stream.ServerDownloadLimit,
		Ctx:     ctx,
	}
	return &model.Link{
		RangeReader: stream.GetRangeReaderFromMFile(file.GetSize(), mFile),
		SyncClosers: utils.NewSyncClosers(remoteFile, utils.CloseFunc(func() error {
			release()
			return nil
		})),
		RequireReference: true,
	}, nil
}

func (d *SMB) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return err
	}
	defer release()
	fullPath := filepath.Join(parentDir.GetPath(), dirName)
	err = fs.MkdirAll(fullPath, 0700)
	if err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

func (d *SMB) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return err
	}
	defer release()
	srcPath := srcObj.GetPath()
	dstPath := filepath.Join(dstDir.GetPath(), srcObj.GetName())
	err = fs.Rename(srcPath, dstPath)
	if err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

func (d *SMB) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return err
	}
	defer release()
	srcPath := srcObj.GetPath()
	dstPath := filepath.Join(filepath.Dir(srcPath), newName)
	err = fs.Rename(srcPath, dstPath)
	if err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

func (d *SMB) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return err
	}
	defer release()
	srcPath := srcObj.GetPath()
	dstPath := filepath.Join(dstDir.GetPath(), srcObj.GetName())
	if srcObj.IsDir() {
		err = copyDir(fs, srcPath, dstPath)
	} else {
		err = copyFile(fs, srcPath, dstPath)
	}
	if err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

func (d *SMB) Remove(ctx context.Context, obj model.Obj) error {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return err
	}
	defer release()
	fullPath := obj.GetPath()
	if obj.IsDir() {
		err = fs.RemoveAll(fullPath)
	} else {
		err = fs.Remove(fullPath)
	}
	if err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

func (d *SMB) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return err
	}
	defer release()
	fullPath := filepath.Join(dstDir.GetPath(), stream.GetName())
	out, err := fs.Create(fullPath)
	if err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	defer func() {
		_ = out.Close()
		if errors.Is(err, context.Canceled) {
			_ = fs.Remove(fullPath)
		}
	}()
	err = utils.CopyWithCtx(ctx, out, driver.NewLimitedUploadStream(ctx, stream), stream.GetSize(), up)
	if err != nil {
		return err
	}
	return nil
}

func (d *SMB) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	fs, release, err := d.acquireFS(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	stat, err := fs.Statfs(d.RootFolderPath)
	if err != nil {
		return nil, err
	}
	total := int64(stat.BlockSize() * stat.TotalBlockCount())
	free := int64(stat.BlockSize() * stat.AvailableBlockCount())
	return &model.StorageDetails{
		DiskUsage: model.DiskUsage{
			TotalSpace: total,
			UsedSpace:  total - free,
		},
	}, nil
}

//func (d *SMB) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
//	return nil, errs.NotSupport
//}

var _ driver.Driver = (*SMB)(nil)
