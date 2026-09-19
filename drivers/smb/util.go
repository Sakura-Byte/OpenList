package smb

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"

	"github.com/cloudsoda/go-smb2"
	log "github.com/sirupsen/logrus"
)

const connectionIdleTimeout = 5 * time.Minute

var (
	errSMBClosed          = errors.New("SMB storage is closed")
	errSMBConnectionInUse = errors.New("SMB connection is in use")
)

type smbConnection struct {
	share     *smb2.Share
	session   *smb2.Session
	transport net.Conn
	closeOnce sync.Once
	closeErr  error
}

func (c *smbConnection) close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		var closeErrors []error
		if c.share != nil {
			closeErrors = append(closeErrors, c.share.Umount())
		}

		var logoffErr error
		if c.session != nil {
			logoffErr = c.session.Logoff()
			closeErrors = append(closeErrors, logoffErr)
		}
		if c.transport != nil && (c.session == nil || logoffErr != nil) {
			closeErrors = append(closeErrors, c.transport.Close())
		}
		c.closeErr = errors.Join(closeErrors...)
	})
	return c.closeErr
}

func (d *SMB) updateLastConnTime() {
	d.lastConnTime.Store(time.Now().Unix())
}

func (d *SMB) cleanLastConnTime() {
	d.lastConnTime.Store(0)
}

func (d *SMB) getLastConnTime() time.Time {
	return time.Unix(d.lastConnTime.Load(), 0)
}

func (d *SMB) initFS(ctx context.Context) error {
	d.connMu.Lock()
	defer d.connMu.Unlock()

	if d.activeLeases != 0 {
		return errSMBConnectionInUse
	}
	d.dropped = false
	if err := d.closeConnectionLocked(); err != nil {
		log.Warnf("failed to close previous SMB connection: %v", err)
	}
	return d.openConnectionLocked(ctx)
}

func (d *SMB) openConnectionLocked(ctx context.Context) error {
	newConnection := d.newConnection
	if newConnection == nil {
		newConnection = d.dial
	}
	connection, err := newConnection(ctx)
	if err != nil {
		return err
	}
	if connection == nil {
		return errors.New("SMB connection factory returned nil")
	}
	d.connection = connection
	d.updateLastConnTime()
	return nil
}

func (d *SMB) dial(ctx context.Context) (*smbConnection, error) {
	transport, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.Address)
	if err != nil {
		return nil, err
	}

	dialer := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     d.Username,
			Password: d.Password,
		},
	}
	session, err := dialer.DialConn(ctx, transport, d.Address)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}

	connection := &smbConnection{
		session:   session,
		transport: transport,
	}
	connection.share, err = session.Mount(d.ShareName)
	if err != nil {
		_ = connection.close()
		return nil, err
	}
	return connection, nil
}

func (d *SMB) acquireFS(ctx context.Context) (*smb2.Share, func(), error) {
	d.connMu.Lock()
	defer d.connMu.Unlock()

	if d.dropped {
		return nil, nil, errSMBClosed
	}
	stale := time.Since(d.getLastConnTime()) >= connectionIdleTimeout
	if d.connection == nil || stale && d.activeLeases == 0 {
		if err := d.closeConnectionLocked(); err != nil {
			log.Warnf("failed to close stale SMB connection: %v", err)
		}
		if err := d.openConnectionLocked(ctx); err != nil {
			return nil, nil, err
		}
	}

	d.activeLeases++
	var once sync.Once
	release := func() {
		once.Do(d.releaseFS)
	}
	return d.connection.share, release, nil
}

func (d *SMB) releaseFS() {
	d.connMu.Lock()
	if d.activeLeases > 0 {
		d.activeLeases--
	}
	shouldClose := d.dropped && d.activeLeases == 0
	if !shouldClose {
		d.connMu.Unlock()
		return
	}
	connection := d.detachConnectionLocked()
	d.connMu.Unlock()

	if err := connection.close(); err != nil {
		log.Warnf("failed to close dropped SMB connection: %v", err)
	}
}

func (d *SMB) dropFS() error {
	d.connMu.Lock()
	d.dropped = true
	if d.activeLeases != 0 {
		d.connMu.Unlock()
		return nil
	}
	connection := d.detachConnectionLocked()
	d.connMu.Unlock()
	return connection.close()
}

func (d *SMB) closeConnectionLocked() error {
	return d.detachConnectionLocked().close()
}

func (d *SMB) detachConnectionLocked() *smbConnection {
	connection := d.connection
	d.connection = nil
	d.cleanLastConnTime()
	return connection
}

// CopyFile copies a single file from src to dst.
func (d *SMB) CopyFile(src, dst string) error {
	share, release, err := d.acquireFS(context.Background())
	if err != nil {
		return err
	}
	defer release()
	if err := copyFile(share, src, dst); err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

// CopyDir copies a whole directory recursively.
func (d *SMB) CopyDir(src, dst string) error {
	share, release, err := d.acquireFS(context.Background())
	if err != nil {
		return err
	}
	defer release()
	if err := copyDir(share, src, dst); err != nil {
		d.cleanLastConnTime()
		return err
	}
	d.updateLastConnTime()
	return nil
}

// Exists reports whether name exists on the mounted share.
func (d *SMB) Exists(name string) bool {
	share, release, err := d.acquireFS(context.Background())
	if err != nil {
		return false
	}
	defer release()
	return exists(share, name)
}

// CreateNestedFile creates path and any missing parent directories.
func (d *SMB) CreateNestedFile(path string) (*smb2.File, error) {
	share, release, err := d.acquireFS(context.Background())
	if err != nil {
		return nil, err
	}
	defer release()
	return createNestedFile(share, path)
}

// copyFile copies a single file from src to dst.
func copyFile(share *smb2.Share, src, dst string) error {
	var err error
	var srcfd *smb2.File
	var dstfd *smb2.File
	var srcinfo fs.FileInfo

	if srcfd, err = share.Open(src); err != nil {
		return err
	}
	defer srcfd.Close()

	if dstfd, err = createNestedFile(share, dst); err != nil {
		return err
	}
	defer dstfd.Close()

	if _, err = utils.CopyWithBuffer(dstfd, srcfd); err != nil {
		return err
	}
	if srcinfo, err = share.Stat(src); err != nil {
		return err
	}
	return share.Chmod(dst, srcinfo.Mode())
}

// copyDir copies a whole directory recursively.
func copyDir(share *smb2.Share, src, dst string) error {
	var err error
	var fds []fs.FileInfo
	var srcinfo fs.FileInfo

	if srcinfo, err = share.Stat(src); err != nil {
		return err
	}
	if err = share.MkdirAll(dst, srcinfo.Mode()); err != nil {
		return err
	}
	if fds, err = share.ReadDir(src); err != nil {
		return err
	}
	for _, fd := range fds {
		srcfp := filepath.Join(src, fd.Name())
		dstfp := filepath.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = copyDir(share, srcfp, dstfp); err != nil {
				return err
			}
		} else if err = copyFile(share, srcfp, dstfp); err != nil {
			return err
		}
	}
	return nil
}

func exists(share *smb2.Share, name string) bool {
	if _, err := share.Stat(name); err != nil {
		return !os.IsNotExist(err)
	}
	return true
}

func createNestedFile(share *smb2.Share, path string) (*smb2.File, error) {
	basePath := filepath.Dir(path)
	if !exists(share, basePath) {
		if err := share.MkdirAll(basePath, 0700); err != nil {
			return nil, err
		}
	}
	return share.Create(path)
}
