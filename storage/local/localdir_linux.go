//+build linux

package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dennwc/ioctl"
	"golang.org/x/sys/unix"

	"github.com/dennwc/cas/cow"
	"github.com/dennwc/cas/types"
)

const cloneSupported = true

var iocFICLONE = ioctl.IOW(0x94, 9, 4) // from linux/fs.h

func cloneFile(dst, src *os.File) error {
	return ioctl.Ioctl(dst, iocFICLONE, src.Fd())
}

var noTmpFile int32

type storageImpl struct {
	blobDir *os.File
}

func (s *Storage) init() error {
	blobDir, err := os.Open(filepath.Join(s.dir, dirBlobs))
	if err != nil {
		return err
	}
	s.blobDir = blobDir
	return nil
}

func (s *Storage) close() error {
	if s.blobDir != nil {
		s.blobDir.Close()
	}
	return nil
}

func (s *Storage) importFile(ctx context.Context, path string) (types.SizedRef, error) {
	// first, use CoW to copy blocks to a temp file
	f, err := s.tmpFileRaw()
	if err != nil {
		return types.SizedRef{}, err
	}
	f.Close()
	name := f.Name()
	if err = cow.Clone(ctx, name, path); err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	// calculate the hash and move the file into the blobs directory
	if err := os.Chmod(name, roPerm); err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	f, err = os.Open(name)
	if err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	sr, err := types.Hash(f)
	f.Close()
	if err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	if err = os.Rename(name, s.blobPath(sr.Ref)); err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	return sr, nil
}

func (s *Storage) tmpFile(rw bool) (tempFile, error) {
	if atomic.LoadInt32(&noTmpFile) != 0 {
		return s.tmpFileGen()
	}
	flags := unix.O_TMPFILE | unix.O_CLOEXEC
	if rw {
		flags |= unix.O_RDWR
	} else {
		flags |= unix.O_WRONLY
	}
	fd, err := unix.Open(s.dir, flags, 0600)
	switch err {
	case syscall.EISDIR:
		// system doesn't understand this flag; disable permanently
		atomic.StoreInt32(&noTmpFile, 1)
		fallthrough
	case syscall.EOPNOTSUPP:
		return s.tmpFileGen()
	}
	if err != nil {
		return nil, fmt.Errorf("cannot create temp file: %v", err)
	}
	name := fmt.Sprintf("/proc/self/fd/%d", fd)
	f := os.NewFile(uintptr(fd), name)
	return &linuxTmpFile{s: s, f: f}, nil
}

type linuxTmpFile struct {
	s *Storage
	f *os.File
}

func (f *linuxTmpFile) File() *os.File {
	return f.f
}

func (f *linuxTmpFile) Read(p []byte) (int, error) {
	if f.f == nil {
		return 0, os.ErrClosed
	}
	return f.f.Read(p)
}

func (f *linuxTmpFile) Write(p []byte) (int, error) {
	if f.f == nil {
		return 0, os.ErrClosed
	}
	return f.f.Write(p)
}

func (f *linuxTmpFile) Close() error {
	if f.f == nil {
		return nil
	}
	f.f.Close()
	f.f = nil
	return nil
}

func (f *linuxTmpFile) SetWriteDeadline(t time.Time) error {
	if f.f == nil {
		return os.ErrClosed
	}
	return f.f.SetWriteDeadline(t)
}

func (f *linuxTmpFile) Commit(ref types.Ref) error {
	if f.f == nil {
		return os.ErrClosed
	}
	tmp := f.f
	defer tmp.Close()
	f.f = nil

	fd := int(tmp.Fd())

	err := unix.Fchmod(fd, roPerm)
	if err != nil {
		return fmt.Errorf("fchmod: %v", err)
	}

	blobsFD := int(f.s.blobDir.Fd())
	err = unix.Linkat(unix.AT_FDCWD, tmp.Name(), blobsFD, ref.String(), unix.AT_SYMLINK_FOLLOW)
	if err != nil {
		return fmt.Errorf("linkat: %v", err)
	}
	return tmp.Close()
}
