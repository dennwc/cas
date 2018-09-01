package storage

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/dennwc/cas/cow"
	"github.com/dennwc/cas/types"
)

const (
	dirBlobs = "blobs"
	dirPins  = "pins"
	dirTmp   = "tmp"

	roPerm = 0444
)

func NewLocal(dir string, create bool) (*LocalStorage, error) {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if !create {
			return nil, err
		}
		err = os.MkdirAll(filepath.Join(dir, dirBlobs), 0755)
		if err != nil {
			return nil, err
		}
		err = os.MkdirAll(filepath.Join(dir, dirPins), 0755)
		if err != nil {
			return nil, err
		}
		err = os.MkdirAll(filepath.Join(dir, dirTmp), 0755)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return &LocalStorage{dir: dir}, nil
}

type LocalStorage struct {
	dir string
}

func (s *LocalStorage) tmpFile() (*os.File, error) {
	dir := filepath.Join(s.dir, dirTmp)
	return ioutil.TempFile(dir, "")
}

func (s *LocalStorage) blobPath(ref types.Ref) string {
	return filepath.Join(s.dir, dirBlobs, ref.String())
}

func (s *LocalStorage) StatBlob(ctx context.Context, ref types.Ref) (uint64, error) {
	fi, err := os.Stat(s.blobPath(ref))
	if err != nil {
		return 0, err
	}
	return uint64(fi.Size()), nil
}

func (s *LocalStorage) FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	f, err := os.Open(s.blobPath(ref))
	if os.IsNotExist(err) {
		return nil, 0, ErrNotFound
	} else if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, uint64(fi.Size()), nil
}

func (s *LocalStorage) ImportFile(ctx context.Context, path string) (types.SizedRef, error) {
	// first, use CoW to copy the file into temp directory
	f, err := s.tmpFile()
	if err != nil {
		return types.SizedRef{}, err
	}
	f.Close()
	name := f.Name()
	if err = cow.Copy(ctx, name, path); err != nil {
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

func (s *LocalStorage) BeginBlob(ctx context.Context) (BlobWriter, error) {
	f, err := s.tmpFile()
	if err != nil {
		return nil, err
	}
	if t, ok := ctx.Deadline(); ok {
		f.SetWriteDeadline(t)
	}
	return &blobWriter{s: s, ctx: ctx, f: f, hw: Hash()}, nil
}

type blobWriter struct {
	s   *LocalStorage
	ctx context.Context
	f   *os.File
	sr  types.SizedRef
	hw  BlobWriter
}

func (w *blobWriter) Write(p []byte) (int, error) {
	_, err := w.hw.Write(p)
	if err != nil {
		return 0, err
	}
	if w.f == nil {
		return 0, ErrBlobCompleted
	}
	return w.f.Write(p)
}

func (w *blobWriter) Complete() (types.SizedRef, error) {
	sr, err := w.hw.Complete()
	if err != nil {
		return types.SizedRef{}, err
	}
	if !w.sr.Ref.Zero() {
		return sr, nil
	}
	w.sr = sr
	err = w.f.Close()
	if err != nil {
		os.Remove(w.f.Name())
		w.f = nil
	}
	return sr, err
}

func (w *blobWriter) Close() error {
	if err := w.hw.Close(); err != nil {
		return err
	}
	if w.f == nil {
		return nil
	}
	w.f.Close()
	err := os.Remove(w.f.Name())
	w.f = nil
	return err
}

func (w *blobWriter) Commit() error {
	if err := w.hw.Commit(); err != nil {
		return err
	}
	if w.f == nil {
		return ErrBlobDiscarded
	}
	if w.sr.Ref.Zero() {
		if _, err := w.Complete(); err != nil {
			return err
		}
	}
	// file already closed, we only need a name now
	name := w.f.Name()
	w.f = nil
	if err := os.Chmod(name, roPerm); err != nil {
		os.Remove(name)
		return err
	}
	path := w.s.blobPath(w.sr.Ref)
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

func (s *LocalStorage) IterateBlobs(ctx context.Context) Iterator {
	return &dirIterator{s: s, dir: filepath.Join(s.dir, dirBlobs)}
}

type dirIterator struct {
	s   *LocalStorage
	dir string

	err   error
	infos []os.FileInfo
	ref   types.Ref
	size  uint64
}

func (it *dirIterator) Next() bool {
	it.ref, it.size = types.Ref{}, 0
	if it.err != nil {
		return false
	}
	if it.infos == nil {
		d, err := os.Open(it.dir)
		if os.IsNotExist(err) {
			it.infos = []os.FileInfo{}
			return false
		} else if err != nil {
			it.err = err
			return false
		}
		infos, err := d.Readdir(-1)
		d.Close()
		if err != nil {
			it.err = err
			return false
		}
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name() < infos[j].Name()
		})
		it.infos = infos
	}
	if len(it.infos) == 0 {
		return false
	}
	info := it.infos[0]
	it.infos = it.infos[1:]
	it.size = uint64(info.Size())
	it.ref, it.err = types.ParseRef(info.Name())
	if it.err != nil {
		return false
	}
	return true
}

func (it *dirIterator) Err() error {
	return it.err
}

func (it *dirIterator) Ref() types.Ref {
	return it.ref
}

func (it *dirIterator) Size() uint64 {
	return it.size
}

func (it *dirIterator) Close() error {
	it.infos = []os.FileInfo{}
	return nil
}

func (s *LocalStorage) pinPath(name string) string {
	return filepath.Join(s.dir, dirPins, name)
}

func (s *LocalStorage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	return ioutil.WriteFile(s.pinPath(name), []byte(ref.String()), 0644)
}

func (s *LocalStorage) DeletePin(ctx context.Context, name string) error {
	return os.Remove(s.pinPath(name))
}

func (s *LocalStorage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	data, err := ioutil.ReadFile(s.pinPath(name))
	if os.IsNotExist(err) {
		return types.Ref{}, ErrNotFound
	} else if err != nil {
		return types.Ref{}, err
	}
	return types.ParseRef(string(data))
}

func (s *LocalStorage) IteratePins(ctx context.Context) PinIterator {
	return &pinIterator{s: s, dir: filepath.Join(s.dir, dirPins)}
}

type pinIterator struct {
	s   *LocalStorage
	dir string

	err   error
	infos []os.FileInfo
	ref   types.Ref
	name  string
}

func (it *pinIterator) Next() bool {
	it.ref, it.name = types.Ref{}, ""
	if it.err != nil {
		return false
	}
	if it.infos == nil {
		d, err := os.Open(it.dir)
		if os.IsNotExist(err) {
			it.infos = []os.FileInfo{}
			return false
		} else if err != nil {
			it.err = err
			return false
		}
		infos, err := d.Readdir(-1)
		d.Close()
		if err != nil {
			it.err = err
			return false
		}
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name() < infos[j].Name()
		})
		it.infos = infos
	}
	if len(it.infos) == 0 {
		return false
	}
	info := it.infos[0]
	it.infos = it.infos[1:]
	it.name = info.Name()
	data, err := ioutil.ReadFile(filepath.Join(it.dir, info.Name()))
	if err != nil {
		it.err = err
		return false
	}
	it.ref, it.err = types.ParseRef(string(data))
	if it.err != nil {
		return false
	}
	return true
}

func (it *pinIterator) Err() error {
	return it.err
}

func (it *pinIterator) Ref() types.Ref {
	return it.ref
}

func (it *pinIterator) Name() string {
	return it.name
}

func (it *pinIterator) Close() error {
	it.infos = []os.FileInfo{}
	return nil
}
