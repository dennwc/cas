package storage

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/dennwc/cas/types"
)

const (
	dirBlobs = "blobs"
	dirPins  = "pins"
	dirTmp   = "tmp"
)

func NewLocal(dir string, create bool) (Storage, error) {
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
	return &localStorage{dir: dir}, nil
}

type localStorage struct {
	dir string
}

func (s *localStorage) tmpFile() (*os.File, error) {
	dir := filepath.Join(s.dir, dirTmp)
	return ioutil.TempFile(dir, "")
}

func (s *localStorage) blobPath(ref types.Ref) string {
	return filepath.Join(s.dir, dirBlobs, ref.String())
}

func (s *localStorage) StatBlob(ctx context.Context, ref types.Ref) (uint64, error) {
	fi, err := os.Stat(s.blobPath(ref))
	if err != nil {
		return 0, err
	}
	return uint64(fi.Size()), nil
}

func (s *localStorage) FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
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

func (s *localStorage) StoreBlob(ctx context.Context, exp types.Ref, r io.Reader) (types.SizedRef, error) {
	f, err := s.tmpFile()
	if err != nil {
		return types.SizedRef{}, fmt.Errorf("cannot create temp file: %v", err)
	}
	name := f.Name()

	ref := types.NewRef()
	h := ref.Hash()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		f.Close()
		os.Remove(name)
		return types.SizedRef{}, fmt.Errorf("cannot write file: %v", err)
	}
	if err = f.Close(); err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	ref = ref.WithHash(h)
	if !exp.Zero() && ref != exp {
		os.Remove(name)
		return types.SizedRef{}, ErrRefMissmatch{Exp: exp, Got: ref}
	}
	if err = os.Rename(name, s.blobPath(ref)); err != nil {
		os.Remove(name)
		return types.SizedRef{}, err
	}
	name = s.blobPath(ref)
	if err = os.Chmod(name, 0444); err != nil {
		return types.SizedRef{}, err
	}
	return types.SizedRef{Ref: ref, Size: uint64(n)}, nil
}

func (s *localStorage) IterateBlobs(ctx context.Context) Iterator {
	return &dirIterator{s: s, dir: filepath.Join(s.dir, dirBlobs)}
}

type dirIterator struct {
	s   *localStorage
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

func (s *localStorage) pinPath(name string) string {
	return filepath.Join(s.dir, dirPins, name)
}

func (s *localStorage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	return ioutil.WriteFile(s.pinPath(name), []byte(ref.String()), 0644)
}

func (s *localStorage) DeletePin(ctx context.Context, name string) error {
	return os.Remove(s.pinPath(name))
}

func (s *localStorage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	data, err := ioutil.ReadFile(s.pinPath(name))
	if os.IsNotExist(err) {
		return types.Ref{}, ErrNotFound
	} else if err != nil {
		return types.Ref{}, err
	}
	return types.ParseRef(string(data))
}

func (s *localStorage) IteratePins(ctx context.Context) PinIterator {
	return &pinIterator{s: s, dir: filepath.Join(s.dir, dirPins)}
}

type pinIterator struct {
	s   *localStorage
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
