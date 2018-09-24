package storage

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/dennwc/cas/cow"
	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
	"github.com/dennwc/cas/xattr"
)

const (
	dirBlobs = "blobs"
	dirPins  = "pins"
	dirTmp   = "tmp"

	xattrNS         = "cas."
	xattrSchemaType = xattrNS + "schema.type"

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

func (w *blobWriter) Size() uint64 {
	return w.hw.Size()
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
	sr    types.SizedRef
}

func (it *dirIterator) Next() bool {
	it.sr = types.SizedRef{}
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
	it.sr.Size = uint64(info.Size())
	it.sr.Ref, it.err = types.ParseRef(info.Name())
	if it.err != nil {
		return false
	}
	return true
}

func (it *dirIterator) Err() error {
	return it.err
}

func (it *dirIterator) SizedRef() types.SizedRef {
	return it.sr
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
	cur   types.Pin
}

func (it *pinIterator) Next() bool {
	it.cur = types.Pin{}
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
	it.cur.Name = info.Name()
	data, err := ioutil.ReadFile(filepath.Join(it.dir, info.Name()))
	if err != nil {
		it.err = err
		return false
	}
	it.cur.Ref, it.err = types.ParseRef(string(data))
	if it.err != nil {
		return false
	}
	return true
}

func (it *pinIterator) Err() error {
	return it.err
}

func (it *pinIterator) Pin() types.Pin {
	return it.cur
}

func (it *pinIterator) Close() error {
	it.infos = []os.FileInfo{}
	return nil
}

func (s *LocalStorage) IterateSchema(ctx context.Context, typs ...string) SchemaIterator {
	var filter map[string]struct{}
	if len(typs) != 0 {
		filter = make(map[string]struct{})
		for _, v := range typs {
			filter[v] = struct{}{}
		}
	}
	return &schemaIterator{s: s, ctx: ctx, typs: filter, dir: filepath.Join(s.dir, dirBlobs)}
}

func (s *LocalStorage) Reindex(ctx context.Context, force bool) error {
	it := &schemaIterator{s: s, ctx: ctx, force: force, dir: filepath.Join(s.dir, dirBlobs)}
	defer it.Close()
	for it.Next() {
		_ = it.SchemaRef()
	}
	return it.Err()
}

func (s *LocalStorage) FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	if typ, err := xattr.GetString(s.blobPath(ref), xattrSchemaType); err == nil && typ == "" {
		return nil, 0, schema.ErrNotSchema
	}
	return s.FetchBlob(ctx, ref)
}

type schemaIterator struct {
	s     *LocalStorage
	ctx   context.Context
	typs  map[string]struct{}
	dir   string
	force bool

	d   *os.File
	buf []string

	sr  types.SchemaRef
	err error
}

func (it *schemaIterator) Next() bool {
	if it.d == nil {
		d, err := os.Open(it.dir)
		if os.IsNotExist(err) {
			return false
		} else if err != nil {
			it.err = err
			return false
		}
		it.d = d
	}
	for {
		if len(it.buf) == 0 {
			buf, err := it.d.Readdirnames(1024)
			if err == io.EOF {
				return false
			} else if err != nil {
				it.err = err
				return false
			}
			it.buf = buf
		}
		for len(it.buf) > 0 {
			name := it.buf[0]
			it.buf = it.buf[1:]

			typ, err := it.getType(name)
			if err != nil {
				it.err = err
				return false
			} else if typ == "" {
				continue
			}
			if it.typs != nil {
				if _, ok := it.typs[typ]; !ok {
					continue
				}
			}
			ref, err := types.ParseRef(name)
			if err != nil {
				it.err = err
				return false
			}
			st, err := os.Stat(filepath.Join(it.dir, name))
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				it.err = err
				return false
			}
			it.sr.Type, it.sr.Ref, it.sr.Size = typ, ref, uint64(st.Size())
			return true
		}
	}
}

func (it *schemaIterator) getType(name string) (string, error) {
	path := filepath.Join(it.dir, name)
	if !it.force {
		// first try to read cached xattr
		typ, err := xattr.GetString(path, xattrSchemaType)
		if err == nil {
			return typ, nil
		} else if err != nil && err != xattr.ErrNotSet {
			return "", err
		}
	}
	// not set
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	defer f.Close()

	typ, err := schema.DecodeType(f)
	if err == schema.ErrNotSchema || err == nil {
		// files are set to RO so we need to set them to RW and then reset back
		err = os.Chmod(path, 0644)
		if err == nil {
			err = xattr.SetString(path, xattrSchemaType, typ)
			_ = os.Chmod(path, roPerm)
		}
	}
	if err != nil {
		return "", err
	}
	return typ, nil
}

func (it *schemaIterator) Err() error {
	return it.err
}

func (it *schemaIterator) Close() error {
	if it.d != nil {
		it.d.Close()
		it.d = nil
	}
	it.buf = nil
	return it.err
}

func (it *schemaIterator) SizedRef() types.SizedRef {
	return it.sr.SizedRef()
}

func (it *schemaIterator) SchemaRef() types.SchemaRef {
	return it.sr
}

func (it *schemaIterator) Decode() (schema.Object, error) {
	rc, _, err := it.s.FetchBlob(it.ctx, it.sr.Ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return schema.Decode(rc)
}
