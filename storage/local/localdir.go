package local

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
	"github.com/dennwc/cas/xattr"
)

const (
	dirBlobs     = "blobs"
	dirPins      = "pins"
	dirTmp       = "tmp"
	dirIndex     = "indexes"
	dirUnindexed = "unindexed"

	xattrNS         = "cas."
	xattrSchemaType = xattrNS + "schema.type"

	indexType = "@type"

	readDirPage = 1024

	roPerm  = 0444
	dirPerm = 0755
)

var (
	errCantClone = errors.New("copy-on-write not supported")
)

var (
	mkDirs = []string{
		dirBlobs,
		dirPins,
		dirTmp,
	}
)

var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.BlobIndexer = (*Storage)(nil)
)

func init() {
	storage.RegisterConfig("cas:LocalDirConfig", &Config{})
}

type Config struct {
	Dir string `json:"dir"`
}

func (c *Config) References() []types.Ref {
	return nil
}

func (c *Config) OpenStorage(ctx context.Context) (storage.Storage, error) {
	s, err := New(c.Dir, false)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func New(dir string, create bool) (*Storage, error) {
	_, err := os.Stat(dir)
	if err == nil {
		_, err = os.Stat(filepath.Join(dir, dirBlobs))
	}
	if os.IsNotExist(err) {
		if !create {
			return nil, err
		}
		err = os.MkdirAll(dir, dirPerm)
		if err != nil {
			return nil, err
		}
		for _, name := range mkDirs {
			err = os.Mkdir(filepath.Join(dir, name), dirPerm)
			if err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}
	s := &Storage{
		dir: dir,
	}
	if err := s.initIndexes(); err != nil {
		s.Close()
		return nil, err
	}
	if err := s.init(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

type Storage struct {
	dir       string
	unindexed *os.File
	storageImpl
}

func (s *Storage) ensureDir(dir string) error {
	path := filepath.Join(s.dir, dir)
	_, err := os.Stat(path)
	if err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Mkdir(path, dirPerm)
}
func (s *Storage) openOrMake(dir string) (*os.File, error) {
	path := filepath.Join(s.dir, dir)
	d, err := os.Open(path)
	if os.IsNotExist(err) {
		err = os.Mkdir(path, dirPerm)
		if err != nil {
			return nil, err
		}
		d, err = os.Open(path)
	}
	return d, err
}
func (s *Storage) ensureIndex(field string) error {
	return s.ensureDir(filepath.Join(dirIndex, field))
}
func (s *Storage) initIndexes() error {
	var err error
	s.unindexed, err = s.openOrMake(dirUnindexed)
	if err != nil {
		return err
	}
	if err = s.ensureDir(dirIndex); err != nil {
		return err
	}
	if err = s.ensureIndex(indexType); err != nil {
		return err
	}
	return nil
}

func (s *Storage) Close() error {
	s.closeIndexes()
	return s.close()
}

func (s *Storage) closeIndexes() error {
	if s.unindexed != nil {
		s.unindexed.Close()
	}
	return nil
}

type tempFile interface {
	io.Writer
	io.Reader
	io.Closer
	File() *os.File
	SetWriteDeadline(t time.Time) error
	Commit(ref types.Ref) error
}

func (s *Storage) tmpFileRaw() (*os.File, error) {
	dir := filepath.Join(s.dir, dirTmp)
	return ioutil.TempFile(dir, "blob_")
}

func (s *Storage) tmpFileGen() (tempFile, error) {
	f, err := s.tmpFileRaw()
	if err != nil {
		return nil, err
	}
	return &genTmpFile{s: s, f: f}, nil
}

func (s *Storage) blobPath(ref types.Ref) string {
	return filepath.Join(s.dir, dirBlobs, ref.String())
}

// removeIfInvalid does a quick check for an invalid blob and removes it, if necessary, returning true as the result.
func (s *Storage) removeIfInvalid(fi os.FileInfo, ref types.Ref) (bool, error) {
	// the only case that can be detected is an empty file stored with a non-empty ref
	if fi.Size() != 0 || ref.Empty() {
		return false, nil
	}
	// it's definitely a corrupted blob - remove it
	// those might be left by an instant system shutdown

	// if any error happens during cleanup - ignore it and report "ref mismatch"
	err := os.Chmod(s.blobPath(ref), 0666)
	if err != nil {
		return false, storage.ErrRefMissmatch{Exp: ref, Got: types.BytesRef(nil)}
	}
	err = os.Remove(s.blobPath(ref))
	if err != nil {
		return false, storage.ErrRefMissmatch{Exp: ref, Got: types.BytesRef(nil)}
	}
	return true, nil
}

func (s *Storage) StatBlob(ctx context.Context, ref types.Ref) (uint64, error) {
	if ref.Zero() {
		return 0, storage.ErrInvalidRef
	}
	fi, err := os.Stat(s.blobPath(ref))
	if err != nil {
		return 0, err
	}
	if invalid, err := s.removeIfInvalid(fi, ref); err != nil {
		return 0, err
	} else if invalid {
		return 0, storage.ErrNotFound
	}
	return uint64(fi.Size()), nil
}

func (s *Storage) FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	if ref.Zero() {
		return nil, 0, storage.ErrInvalidRef
	}
	f, err := os.Open(s.blobPath(ref))
	if os.IsNotExist(err) {
		return nil, 0, storage.ErrNotFound
	} else if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if invalid, err := s.removeIfInvalid(fi, ref); err != nil {
		f.Close()
		return nil, 0, err
	} else if invalid {
		f.Close()
		return nil, 0, storage.ErrNotFound
	}
	return f, uint64(fi.Size()), nil
}

func (s *Storage) ImportFile(ctx context.Context, path string) (types.SizedRef, error) {
	if !cloneSupported {
		return types.SizedRef{}, errCantClone
	}
	inp, err := os.Open(path)
	if err != nil {
		return types.SizedRef{}, err
	}
	defer inp.Close()

	dst, err := s.tmpFile(true)
	if err != nil {
		return types.SizedRef{}, err
	}

	// copy the blocks directly by cloning the file
	err = cloneFile(dst.File(), inp)
	if err != nil {
		dst.Close()
		return types.SizedRef{}, err
	}
	// get the hash of the file by reading the clone (snapshot)
	sr, err := types.Hash(dst)
	if err != nil {
		dst.Close()
		return types.SizedRef{}, err
	}

	// store the file
	err = dst.Commit(sr.Ref)
	if err != nil {
		dst.Close()
		return types.SizedRef{}, err
	}
	return sr, nil
}

func (s *Storage) addNotIndexed(f *os.File, ref types.Ref) error {
	return linkFile(s.unindexed, ref.String(), f)
}

func (s *Storage) BeginBlob(ctx context.Context) (storage.BlobWriter, error) {
	f, err := s.tmpFile(false)
	if err != nil {
		return nil, err
	}
	if t, ok := ctx.Deadline(); ok {
		f.SetWriteDeadline(t)
	}
	return &blobWriter{s: s, ctx: ctx, f: f, hw: storage.Hash()}, nil
}

type blobWriter struct {
	s   *Storage
	ctx context.Context
	f   tempFile
	sr  types.SizedRef
	hw  storage.BlobWriter
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
		return 0, storage.ErrBlobCompleted
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
	return sr, err
}

func (w *blobWriter) Close() error {
	if err := w.hw.Close(); err != nil {
		return err
	}
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func (w *blobWriter) Commit() error {
	if err := w.hw.Commit(); err != nil {
		return err
	}
	if w.f == nil {
		return storage.ErrBlobDiscarded
	}
	if w.sr.Ref.Zero() {
		if _, err := w.Complete(); err != nil {
			return err
		}
	}
	// file already closed, we only need a name now
	err := w.f.Commit(w.sr.Ref)
	w.f = nil
	return err
}

func (s *Storage) IterateBlobs(ctx context.Context) storage.Iterator {
	return &dirIterator{s: s, dir: filepath.Join(s.dir, dirBlobs)}
}

type dirIterator struct {
	s   *Storage
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
	for {
		if len(it.infos) == 0 {
			return false
		}
		info := it.infos[0]
		it.infos = it.infos[1:]
		if !info.Mode().IsRegular() {
			continue
		}
		it.sr.Size = uint64(info.Size())
		it.sr.Ref, it.err = types.ParseRef(info.Name())
		if it.err != nil {
			return false
		}
		if invalid, err := it.s.removeIfInvalid(info, it.sr.Ref); err != nil {
			it.err = err
			return false
		} else if invalid {
			continue
		}
		return true
	}
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

func (s *Storage) pinPath(name string) string {
	return filepath.Join(s.dir, dirPins, name)
}

func (s *Storage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	return ioutil.WriteFile(s.pinPath(name), []byte(ref.String()), 0644)
}

func (s *Storage) DeletePin(ctx context.Context, name string) error {
	return os.Remove(s.pinPath(name))
}

func (s *Storage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	data, err := ioutil.ReadFile(s.pinPath(name))
	if os.IsNotExist(err) {
		return types.Ref{}, storage.ErrNotFound
	} else if err != nil {
		return types.Ref{}, err
	}
	return types.ParseRef(string(data))
}

func (s *Storage) IteratePins(ctx context.Context) storage.PinIterator {
	return &pinIterator{s: s, dir: filepath.Join(s.dir, dirPins)}
}

type pinIterator struct {
	s   *Storage
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

func (s *Storage) IterateSchema(ctx context.Context, typs ...string) storage.SchemaIterator {
	if len(typs) == 0 {
		return &schemaAnyIterator{
			s: s, ctx: ctx,
			blobs: s.iterateNames(ctx, dirBlobs, true),
		}
	}
	filter := make(map[string]struct{})
	for _, v := range typs {
		filter[v] = struct{}{}
	}
	return &schemaIterator{
		s: s, ctx: ctx,
		types: typs, filter: filter,
	}
}

func (s *Storage) resetIndexes() error {
	dstDir := filepath.Join(s.dir, dirUnindexed)
	// drop indexes
	s.closeIndexes()
	if err := os.RemoveAll(dstDir); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(s.dir, dirIndex)); err != nil {
		return err
	}
	if err := s.initIndexes(); err != nil {
		return err
	}
	// mark every blob as unindexed
	srcDir := filepath.Join(s.dir, dirBlobs)
	d, err := os.Open(srcDir)
	if err != nil {
		return err
	}
	defer d.Close()

	for {
		buf, err := d.Readdirnames(readDirPage)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		for _, name := range buf {
			err = os.Link(filepath.Join(srcDir, name), filepath.Join(dstDir, name))
			if err != nil {
				return err
			}
		}
	}
}

func (s *Storage) ReindexSchema(ctx context.Context, force bool) error {
	if force {
		if err := s.resetIndexes(); err != nil {
			return err
		}
	}
	// only restore the global index
	it := s.IterateSchema(ctx)
	it.(*schemaAnyIterator).force = true
	for it.Next() {
		_ = it.SchemaRef()
	}
	return it.Err()
}

func (s *Storage) FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	if ref.Zero() {
		return nil, 0, storage.ErrInvalidRef
	}
	if typ, err := xattr.GetString(s.blobPath(ref), xattrSchemaType); err == nil && typ == "" {
		return nil, 0, schema.ErrNotSchema
	}
	return s.FetchBlob(ctx, ref)
}

func (s *Storage) iterateNames(ctx context.Context, dir string, fix bool) *namesIterator {
	return &namesIterator{
		s: s, dir: filepath.Join(s.dir, dir),
		noRemove: !fix,
	}
}

type namesIterator struct {
	s      *Storage
	dir    string
	d      *os.File
	buf    []os.FileInfo
	filter func(path string) (bool, error)

	noRemove bool
	sr       types.SizedRef
	err      error
}

func (it *namesIterator) Next() bool {
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
			buf, err := it.d.Readdir(readDirPage)
			if err == io.EOF {
				return false
			} else if err != nil {
				it.err = err
				return false
			}
			it.buf = buf
		}
		for len(it.buf) > 0 {
			fi := it.buf[0]
			it.buf = it.buf[1:]
			name := fi.Name()

			if it.filter != nil {
				ok, err := it.filter(filepath.Join(it.dir, name))
				if err != nil {
					it.err = err
					return false
				} else if !ok {
					continue
				}
			}

			ref, err := types.ParseRef(name)
			if err != nil {
				it.err = err
				return false
			}
			if !it.noRemove {
				if invalid, err := it.s.removeIfInvalid(fi, ref); err != nil {
					it.err = err
					return false
				} else if invalid {
					continue
				}
			}
			it.sr.Ref, it.sr.Size = ref, uint64(fi.Size())
			return true
		}
	}
}

func (it *namesIterator) Err() error {
	return it.err
}

func (it *namesIterator) Close() error {
	if it.d != nil {
		it.d.Close()
		it.d = nil
	}
	it.buf = nil
	return it.err
}

func (it *namesIterator) SizedRef() types.SizedRef {
	return it.sr
}

// schemaIterator uses indexes to iterate over blobs with a specific schema type.
// It will automatically index blobs that are unindexed.
type schemaIterator struct {
	s   *Storage
	ctx context.Context

	types     []string
	filter    map[string]struct{}
	unindexed bool

	it *namesIterator

	sr types.SchemaRef
}

func (it *schemaIterator) Next() bool {
	for {
		if it.it == nil {
			it.sr.Type = ""
			if it.unindexed {
				// drained unindexed blobs - we are done
				return false
			}
			if len(it.types) == 0 {
				// no types left - list unindexed blobs
				it.unindexed = true
				it.it = it.s.iterateNames(it.ctx, dirUnindexed, false)
				it.it.filter = it.filterUnindexed
			} else {
				// list types one by one
				typ := it.types[0]
				it.types = it.types[1:]
				it.sr.Type = typ
				ipath := filepath.Join(dirIndex, indexType, typ)
				it.it = it.s.iterateNames(it.ctx, ipath, false)
			}
		}
		if it.it.Next() {
			sr := it.it.SizedRef()
			it.sr.Ref, it.sr.Size = sr.Ref, sr.Size
			return true
		}
		it.it.Close()
		it.it = nil
	}
}

func (it *schemaIterator) filterUnindexed(path string) (bool, error) {
	typ, err := it.indexType(path)
	if err != nil {
		return false, err
	} else if typ == "" {
		return false, nil
	}
	if _, ok := it.filter[typ]; !ok {
		return false, nil
	}
	it.sr.Type = typ
	return true, nil
}

func (it *schemaIterator) indexType(path string) (string, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// blob gone
		return "", nil
	} else if err != nil {
		return "", err
	}
	defer f.Close()

	typ, err := schema.DecodeType(f)
	if err == schema.ErrNotSchema || typ == "" {
		// data blob - remove from unindexed list
		err = os.Remove(path)
		return "", err
	} else if err != nil {
		return "", err
	}
	// schema blob - move it to the right index folder
	sref := filepath.Base(path)
	ipath := filepath.Join(it.s.dir, dirIndex, indexType, typ)
	err = os.Rename(path, filepath.Join(ipath, sref))
	if os.IsNotExist(err) {
		err = os.MkdirAll(ipath, dirPerm)
		if err != nil {
			return typ, err
		}
		err = os.Rename(path, filepath.Join(ipath, sref))
	}
	return typ, err
}

func (it *schemaIterator) Err() error {
	if it.it == nil {
		return nil
	}
	return it.it.Err()
}

func (it *schemaIterator) Close() error {
	if it.it != nil {
		it.it.Close()
		it.it = nil
	}
	it.unindexed = true
	it.types = nil
	return nil
}

func (it *schemaIterator) SizedRef() types.SizedRef {
	return it.sr.SizedRef()
}

func (it *schemaIterator) SchemaRef() types.SchemaRef {
	return it.sr
}

func (it *schemaIterator) Decode() (schema.Object, error) {
	rc, _, err := it.s.FetchBlob(it.ctx, it.SizedRef().Ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return schema.Decode(rc)
}

// schemaAnyIterator iterates over all blobs and lists only schema blobs.
// It stores an xattr that specifies the type of a blob to scan faster.
// Force flag can be set to reindex blobs.
type schemaAnyIterator struct {
	s     *Storage
	ctx   context.Context
	force bool

	blobs *namesIterator

	typ string
}

func (it *schemaAnyIterator) Next() bool {
	if it.blobs.filter == nil {
		it.blobs.filter = it.filterType
	}
	if !it.blobs.Next() {
		it.typ = ""
		return false
	}
	return true
}

func (it *schemaAnyIterator) filterType(path string) (bool, error) {
	typ, err := it.getType(path)
	if err != nil {
		return false, err
	} else if typ == "" {
		return false, nil
	}
	it.typ = typ
	return true, nil
}

func (it *schemaAnyIterator) getType(path string) (string, error) {
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

func (it *schemaAnyIterator) Err() error {
	return it.blobs.Err()
}

func (it *schemaAnyIterator) Close() error {
	return it.blobs.Close()
}

func (it *schemaAnyIterator) SizedRef() types.SizedRef {
	return it.blobs.SizedRef()
}

func (it *schemaAnyIterator) SchemaRef() types.SchemaRef {
	sr := it.SizedRef()
	return types.SchemaRef{
		Ref: sr.Ref, Size: sr.Size,
		Type: it.typ,
	}
}

func (it *schemaAnyIterator) Decode() (schema.Object, error) {
	rc, _, err := it.s.FetchBlob(it.ctx, it.SizedRef().Ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return schema.Decode(rc)
}

type genTmpFile struct {
	s *Storage
	f *os.File
}

func (f *genTmpFile) File() *os.File {
	return f.f
}

func (f *genTmpFile) Write(p []byte) (int, error) {
	if f.f == nil {
		return 0, os.ErrClosed
	}
	return f.f.Write(p)
}

func (f *genTmpFile) Read(p []byte) (int, error) {
	if f.f == nil {
		return 0, os.ErrClosed
	}
	return f.f.Read(p)
}

func (f *genTmpFile) Close() error {
	if f.f == nil {
		return nil
	}
	f.f.Close()
	os.Remove(f.f.Name())
	f.f = nil
	return nil
}

func (f *genTmpFile) SetWriteDeadline(t time.Time) error {
	if f.f == nil {
		return os.ErrClosed
	}
	return f.f.SetWriteDeadline(t)
}

func (f *genTmpFile) Commit(ref types.Ref) error {
	if f.f == nil {
		return os.ErrClosed
	}
	tmp := f.f
	f.f = nil
	defer tmp.Close()
	name := tmp.Name()

	if err := os.Chmod(name, roPerm); err != nil {
		os.Remove(name)
		return err
	}
	path := f.s.blobPath(ref)
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	if err := f.s.addNotIndexed(tmp, ref); err != nil {
		return err
	}
	return tmp.Close()
}
