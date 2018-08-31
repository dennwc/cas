package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

const (
	DefaultDir = ".cas"
	DefaultPin = "root"
)

const (
	maxDirEntries = 1024
)

type OpenOptions struct {
	Dir     string
	Create  bool
	Storage storage.Storage
}

func Open(opt OpenOptions) (*Storage, error) {
	s := opt.Storage
	if s == nil {
		var err error
		s, err = storage.NewLocal(opt.Dir, opt.Create)
		if err != nil {
			return nil, err
		}
	}
	return New(s)
}

func New(st storage.Storage) (*Storage, error) {
	return &Storage{st: st}, nil
}

var _ storage.Storage = (*Storage)(nil)

type Storage struct {
	st storage.Storage
}

func (s *Storage) FetchBlob(ctx context.Context, ref Ref) (io.ReadCloser, uint64, error) {
	if ref.Empty() {
		// generate empty blobs
		return ioutil.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	return s.st.FetchBlob(ctx, ref)
}

func (s *Storage) IterateBlobs(ctx context.Context) storage.Iterator {
	return s.st.IterateBlobs(ctx)
}

func (s *Storage) StatBlob(ctx context.Context, ref Ref) (uint64, error) {
	if ref.Empty() {
		return 0, nil
	}
	return s.st.StatBlob(ctx, ref)
}

func (s *Storage) StoreBlob(ctx context.Context, exp Ref, r io.Reader) (SizedRef, error) {
	if exp.Empty() {
		// do not store empty blobs - we can generate them
		var b [1]byte
		_, err := r.Read(b[:])
		if err == io.EOF {
			return SizedRef{Ref: exp, Size: 0}, nil
		}
		return SizedRef{}, fmt.Errorf("expected empty blob")
	}
	if !exp.Zero() {
		if sz, err := s.StatBlob(ctx, exp); err == nil {
			return SizedRef{Ref: exp, Size: sz}, nil
		}
	}
	return s.st.StoreBlob(ctx, exp, r)
}

func (s *Storage) StoreSchema(ctx context.Context, o schema.Object) (SizedRef, error) {
	buf := new(bytes.Buffer)
	if err := schema.Encode(buf, o); err != nil {
		return SizedRef{}, err
	}
	exp := types.BytesRef(buf.Bytes())
	return s.StoreBlob(ctx, exp, buf)
}

func (s *Storage) storeAsFile(ctx context.Context, r io.Reader, fi os.FileInfo) (*schema.DirEntry, error) {
	sr, err := s.StoreBlob(ctx, types.Ref{}, r)
	if err != nil {
		return nil, err
	} else if sr.Size != uint64(fi.Size()) {
		return nil, fmt.Errorf("file changed while writing it")
	}
	m := &schema.DirEntry{
		Ref: sr.Ref, Size: sr.Size,
		Name: filepath.Base(fi.Name()),
	}
	return m, nil
}

func (s *Storage) StoreAsFile(ctx context.Context, r io.Reader, fi os.FileInfo) (SizedRef, error) {
	m, err := s.storeAsFile(ctx, r, fi)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreSchema(ctx, m)
}

func (s *Storage) StoreFilePath(ctx context.Context, path string) (SizedRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return SizedRef{}, nil
	}
	defer f.Close()
	return s.StoreFile(ctx, f)
}

func (s *Storage) storeDirList(ctx context.Context, list []schema.DirEntry) (SizedRef, schema.DirEntry, error) {
	var (
		cnt  uint
		size uint64
	)
	for _, e := range list {
		cnt += e.Count + 1
		size += e.Size
	}
	m := &schema.Directory{List: list}
	sr, err := s.StoreSchema(ctx, m)
	if err != nil {
		return SizedRef{}, schema.DirEntry{}, err
	}
	return sr, schema.DirEntry{Ref: sr.Ref, Count: cnt, Size: size}, nil
}
func (s *Storage) storeDirJoin(ctx context.Context, refs []Ref, list []schema.JoinDirectories) (SizedRef, schema.JoinDirectories, error) {
	var (
		cnt  uint
		size uint64
	)
	for _, e := range list {
		cnt += e.Count
		size += e.Size
	}
	m := schema.JoinDirectories{List: refs, Count: cnt, Size: size}
	sr, err := s.StoreSchema(ctx, &m)
	if err != nil {
		return SizedRef{}, schema.JoinDirectories{}, err
	}
	return sr, m, nil
}
func (s *Storage) storeDir(ctx context.Context, dir *os.File) (SizedRef, schema.DirEntry, error) {
	var base []schema.DirEntry
	for {
		buf, err := dir.Readdir(maxDirEntries)
		if err == io.EOF {
			break
		} else if err != nil {
			return SizedRef{}, schema.DirEntry{}, err
		}
		for _, fi := range buf {
			if fi.Name() == DefaultDir {
				continue
			}
			f, err := os.Open(filepath.Join(dir.Name(), fi.Name()))
			if err != nil {
				return SizedRef{}, schema.DirEntry{}, err
			}
			if fi.IsDir() {
				sr, st, err := s.storeDir(ctx, f)
				f.Close()
				if err != nil {
					return SizedRef{}, schema.DirEntry{}, err
				}
				st.Ref = sr.Ref
				st.Name = fi.Name()
				base = append(base, st)
			} else {
				ent, err := s.storeAsFile(ctx, f, fi)
				f.Close()
				if err != nil {
					return SizedRef{}, schema.DirEntry{}, err
				}
				base = append(base, *ent)
			}
		}
	}
	sort.Slice(base, func(i, j int) bool {
		return base[i].Name < base[j].Name
	})
	var (
		level []schema.JoinDirectories
		refs  []Ref
		cur   schema.JoinDirectories
	)
	if len(base) <= maxDirEntries {
		return s.storeDirList(ctx, base)
	}
	for len(base) > 0 {
		page := base
		if len(page) > maxDirEntries {
			page = page[:maxDirEntries]
		}
		base = base[len(page):]

		sr, st, err := s.storeDirList(ctx, page)
		if err != nil {
			return SizedRef{}, schema.DirEntry{}, err
		}
		cur.Size += st.Size
		cur.Count += st.Count
		cur.List = append(cur.List, sr.Ref)
		if len(cur.List) >= maxDirEntries || len(base) == 0 {
			sr, err = s.StoreSchema(ctx, &cur)
			if err != nil {
				return SizedRef{}, schema.DirEntry{}, err
			}
			level = append(level, cur)
			refs = append(refs, sr.Ref)
			cur = schema.JoinDirectories{}
		}
	}
	for len(level) > 1 {
		var (
			newLevel []schema.JoinDirectories
			newRefs  []Ref
		)
		for len(level) > 0 {
			page := level
			if len(page) > maxDirEntries {
				page = page[:maxDirEntries]
			}
			pref := refs[:len(page)]

			level = level[len(page):]
			refs = refs[len(page):]

			sr, cur, err := s.storeDirJoin(ctx, pref, page)
			if err != nil {
				return SizedRef{}, schema.DirEntry{}, err
			}
			newLevel = append(newLevel, cur)
			newRefs = append(newRefs, sr.Ref)
		}
		level, refs = newLevel, newRefs
	}
	top := level[0]
	sr, err := s.StoreSchema(ctx, &top)
	if err != nil {
		return SizedRef{}, schema.DirEntry{}, err
	}
	return sr, schema.DirEntry{Ref: sr.Ref, Count: top.Count, Size: top.Size}, nil
}

func (s *Storage) StoreFile(ctx context.Context, f *os.File) (SizedRef, error) {
	fi, err := f.Stat()
	if err != nil {
		return SizedRef{}, err
	}
	if fi.IsDir() {
		sr, _, err := s.storeDir(ctx, f)
		return sr, err
	}
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreAsFile(ctx, f, fi)
}

func (s *Storage) StoreURLContent(ctx context.Context, url string) (SizedRef, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreHTTPContent(ctx, req)
}

func (s *Storage) StoreHTTPContent(ctx context.Context, req *http.Request) (SizedRef, error) {
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return SizedRef{}, fmt.Errorf("cannot fetch http content: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return SizedRef{}, fmt.Errorf("status: %v", resp.Status)
	}
	sr, err := s.StoreBlob(ctx, types.Ref{}, resp.Body)
	if err != nil {
		return SizedRef{}, err
	}
	resp.Body.Close()

	m := schema.WebContent{
		URL: req.URL.String(), Ref: sr.Ref, Size: sr.Size,
		ETag: strings.Trim(resp.Header.Get("ETag"), `"`),
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		if t, err := time.Parse(time.RFC1123, v); err == nil {
			t = t.UTC()
			m.TS = &t
		}
	} else if m.ETag == "" {
		t := time.Now().UTC()
		m.TS = &t
	}
	return s.StoreSchema(ctx, &m)
}
