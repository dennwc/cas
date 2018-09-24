package cas

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

const (
	maxDirEntries = 1024
)

type FileDesc interface {
	Name() string
	Open() (io.ReadCloser, SizedRef, error)
	SetRef(ref types.SizedRef)
}

func LocalFile(path string) FileDesc {
	return &localFile{path: path}
}

func (s *Storage) storeAsFile(ctx context.Context, fd FileDesc, conf *StoreConfig) (*schema.DirEntry, error) {
	sr, err := s.storeFileContent(ctx, fd, conf)
	if err != nil {
		return nil, err
	}
	return &schema.DirEntry{
		Ref:  sr.Ref,
		Name: filepath.Base(fd.Name()),
		Stats: Stats{
			schema.StatDataSize: sr.Size,
		},
	}, nil
}

func (s *Storage) storeFileContent(ctx context.Context, fd FileDesc, conf *StoreConfig) (types.SizedRef, error) {
	// open the file, snapshot metadata
	rc, xr, err := fd.Open()
	if err != nil {
		return types.SizedRef{}, err
	}
	defer rc.Close()

	// if we know the ref - check if we expect it, and if not - set as expected
	if !xr.Ref.Zero() {
		if err = conf.checkRef(xr); err != nil {
			return types.SizedRef{}, err
		}
		conf.Expect = xr
	}

	// not splitting the file - can optimize in few cases
	if conf.Split == nil {
		// we know the ref beforehand, so we might return earlier
		// if we are in indexing mode or we have this blob
		if !xr.Ref.Zero() {
			if conf.IndexOnly {
				// if only indexing - return the response directly
				return xr, nil
			}
			// if storing, check if a blob store has this ref already
			_, err := s.StatBlob(ctx, xr.Ref)
			if err == nil {
				return xr, nil
			}
			// if not, continue as usual
		}

		if !conf.IndexOnly {
			// if we are not indexing, storing a local file and the backend
			// is a local FS, then try to import the file directly without copying it
			if lf, ok := fd.(*localFile); ok {
				if l, ok := s.st.(*storage.LocalStorage); ok {
					// clone file, if possible
					if sr, err := l.ImportFile(ctx, lf.path); err == nil {
						// write resulting ref to source file, so we know it next time
						fd.SetRef(sr)
						return sr, nil
					}
				}
			}
		}
	}

	sr, err := s.StoreBlob(ctx, rc, conf)
	if err != nil {
		return types.SizedRef{}, err
	}
	if conf.Split == nil {
		fd.SetRef(sr)
	}
	return sr, nil
}

func (s *Storage) storeDirList(ctx context.Context, list []schema.DirEntry) (SizedRef, Stats, error) {
	stats := make(Stats)
	olist := make([]schema.Object, 0, len(list))
	for _, e := range list {
		for k, v := range e.Stats {
			stats[k] += v
		}
		stats[schema.StatDataCount]++
		e := e
		olist = append(olist, &e)
	}
	if len(stats) == 0 {
		stats = nil
	}
	m := &schema.InlineList{Elem: typeDirEnt, List: olist, Stats: stats}
	sr, err := s.StoreSchema(ctx, m)
	if err != nil {
		return SizedRef{}, nil, err
	}
	return sr, stats, nil
}

func (s *Storage) storeDirJoin(ctx context.Context, refs []Ref, list []schema.List) (SizedRef, schema.List, error) {
	stats := make(schema.Stats)
	for _, e := range list {
		for k, v := range e.Stats {
			stats[k] += v
		}
	}
	if len(stats) == 0 {
		stats = nil
	}
	m := schema.List{Elem: typeDirEnt, List: refs, Stats: stats}
	sr, err := s.StoreSchema(ctx, &m)
	if err != nil {
		return SizedRef{}, schema.List{}, err
	}
	return sr, m, nil
}

func (s *Storage) storeDir(ctx context.Context, dir string, conf *StoreConfig) (SizedRef, Stats, error) {
	d, err := os.Open(dir)
	if err != nil {
		return SizedRef{}, nil, err
	}
	defer d.Close()

	var base []schema.DirEntry
	for {
		buf, err := d.Readdir(maxDirEntries)
		if err == io.EOF {
			d.Close()
			break
		} else if err != nil {
			return SizedRef{}, nil, err
		}
		for _, fi := range buf {
			if fi.Name() == DefaultDir {
				continue
			}
			fpath := filepath.Join(dir, fi.Name())
			if fi.IsDir() {
				sr, st, err := s.storeDir(ctx, fpath, conf)
				if err != nil {
					return SizedRef{}, nil, err
				}
				base = append(base, schema.DirEntry{
					Ref: sr.Ref, Name: fi.Name(),
					Stats: st,
				})
			} else {
				ent, err := s.storeAsFile(ctx, LocalFile(fpath), conf)
				if err != nil {
					return SizedRef{}, nil, err
				}
				base = append(base, *ent)
			}
		}
	}
	sort.Slice(base, func(i, j int) bool {
		return base[i].Name < base[j].Name
	})
	var (
		level []schema.List
		refs  []Ref
		cur   schema.List
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

		sr, stats, err := s.storeDirList(ctx, page)
		if err != nil {
			return SizedRef{}, nil, err
		}
		if cur.Stats == nil {
			cur.Stats = make(schema.Stats, 2)
			for k, v := range stats {
				cur.Stats[k] += v
			}
		}
		cur.List = append(cur.List, sr.Ref)
		if len(cur.List) >= maxDirEntries || len(base) == 0 {
			cur.Elem = typeDirEnt
			sr, err = s.StoreSchema(ctx, &cur)
			if err != nil {
				return SizedRef{}, nil, err
			}
			level = append(level, cur)
			refs = append(refs, sr.Ref)
			cur = schema.List{}
		}
	}
	for len(level) > 1 {
		var (
			newLevel []schema.List
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
				return SizedRef{}, nil, err
			}
			newLevel = append(newLevel, cur)
			newRefs = append(newRefs, sr.Ref)
		}
		level, refs = newLevel, newRefs
	}
	top := level[0]
	sr, err := s.StoreSchema(ctx, &top)
	if err != nil {
		return SizedRef{}, nil, err
	}
	return sr, top.Stats, nil
}

func (s *Storage) StoreAsFile(ctx context.Context, fd FileDesc, conf *StoreConfig) (SizedRef, error) {
	m, err := s.storeAsFile(ctx, fd, conf)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreSchema(ctx, m)
}

func (s *Storage) StoreFilePath(ctx context.Context, path string, conf *StoreConfig) (SizedRef, error) {
	conf = checkConfig(conf)
	fi, err := os.Stat(path)
	if err != nil {
		return SizedRef{}, err
	}
	if fi.IsDir() {
		sr, _, err := s.storeDir(ctx, path, conf)
		return sr, err
	}
	ent, err := s.storeAsFile(ctx, LocalFile(path), conf)
	if err != nil {
		return SizedRef{}, err
	}
	return SizedRef{Ref: ent.Ref, Size: ent.Size()}, err
}

type localFile struct {
	path string
	fi   os.FileInfo
}

func (f *localFile) Name() string {
	return filepath.Base(f.path)
}

func (f *localFile) Open() (io.ReadCloser, SizedRef, error) {
	fd, err := os.Open(f.path)
	if err != nil {
		return nil, types.SizedRef{}, err
	}
	st, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, SizedRef{}, err
	}
	f.fi = st
	sr := SizedRef{Size: uint64(st.Size())}
	if xr, err := Stat(context.Background(), f.path); err == nil && xr.Size == sr.Size {
		sr.Ref = xr.Ref
	}
	return fd, sr, nil
}

func (f *localFile) SetRef(ref types.SizedRef) {
	if f.fi == nil || uint64(f.fi.Size()) != ref.Size {
		// this is the only case that we can reject directly
		// all other checks happen at read time
		return
	}
	_ = SaveRef(context.Background(), f.path, f.fi, ref.Ref)
}
