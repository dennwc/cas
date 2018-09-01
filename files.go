package cas

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
)

const (
	maxDirEntries = 1024
)

func (s *Storage) storeAsFile(ctx context.Context, r io.Reader, fi os.FileInfo, index bool) (*schema.DirEntry, error) {
	var fw storage.BlobWriter
	if index {
		fw = storage.Hash()
	} else {
		var err error
		fw, err = s.BeginBlob(ctx)
		if err != nil {
			return nil, err
		}
	}
	defer fw.Close()

	name := filepath.Base(fi.Name())

	iw := s.indexFileByExt(ctx, name)
	if iw != nil {
		defer iw.Close()
	}

	writers := []io.Writer{fw}
	if iw != nil {
		writers = append(writers, iw)
	}
	var w io.Writer
	if len(writers) == 1 {
		w = writers[0]
	} else {
		w = io.MultiWriter(writers...)
	}

	n, err := io.Copy(w, r)
	if err != nil {
		return nil, err
	} else if uint64(n) != uint64(fi.Size()) {
		return nil, fmt.Errorf("file changed while writing it")
	}
	sr, err := fw.Complete()
	if err != nil {
		return nil, err
	} else if sr.Size != uint64(fi.Size()) {
		return nil, fmt.Errorf("file changed while writing it")
	}
	err = fw.Commit()
	if err != nil {
		return nil, err
	}
	m := &schema.DirEntry{
		Ref: sr.Ref, Size: sr.Size,
		Name: name,
	}
	if iw != nil {
		// best effort, ignore indexing errors
		if isr, err := iw.Complete(); err == nil {
			err = iw.Commit()
			_ = err
			_, _ = s.storeIndexByExt(ctx, name, sr, isr)
		}
	}
	return m, nil
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
func (s *Storage) storeDir(ctx context.Context, dir *os.File, index bool) (SizedRef, schema.DirEntry, error) {
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
				sr, st, err := s.storeDir(ctx, f, index)
				f.Close()
				if err != nil {
					return SizedRef{}, schema.DirEntry{}, err
				}
				st.Ref = sr.Ref
				st.Name = fi.Name()
				base = append(base, st)
			} else {
				ent, err := s.storeAsFile(ctx, f, fi, index)
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

func (s *Storage) storeFile(ctx context.Context, f *os.File, index bool) (SizedRef, error) {
	fi, err := f.Stat()
	if err != nil {
		return SizedRef{}, err
	}
	if fi.IsDir() {
		sr, _, err := s.storeDir(ctx, f, index)
		return sr, err
	}
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return SizedRef{}, err
	}
	ent, err := s.storeAsFile(ctx, f, fi, index)
	return SizedRef{Ref: ent.Ref, Size: ent.Size}, err
}

func (s *Storage) IndexAsFile(ctx context.Context, r io.Reader, fi os.FileInfo) (SizedRef, error) {
	m, err := s.storeAsFile(ctx, r, fi, true)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreSchema(ctx, m)
}

func (s *Storage) StoreAsFile(ctx context.Context, r io.Reader, fi os.FileInfo) (SizedRef, error) {
	m, err := s.storeAsFile(ctx, r, fi, false)
	if err != nil {
		return SizedRef{}, err
	}
	return s.StoreSchema(ctx, m)
}

func (s *Storage) storeFilePath(ctx context.Context, path string, index bool) (SizedRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return SizedRef{}, nil
	}
	defer f.Close()
	return s.storeFile(ctx, f, index)
}

func (s *Storage) IndexFilePath(ctx context.Context, path string) (SizedRef, error) {
	return s.storeFilePath(ctx, path, true)
}

func (s *Storage) StoreFilePath(ctx context.Context, path string) (SizedRef, error) {
	return s.storeFilePath(ctx, path, false)
}

func (s *Storage) StoreFile(ctx context.Context, f *os.File) (SizedRef, error) {
	return s.storeFile(ctx, f, false)
}

func (s *Storage) IndexFile(ctx context.Context, f *os.File) (SizedRef, error) {
	return s.storeFile(ctx, f, true)
}
