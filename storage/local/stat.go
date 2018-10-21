package local

import (
	"context"
	"os"

	"github.com/dennwc/cas/types"
	"github.com/dennwc/cas/xattr"
)

// Stat returns the size of the file and the ref if it's written into the metadata and considered valid.
func Stat(ctx context.Context, path string) (types.SizedRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return types.SizedRef{}, err
	}
	defer f.Close()
	return StatFile(ctx, f)
}

// StatFile returns the size of the file and the ref if it's written into the metadata and considered valid.
func StatFile(ctx context.Context, f *os.File) (types.SizedRef, error) {
	st, err := f.Stat()
	if err != nil {
		return types.SizedRef{}, err
	}
	sr := types.SizedRef{Size: uint64(st.Size())}
	sref, err := xattr.GetStringF(f, xattrNS+"hash")
	if err != nil || len(sref) == 0 {
		// fallback to size only
		return sr, nil
	}
	ref, err := types.ParseRef(sref)
	if err != nil {
		return sr, nil
	}
	// to verify that this hash is correct, read an old size and mtime
	size, err := xattr.GetUintF(f, xattrNS+"size")
	if err != nil || size != uint64(st.Size()) {
		// TODO: remove xattr?
		return sr, nil
	}
	mtime, err := xattr.GetTimeF(f, xattrNS+"mtime")
	if err != nil || !mtime.Equal(st.ModTime()) {
		// TODO: remove xattr?
		return sr, nil
	}
	sr.Ref = ref
	return sr, nil
}

// SaveRef stores the ref into file's metadata.
// Additionally, it will write the size and mtime to know if ref is still valid.
func SaveRef(ctx context.Context, path string, fi os.FileInfo, ref types.Ref) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return SaveRefFile(ctx, f, fi, ref)
}

// SaveRefFile stores the ref into file's metadata.
// Additionally, it will write the size and mtime to know if ref is still valid.
func SaveRefFile(ctx context.Context, f *os.File, fi os.FileInfo, ref types.Ref) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if fi != nil {
		if st.Size() != fi.Size() || !st.ModTime().Equal(fi.ModTime()) {
			// file was already modified
			return nil
		}
	} else {
		fi = st
	}
	err = xattr.SetUintF(f, xattrNS+"size", uint64(fi.Size()))
	if err != nil {
		return err
	}
	mtime := fi.ModTime()
	err = xattr.SetTimeF(f, xattrNS+"mtime", mtime)
	if err != nil {
		return err
	}
	err = xattr.SetStringF(f, xattrNS+"hash", ref.String())
	if err != nil {
		return err
	}
	return fchtimes(f, mtime, mtime)
}
