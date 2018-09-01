package cas

import (
	"context"
	"os"

	"github.com/dennwc/cas/types"
	"github.com/dennwc/cas/xattr"
)

const xattrNS = "cas."

// Stat returns the size of the file and the ref if it's written into the metadata and considered valid.
func Stat(ctx context.Context, path string) (SizedRef, error) {
	st, err := os.Stat(path)
	if err != nil {
		return SizedRef{}, err
	}
	sr := SizedRef{Size: uint64(st.Size())}
	sref, err := xattr.GetString(path, xattrNS+"hash")
	if err != nil || len(sref) == 0 {
		// fallback to size only
		return sr, nil
	}
	ref, err := types.ParseRef(sref)
	if err != nil {
		return sr, nil
	}
	// to verify that this hash is correct, read an old size and mtime
	size, err := xattr.GetUint(path, xattrNS+"size")
	if err != nil || size != uint64(st.Size()) {
		// TODO: remove xattr?
		return sr, nil
	}
	mtime, err := xattr.GetTime(path, xattrNS+"mtime")
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
	if st, err := os.Stat(path); err != nil {
		return err
	} else if st.Size() != fi.Size() || !st.ModTime().Equal(fi.ModTime()) {
		// file was already modified
		return nil
	}
	err := xattr.SetUint(path, xattrNS+"size", uint64(fi.Size()))
	if err != nil {
		return err
	}
	mtime := fi.ModTime()
	err = xattr.SetTime(path, xattrNS+"mtime", mtime)
	if err != nil {
		return err
	}
	err = xattr.SetString(path, xattrNS+"hash", ref.String())
	if err != nil {
		return err
	}
	return os.Chtimes(path, mtime, mtime)
}
