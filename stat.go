package cas

import (
	"context"
	"encoding/binary"
	"os"
	"time"

	"github.com/dennwc/cas/types"
	"github.com/pkg/xattr"
)

const xattrNS = "user.cas."

// Stat returns the size of the file and the ref if it's written into the metadata and considered valid.
func Stat(ctx context.Context, path string) (SizedRef, error) {
	st, err := os.Stat(path)
	if err != nil {
		return SizedRef{}, err
	}
	sr := SizedRef{Size: uint64(st.Size())}
	data, err := xattr.Get(path, xattrNS+"hash")
	if err != nil || len(data) == 0 {
		// fallback to size only
		return sr, nil
	}
	ref, err := types.ParseRef(string(data))
	if err != nil {
		return sr, nil
	}
	// to verify that this hash is correct, read an old size and mtime
	data, err = xattr.Get(path, xattrNS+"size")
	if err != nil || len(data) != 8 {
		return sr, nil
	}
	size := binary.LittleEndian.Uint64(data)
	if size != uint64(st.Size()) {
		// TODO: remove xattr?
		return sr, nil
	}
	data, err = xattr.Get(path, xattrNS+"mtime")
	if err != nil || len(data) != 8 {
		return sr, nil
	}
	nanos := binary.LittleEndian.Uint64(data)
	mtime := time.Unix(0, int64(nanos)).UTC()
	if !mtime.Equal(st.ModTime()) {
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
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(fi.Size()))
	err := xattr.Set(path, xattrNS+"size", buf[:])
	if err != nil {
		return err
	}
	mtime := fi.ModTime()
	nanos := mtime.UnixNano()
	binary.LittleEndian.PutUint64(buf[:], uint64(nanos))
	err = xattr.Set(path, xattrNS+"mtime", buf[:])
	if err != nil {
		return err
	}
	err = xattr.Set(path, xattrNS+"hash", []byte(ref.String()))
	if err != nil {
		return err
	}
	return os.Chtimes(path, mtime, mtime)
}
