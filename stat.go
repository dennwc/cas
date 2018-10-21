package cas

import (
	"context"
	"os"

	"github.com/dennwc/cas/storage/local"
	"github.com/dennwc/cas/types"
)

// Stat returns the size of the file and the ref if it's written into the metadata and considered valid.
func Stat(ctx context.Context, path string) (SizedRef, error) {
	return local.Stat(ctx, path)
}

// StatFile returns the size of the file and the ref if it's written into the metadata and considered valid.
func StatFile(ctx context.Context, f *os.File) (SizedRef, error) {
	return local.StatFile(ctx, f)
}

// SaveRef stores the ref into file's metadata.
// Additionally, it will write the size and mtime to know if ref is still valid.
func SaveRef(ctx context.Context, path string, fi os.FileInfo, ref types.Ref) error {
	return local.SaveRef(ctx, path, fi, ref)
}

// SaveRefFile stores the ref into file's metadata.
// Additionally, it will write the size and mtime to know if ref is still valid.
func SaveRefFile(ctx context.Context, f *os.File, fi os.FileInfo, ref types.Ref) error {
	return local.SaveRefFile(ctx, f, fi, ref)
}
