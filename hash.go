package cas

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/dennwc/cas/types"
)

func HashWith(ctx context.Context, path string, info os.FileInfo, force bool) (SizedRef, error) {
	if !force {
		if sr, err := Stat(ctx, path); err == nil && !sr.Ref.Zero() {
			return sr, nil
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return SizedRef{}, err
	}
	defer f.Close()

	h := types.NewRef().Hash()
	n, err := io.Copy(h, f)
	if err != nil {
		return SizedRef{}, err
	}
	ref := types.NewRef().WithHash(h)
	if err = SaveRef(ctx, path, info, ref); err != nil {
		log.Println(err)
	}
	return SizedRef{Ref: ref, Size: uint64(n)}, nil
}

func Hash(ctx context.Context, path string) (SizedRef, error) {
	st, err := os.Stat(path)
	if err != nil {
		return SizedRef{}, err
	}
	return HashWith(ctx, path, st, false)
}
