package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dennwc/cas/types"
)

var ErrNotFound = errors.New("blob: not found")

type ErrRefMissmatch struct {
	Exp, Got types.Ref
}

func (e ErrRefMissmatch) Error() string {
	return fmt.Sprintf("hash missmatch: exp: %v, got: %v", e.Exp, e.Got)
}

type Storage interface {
	StatBlob(ctx context.Context, ref types.Ref) (uint64, error)
	FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error)
	StoreBlob(ctx context.Context, exp types.Ref, r io.Reader) (types.SizedRef, error)
	IterateBlobs(ctx context.Context) Iterator
}

type Iterator interface {
	Next() bool
	Err() error
	Ref() types.Ref
	Size() uint64
	Close() error
}
