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

type BlobStorage interface {
	StatBlob(ctx context.Context, ref types.Ref) (uint64, error)
	FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error)
	StoreBlob(ctx context.Context, exp types.Ref, r io.Reader) (types.SizedRef, error)
	IterateBlobs(ctx context.Context) Iterator
}

type PinStorage interface {
	SetPin(ctx context.Context, name string, ref types.Ref) error
	DeletePin(ctx context.Context, name string) error
	GetPin(ctx context.Context, name string) (types.Ref, error)
	IteratePins(ctx context.Context) PinIterator
}

type Storage interface {
	BlobStorage
	PinStorage
}

type BaseIterator interface {
	Next() bool
	Err() error
	Close() error
}

type Iterator interface {
	BaseIterator
	Ref() types.Ref
	Size() uint64
}

type PinIterator interface {
	BaseIterator
	Ref() types.Ref
	Name() string
}
