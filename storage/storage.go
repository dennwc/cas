package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dennwc/cas/schema"

	"github.com/dennwc/cas/types"
)

var (
	ErrNotFound      = errors.New("blob: not found")
	ErrBlobDiscarded = errors.New("blob was discarded")
	ErrBlobCompleted = errors.New("blob was completed")
)

type ErrRefMissmatch struct {
	Exp, Got types.Ref
}

func (e ErrRefMissmatch) Error() string {
	return fmt.Sprintf("hash missmatch: exp: %v, got: %v", e.Exp, e.Got)
}

type BlobStorage interface {
	StatBlob(ctx context.Context, ref types.Ref) (uint64, error)
	FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error)
	BeginBlob(ctx context.Context) (BlobWriter, error)
	IterateBlobs(ctx context.Context) Iterator
}

type BlobIndexer interface {
	FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error)
	IterateSchema(ctx context.Context, typs ...string) SchemaIterator
	Reindex(ctx context.Context, force bool) error
}

type SchemaIterator interface {
	Iterator
	SchemaRef() types.SchemaRef
	Decode() (schema.Object, error)
}

type BlobWriter interface {
	io.Writer
	// Complete returns the hash and size of the written blob.
	// User should either Close the writer to discard the blob or Commit to store the blob.
	// All writes after this call will fail.
	Complete() (types.SizedRef, error)
	// Close will discard the blob.
	Close() error
	// Commit stores the blob and closes it automatically.
	// It will call Complete if it was not called before.
	Commit() error
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
	BlobIndexer
}

type BaseIterator interface {
	Next() bool
	Err() error
	Close() error
}

type Iterator interface {
	BaseIterator
	SizedRef() types.SizedRef
}

type PinIterator interface {
	BaseIterator
	Pin() types.Pin
}
