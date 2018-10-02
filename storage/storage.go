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
	// ErrNotFound is returned when a blob or a pin does not exists.
	ErrNotFound = errors.New("blob: not found")
	// ErrInvalidRef is returned when a passed Ref was invalid (zero, for example).
	ErrInvalidRef = errors.New("blob: invalid ref")
	// ErrReadOnly is returned when trying to perform a mutation ona read-only storage.
	ErrReadOnly = errors.New("blob: storage is read-only")
	// ErrBlobDiscarded is returned for BlobWriter operations after the blob was discarded with Close.
	ErrBlobDiscarded = errors.New("blob was discarded")
	// ErrBlobCompleted is returned for BlobWriter operations after the blob was completed.
	ErrBlobCompleted = errors.New("blob was completed")
)

// ErrRefMissmatch is returned when the streamed content doesn't match an expected blob ref.
type ErrRefMissmatch struct {
	Exp, Got types.Ref
}

func (e ErrRefMissmatch) Error() string {
	return fmt.Sprintf("hash missmatch: exp: %v, got: %v", e.Exp, e.Got)
}

// ErrSizeMissmatch is returned when the size of a streamed content doesn't match an expected size.
type ErrSizeMissmatch struct {
	Exp, Got uint64
}

func (e ErrSizeMissmatch) Error() string {
	return fmt.Sprintf("size missmatch: exp: %v, got: %v", e.Exp, e.Got)
}

// BlobSource is a read-only interface for a blob storage.
type BlobSource interface {
	// StatBlob checks if a blob is in the storage and returns its size.
	// It returns ErrNotFound if this blob does not exist.
	// Calling it with a zero Ref will result in ErrInvalidRef.
	StatBlob(ctx context.Context, ref types.Ref) (uint64, error)
	// FetchBlob opens a blob for reading and returns its size.
	// It returns ErrNotFound if this blob does not exist.
	// Calling it with a zero Ref will result in ErrInvalidRef.
	// Caller should close a reader to free resources.
	FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error)
	// IterateBlobs creates an iterator that lists all blobs in the storage.
	// Caller should close an iterator to free resources.
	//
	// TODO: clarify sorting
	IterateBlobs(ctx context.Context) Iterator
}

// BlobStorage is a minimal interface for storing and retrieving blobs.
type BlobStorage interface {
	BlobSource
	// BeginBlob starts writing a blob to the storage.
	// See BlobWriter for more details.
	BeginBlob(ctx context.Context) (BlobWriter, error)
}

// BlobIndexer is an optional interface for Storage implementations that index schema blobs by type.
type BlobIndexer interface {
	// FetchSchema fetches a schema blob from storage.
	// It returns a schema.ErrNotSchema if a blob contains raw data or is not a CAS schema blob.
	FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error)
	// IterateSchema lists all schema blobs, optionally only with specific types.
	IterateSchema(ctx context.Context, typs ...string) SchemaIterator
	// ReindexSchema rebuilds an index of schema blobs.
	// If force parameter is false, an index is only updated for new blobs that were not indexed.
	// If force is true, an index is rebuilt for all blobs.
	ReindexSchema(ctx context.Context, force bool) error
}

// SchemaIterator iterates over CAS schema blobs.
type SchemaIterator interface {
	Iterator
	// SchemaRef returns the size, ref and the type of current schema blob.
	SchemaRef() types.SchemaRef
	// Decode reads and decodes current schema object. Implementations might optimize this call
	// by serving an object from a different data store.
	Decode() (schema.Object, error)
}

// BlobWriter is an interface that allows to write immutable blobs to the storage.
//
// Blobs are written in a three step process. First the blob is started and the data is written
// using Write, as usual. Next, the blob is finalized by calling Complete. This will return
// a final ref of the blob and its size. All writes after Complete will result in an error.
// At the end, the blob should be either stored with Commit or discarded with Close. Blob would
// not appear in the storage until a successful Commit. It's also possible to discard blob earlier
// by calling Close.
type BlobWriter interface {
	io.Writer
	// Size reports how many bytes were already written.
	Size() uint64
	// Complete returns the hash and size of the written blob.
	// User should either Close the writer to discard the blob or Commit to store the blob.
	// All writes after this call will fail.
	Complete() (types.SizedRef, error)
	// Close will discard the blob. Calling Close after Commit will still preserve the blob,
	// thus it's safe to use it in defer.
	Close() error
	// Commit stores the blob and closes it automatically.
	Commit() error
}

// PinStorage is a minimal interface for implementing a mutable storage over immutable storage.
type PinStorage interface {
	// SetPin overwrites or creates a named pin with a specified blob ref.
	SetPin(ctx context.Context, name string, ref types.Ref) error
	// DeletePin removes a named pin.
	DeletePin(ctx context.Context, name string) error
	// GetPin returns a ref associated with a named pin.
	// It returns ErrNotFound if a named pin does not exist.
	GetPin(ctx context.Context, name string) (types.Ref, error)
	// IteratePins lists all pins in the storage.
	IteratePins(ctx context.Context) PinIterator
}

// Storage is a minimal interface for a Content Addressable Storage.
type Storage interface {
	BlobStorage
	PinStorage
}

// BaseIterator is a common interface for all iterators.
//
// Example:
//		defer it.Close()
//		for it.Next() {
//			// use current item
//		}
//		err := it.Err()
type BaseIterator interface {
	// Next advances an iterator.
	Next() bool
	// Err returns last error encountered by iterator.
	Err() error
	// Close frees resources associated with this iterator.
	Close() error
}

// Iterator iterates over raw blobs and list their sizes and refs.
type Iterator interface {
	BaseIterator
	// SizedRef returns the size and ref of current blob.
	SizedRef() types.SizedRef
}

// PinIterator iterates over all pins and lists their names and associated refs.
type PinIterator interface {
	BaseIterator
	// Pin returns current pin.
	Pin() types.Pin
}
