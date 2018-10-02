package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

// NewBlobIndexer emulates a blob indexer on top of a base storage.
// It will first try to cast the storage directly, and in case of failure it will
// provide an "index" by iterating over all blobs and filtering them.
func NewBlobIndexer(s BlobStorage) BlobIndexer {
	if ind, ok := s.(BlobIndexer); ok {
		return ind
	}
	return &emulatedIndexer{s: s}
}

type emulatedIndexer struct {
	s BlobStorage
}

func (s *emulatedIndexer) FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	rc, sz, err := s.s.FetchBlob(ctx, ref)
	if err != nil {
		return nil, 0, err
	} else if sz <= uint64(schema.MagicSize) {
		rc.Close()
		return nil, 0, schema.ErrNotSchema
	}
	buf := make([]byte, schema.MagicSize)
	_, err = io.ReadFull(rc, buf)
	if err == io.ErrUnexpectedEOF {
		rc.Close()
		return nil, 0, schema.ErrNotSchema
	} else if err != nil {
		rc.Close()
		return nil, 0, err
	}
	if !schema.IsSchema(buf) {
		rc.Close()
		return nil, 0, schema.ErrNotSchema
	}
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.MultiReader(bytes.NewReader(buf), rc),
		Closer: rc,
	}, sz, nil
}

func (s *emulatedIndexer) IterateSchema(ctx context.Context, typs ...string) SchemaIterator {
	it := &emulatedSchemaIter{
		s: s, ctx: ctx, it: s.s.IterateBlobs(ctx),
	}
	if len(typs) != 0 {
		it.typs = make(map[string]struct{}, len(typs))
		for _, typ := range typs {
			it.typs[typ] = struct{}{}
		}
	}
	return it
}

func (s *emulatedIndexer) ReindexSchema(ctx context.Context, force bool) error {
	return nil // always up-to-date
}

type emulatedSchemaIter struct {
	s    *emulatedIndexer
	ctx  context.Context
	it   Iterator
	typs map[string]struct{}

	err error
	typ string
	rc  io.ReadCloser
	obj schema.Object
}

func (it *emulatedSchemaIter) Next() bool {
	for {
		if it.rc != nil {
			it.rc.Close()
			it.rc = nil
		}
		it.obj = nil
		if it.err != nil || !it.it.Next() {
			return false
		}
		sr := it.it.SizedRef()
		// that's right, we are forced to open every blob and check if it's a schema or not
		rc, _, err := it.s.FetchSchema(it.ctx, sr.Ref)
		if err == schema.ErrNotSchema {
			continue
		} else if err != nil {
			it.err = err
			return false
		}
		// we need to pull a part of the blob to know the type, as required by iterator interface
		nr, typ, err := schema.PeekType(rc)
		if err == schema.ErrNotSchema {
			rc.Close()
			continue
		} else if err != nil {
			rc.Close()
			it.err = err
			return false
		}
		if it.typs != nil {
			if _, ok := it.typs[typ]; !ok {
				rc.Close()
				continue
			}
		}
		// type matches, store the new reader
		it.typ = typ
		it.rc = struct {
			io.Reader
			io.Closer
		}{
			Reader: nr,
			Closer: rc,
		}
		return true
	}
}

func (it *emulatedSchemaIter) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Err()
}

func (it *emulatedSchemaIter) Close() error {
	if it.rc != nil {
		it.rc.Close()
		it.rc = nil
	}
	return it.it.Close()
}

func (it *emulatedSchemaIter) SizedRef() types.SizedRef {
	return it.it.SizedRef()
}

func (it *emulatedSchemaIter) SchemaRef() types.SchemaRef {
	sr := it.SizedRef()
	return types.SchemaRef{
		Ref: sr.Ref, Size: sr.Size,
		Type: it.typ,
	}
}

func (it *emulatedSchemaIter) Decode() (schema.Object, error) {
	if it.err != nil {
		return nil, it.err
	} else if it.obj != nil {
		return it.obj, nil
	} else if it.rc == nil {
		return nil, schema.ErrNotSchema
	}
	// need to decode an object from the reader
	defer func() {
		it.rc.Close()
		it.rc = nil
	}()
	it.obj, it.err = schema.Decode(it.rc)
	return it.obj, it.err
}
