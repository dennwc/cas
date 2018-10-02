package cas

import (
	"bytes"
	"context"
	"io"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

var (
	typeDirEnt   = schema.MustTypeOf(&schema.DirEntry{})
	typeSizedRef = schema.MustTypeOf(&types.SizedRef{})
)

type SchemaIterator = storage.SchemaIterator

func (s *Storage) StoreSchema(ctx context.Context, o schema.Object) (SizedRef, error) {
	buf := new(bytes.Buffer)
	if err := schema.Encode(buf, o); err != nil {
		return SizedRef{}, err
	}
	exp := types.BytesRef(buf.Bytes())
	return s.StoreBlob(ctx, buf, &StoreConfig{
		Expect: SizedRef{Ref: exp, Size: uint64(buf.Len())},
	})
}

func (s *Storage) FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	return s.index.FetchSchema(ctx, ref)
}

func (s *Storage) DecodeSchema(ctx context.Context, ref types.Ref) (schema.Object, error) {
	rc, _, err := s.index.FetchSchema(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return schema.Decode(rc)
}

func (s *Storage) IterateSchema(ctx context.Context, typs ...string) SchemaIterator {
	return s.index.IterateSchema(ctx, typs...)
}

func (s *Storage) ReindexSchema(ctx context.Context, force bool) error {
	return s.index.ReindexSchema(ctx, force)
}
