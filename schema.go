package cas

import (
	"context"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

type SchemaIterator interface {
	storage.Iterator
	Type() string
}

func (s *Storage) IterateSchema(ctx context.Context, typs ...string) SchemaIterator {
	it := s.st.IterateBlobs(ctx)
	var filter map[string]struct{}
	if len(typs) != 0 {
		filter = make(map[string]struct{})
		for _, v := range typs {
			filter[v] = struct{}{}
		}
	}
	return &schemaIterator{s: s, ctx: ctx, typs: filter, it: it}
}

type schemaIterator struct {
	s    *Storage
	ctx  context.Context
	it   storage.Iterator
	typs map[string]struct{}
	typ  string
	err  error
}

func (it *schemaIterator) Next() bool {
	for it.err == nil && it.it.Next() {
		if it.it.Size() < uint64(schema.MagicSize) {
			continue
		}
		rc, _, err := it.s.FetchBlob(it.ctx, it.it.Ref())
		if err == storage.ErrNotFound {
			continue
		} else if err != nil {
			it.err = err
			return false
		}
		typ, err := schema.DecodeType(rc)
		rc.Close()
		if err == schema.ErrNotSchema {
			continue
		} else if err != nil {
			it.err = err
			return false
		}
		if it.typs != nil {
			if _, ok := it.typs[typ]; !ok {
				continue
			}
		}
		it.typ = typ
		return true
	}
	return false
}

func (it *schemaIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Err()
}

func (it *schemaIterator) Close() error {
	return it.it.Close()
}

func (it *schemaIterator) Ref() types.Ref {
	return it.it.Ref()
}

func (it *schemaIterator) Size() uint64 {
	return it.it.Size()
}

func (it *schemaIterator) Type() string {
	return it.typ
}
