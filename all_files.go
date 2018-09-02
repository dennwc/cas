package cas

import (
	"context"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

// IterateDataBlobsIn iterates all non-schema blobs in the provided schema blob.
func (s *Storage) IterateDataBlobsIn(ctx context.Context, root Ref) storage.Iterator {
	return &filesInIterator{s: s, ctx: ctx, refs: []Ref{root}}
}

type filesInIterator struct {
	s    *Storage
	ctx  context.Context
	refs []Ref

	cur SizedRef
	err error
}

func (it *filesInIterator) Next() bool {
	for {
		if it.err != nil || len(it.refs) == 0 {
			return false
		}
		ref := it.refs[0]
		it.refs = it.refs[1:]

		obj, err := it.s.DecodeSchema(it.ctx, ref)
		if err == schema.ErrNotSchema {
			sz, err := it.s.StatBlob(it.ctx, ref)
			if err == storage.ErrNotFound {
				continue
			} else if err != nil {
				it.err = err
				return false
			}
			it.cur = SizedRef{Ref: ref, Size: sz}
			return true
		} else if err != nil {
			it.err = err
			return false
		}
		switch obj := obj.(type) {
		case *schema.DirEntry:
			it.refs = append(it.refs, obj.Ref)
		case *schema.Directory:
			for _, ent := range obj.List {
				it.refs = append(it.refs, ent.Ref)
			}
		case *schema.JoinDirectories:
			for _, ent := range obj.List {
				it.refs = append(it.refs, ent)
			}
		}
		// continue loop
	}
}

func (it *filesInIterator) Err() error {
	return it.err
}

func (it *filesInIterator) Close() error {
	it.refs = nil
	return nil
}

func (it *filesInIterator) Ref() types.Ref {
	return it.cur.Ref
}

func (it *filesInIterator) Size() uint64 {
	return it.cur.Size
}
