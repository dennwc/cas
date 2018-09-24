package storage

import (
	"hash"

	"github.com/dennwc/cas/types"
)

func Hash() BlobWriter {
	return &hashWriter{h: types.NewRef().Hash()}
}

type hashWriter struct {
	h    hash.Hash
	size uint64
	ref  types.SizedRef
}

func (w *hashWriter) Size() uint64 {
	return w.size
}

func (w *hashWriter) Write(p []byte) (int, error) {
	if w.h == nil {
		return 0, ErrBlobCompleted
	}
	n, err := w.h.Write(p)
	w.size += uint64(n)
	return n, err
}

func (w *hashWriter) Complete() (types.SizedRef, error) {
	if w.h != nil {
		w.ref.Ref = types.NewRef().WithHash(w.h)
		w.ref.Size = w.size
		w.h = nil
		return w.ref, nil
	}
	if w.ref.Ref.Zero() {
		return types.SizedRef{}, ErrBlobDiscarded
	}
	return w.ref, nil
}

func (w *hashWriter) Close() error {
	w.h = nil
	return nil
}

func (w *hashWriter) Commit() error {
	if w.h != nil {
		if _, err := w.Complete(); err != nil {
			return err
		}
	}
	if w.ref.Ref.Zero() {
		return ErrBlobDiscarded
	}
	return nil
}
