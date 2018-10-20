package storage

import (
	"hash"
	"io"

	"github.com/dennwc/cas/types"
)

// VerifyReader wraps a reader and calculates a ref of the data on EOF.
// It returns an error from Read if ref doesn't match the expected one.
func VerifyReader(rc io.ReadCloser, ref types.Ref) io.ReadCloser {
	return &verifyReader{
		rc: rc, exp: ref, h: ref.Hash(),
	}
}

type verifyReader struct {
	rc  io.ReadCloser
	exp types.Ref
	h   hash.Hash
}

func (r *verifyReader) verify() error {
	got := r.exp.WithHash(r.h)
	if got != r.exp {
		return ErrRefMissmatch{Exp: r.exp, Got: got}
	}
	return nil
}
func (r *verifyReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n != 0 {
		r.h.Write(p[:n])
	}
	if err == io.EOF {
		if err2 := r.verify(); err2 != nil {
			return n, err2
		}
	}
	return n, err
}
func (r *verifyReader) Close() error {
	return r.rc.Close()
}
