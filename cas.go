package cas

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

const (
	DefaultDir = ".cas"
	DefaultPin = "root"
)

type OpenOptions struct {
	Dir     string
	Create  bool
	Storage storage.Storage
}

func Open(opt OpenOptions) (*Storage, error) {
	s := opt.Storage
	if s == nil {
		var err error
		s, err = storage.NewLocal(opt.Dir, opt.Create)
		if err != nil {
			return nil, err
		}
	}
	return New(s)
}

func New(st storage.Storage) (*Storage, error) {
	return &Storage{st: st}, nil
}

var _ storage.Storage = (*Storage)(nil)

type Storage struct {
	st storage.Storage
}

func (s *Storage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	if name == "" {
		name = DefaultPin
	}
	return s.st.SetPin(ctx, name, ref)
}

func (s *Storage) DeletePin(ctx context.Context, name string) error {
	if name == "" {
		name = DefaultPin
	}
	return s.st.DeletePin(ctx, name)
}

func (s *Storage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	if name == "" {
		name = DefaultPin
	}
	return s.st.GetPin(ctx, name)
}

func (s *Storage) GetPinOrRef(ctx context.Context, name string) (types.Ref, error) {
	if !types.IsRef(name) {
		return s.GetPin(ctx, name)
	}
	return types.ParseRef(name)
}

func (s *Storage) IteratePins(ctx context.Context) storage.PinIterator {
	return s.st.IteratePins(ctx)
}

func (s *Storage) FetchBlob(ctx context.Context, ref Ref) (io.ReadCloser, uint64, error) {
	if ref.Empty() {
		// generate empty blobs
		return ioutil.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	return s.st.FetchBlob(ctx, ref)
}

func (s *Storage) IterateBlobs(ctx context.Context) storage.Iterator {
	return s.st.IterateBlobs(ctx)
}

func (s *Storage) StatBlob(ctx context.Context, ref Ref) (uint64, error) {
	if ref.Empty() {
		return 0, nil
	}
	return s.st.StatBlob(ctx, ref)
}

func (s *Storage) BeginBlob(ctx context.Context) (storage.BlobWriter, error) {
	return s.st.BeginBlob(ctx)
}

func (s *Storage) completeBlob(ctx context.Context, w storage.BlobWriter, exp Ref) (SizedRef, error) {
	defer w.Close()
	sr, err := w.Complete()
	if err != nil {
		return SizedRef{}, err
	}
	if !exp.Zero() && exp != sr.Ref {
		return SizedRef{}, storage.ErrRefMissmatch{Exp: exp, Got: sr.Ref}
	}
	if sr.Ref.Empty() {
		// do not store empty blobs - we can generate them
		w.Close()
		return SizedRef{Ref: exp, Size: 0}, nil
	}
	err = w.Commit()
	if err != nil {
		return SizedRef{}, err
	}
	return sr, nil
}

func (s *Storage) StoreBlob(ctx context.Context, exp Ref, r io.Reader) (SizedRef, error) {
	if !exp.Zero() {
		if sz, err := s.StatBlob(ctx, exp); err == nil {
			// TODO: hash the reader to make sure that caller provided the right file?
			return SizedRef{Ref: exp, Size: sz}, nil
		}
	}
	w, err := s.st.BeginBlob(ctx)
	if err != nil {
		return SizedRef{}, err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	if err != nil {
		return SizedRef{}, err
	}
	return s.completeBlob(ctx, w, exp)
}

func (s *Storage) StoreSchema(ctx context.Context, o schema.Object) (SizedRef, error) {
	buf := new(bytes.Buffer)
	if err := schema.Encode(buf, o); err != nil {
		return SizedRef{}, err
	}
	exp := types.BytesRef(buf.Bytes())
	return s.StoreBlob(ctx, exp, buf)
}

func (s *Storage) FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	return s.st.FetchSchema(ctx, ref)
}

func (s *Storage) DecodeSchema(ctx context.Context, ref types.Ref) (schema.Object, error) {
	rc, _, err := s.st.FetchSchema(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return schema.Decode(rc)
}
