package cas

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/storage/local"
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
		if opt.Dir == "" {
			opt.Dir = DefaultDir
		}
		var err error
		s, err = local.New(opt.Dir, opt.Create)
		if err != nil {
			return nil, err
		}
	}
	return New(s)
}

func New(st storage.Storage) (*Storage, error) {
	return &Storage{
		st:    st,
		index: storage.NewBlobIndexer(st),
	}, nil
}

var (
	_ storage.Storage     = (*Storage)(nil)
	_ storage.BlobIndexer = (*Storage)(nil)
)

type Storage struct {
	st    storage.Storage
	index storage.BlobIndexer
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
