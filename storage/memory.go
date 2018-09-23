package storage

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sort"
	"sync"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/types"
)

func NewInMemory() Storage {
	return &memStorage{
		blobs: make(map[types.Ref][]byte),
		pins:  make(map[string]types.Ref),
		types: make(map[types.Ref]string),
	}
}

type memStorage struct {
	mu    sync.RWMutex
	blobs map[types.Ref][]byte
	pins  map[string]types.Ref
	types map[types.Ref]string
}

func (s *memStorage) StatBlob(ctx context.Context, ref types.Ref) (uint64, error) {
	s.mu.RLock()
	b, ok := s.blobs[ref]
	sz := len(b)
	b = nil
	s.mu.RUnlock()
	if !ok {
		return 0, ErrNotFound
	}
	return uint64(sz), nil
}

func (s *memStorage) FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	s.mu.RLock()
	b, ok := s.blobs[ref]
	s.mu.RUnlock()
	if !ok {
		return nil, 0, ErrNotFound
	}
	return ioutil.NopCloser(bytes.NewReader(b)), uint64(len(b)), nil
}

func (s *memStorage) BeginBlob(ctx context.Context) (BlobWriter, error) {
	return &memWriter{s: s, hw: Hash()}, nil
}

type memWriter struct {
	s   *memStorage
	buf bytes.Buffer
	hw  BlobWriter
	sr  types.SizedRef
}

func (w *memWriter) Write(p []byte) (int, error) {
	if n, err := w.hw.Write(p); err != nil {
		return n, err
	}
	return w.buf.Write(p)
}

func (w *memWriter) Complete() (types.SizedRef, error) {
	sr, err := w.hw.Complete()
	if err != nil {
		return types.SizedRef{}, err
	}
	w.sr = sr
	return sr, nil
}

func (w *memWriter) Close() error {
	return w.hw.Close()
}

func (w *memWriter) Commit() error {
	err := w.hw.Commit()
	if err != nil {
		return err
	}
	buf := w.buf.Bytes()
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	w.s.blobs[w.sr.Ref] = buf
	if !schema.IsSchema(buf) {
		return nil
	}
	typ, err := schema.DecodeType(bytes.NewReader(buf))
	if err == nil {
		w.s.types[w.sr.Ref] = typ
	}
	return nil
}

func (s *memStorage) IterateBlobs(ctx context.Context) Iterator {
	return &memIter{s: s}
}

type memIter struct {
	s    *memStorage
	refs []types.SizedRef
	i    int
}

func (it *memIter) Next() bool {
	if it.s == nil {
		return false
	}
	if it.refs == nil {
		it.s.mu.RLock()
		it.refs = make([]types.SizedRef, 0, len(it.s.blobs))
		for ref, b := range it.s.blobs {
			it.refs = append(it.refs, types.SizedRef{Ref: ref, Size: uint64(len(b))})
		}
		it.s.mu.RUnlock()
		sort.Slice(it.refs, func(i, j int) bool {
			return it.refs[i].Ref.String() < it.refs[j].Ref.String()
		})
	} else if it.i+1 <= len(it.refs) {
		it.i++
	}
	return it.i < len(it.refs)
}

func (it *memIter) Err() error {
	return nil
}

func (it *memIter) Close() error {
	it.s = nil
	return nil
}

func (it *memIter) SizedRef() types.SizedRef {
	if it.i >= len(it.refs) {
		return types.SizedRef{}
	}
	return it.refs[it.i]
}

func (s *memStorage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	s.mu.Lock()
	s.pins[name] = ref
	s.mu.Unlock()
	return nil
}

func (s *memStorage) DeletePin(ctx context.Context, name string) error {
	s.mu.Lock()
	delete(s.pins, name)
	s.mu.Unlock()
	return nil
}

func (s *memStorage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	s.mu.RLock()
	ref, ok := s.pins[name]
	s.mu.RUnlock()
	if !ok {
		return types.Ref{}, ErrNotFound
	}
	return ref, nil
}

func (s *memStorage) IteratePins(ctx context.Context) PinIterator {
	return &memPinsIter{s: s}
}

type memPinsIter struct {
	s    *memStorage
	pins []types.Pin
	i    int
}

func (it *memPinsIter) Next() bool {
	if it.s == nil {
		return false
	}
	if it.pins == nil {
		it.s.mu.RLock()
		it.pins = make([]types.Pin, 0, len(it.s.pins))
		for name, ref := range it.s.pins {
			it.pins = append(it.pins, types.Pin{Name: name, Ref: ref})
		}
		it.s.mu.RUnlock()
	} else if it.i+1 <= len(it.pins) {
		it.i++
	}
	return it.i < len(it.pins)
}

func (it *memPinsIter) Err() error {
	return nil
}

func (it *memPinsIter) Close() error {
	it.s = nil
	return nil
}

func (it *memPinsIter) Pin() types.Pin {
	if it.i >= len(it.pins) {
		return types.Pin{}
	}
	return it.pins[it.i]
}

func (s *memStorage) FetchSchema(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	s.mu.RLock()
	typ := s.types[ref]
	s.mu.RUnlock()
	if typ == "" {
		return nil, 0, schema.ErrNotSchema
	}
	return s.FetchBlob(ctx, ref)
}

func (s *memStorage) IterateSchema(ctx context.Context, typs ...string) SchemaIterator {
	it := &memSchemaIter{s: s, ctx: ctx}
	if len(typs) != 0 {
		it.filter = make(map[string]struct{}, len(typs))
		for _, typ := range typs {
			it.filter[typ] = struct{}{}
		}
	}
	return it
}

func (s *memStorage) Reindex(ctx context.Context, force bool) error {
	return nil
}

type memSchemaIter struct {
	s      *memStorage
	ctx    context.Context
	filter map[string]struct{}

	refs []types.SchemaRef
	i    int
}

func (it *memSchemaIter) Next() bool {
	if it.s == nil {
		return false
	}
	if it.refs == nil {
		it.s.mu.RLock()
		it.refs = make([]types.SchemaRef, 0, len(it.s.types))
		for ref, typ := range it.s.types {
			if typ == "" {
				continue
			}
			if it.filter != nil {
				if _, ok := it.filter[typ]; !ok {
					continue
				}
			}
			b, ok := it.s.blobs[ref]
			if !ok {
				continue
			}
			it.refs = append(it.refs, types.SchemaRef{Ref: ref, Size: uint64(len(b)), Type: typ})
		}
		it.s.mu.RUnlock()
		sort.Slice(it.refs, func(i, j int) bool {
			return it.refs[i].Ref.String() < it.refs[j].Ref.String()
		})
	} else if it.i+1 <= len(it.refs) {
		it.i++
	}
	return it.i < len(it.refs)
}

func (it *memSchemaIter) Err() error {
	return nil
}

func (it *memSchemaIter) Close() error {
	it.s = nil
	return nil
}

func (it *memSchemaIter) SizedRef() types.SizedRef {
	return it.SchemaRef().SizedRef()
}

func (it *memSchemaIter) SchemaRef() types.SchemaRef {
	if it.i >= len(it.refs) {
		return types.SchemaRef{}
	}
	return it.refs[it.i]
}

func (it *memSchemaIter) Decode() (schema.Object, error) {
	ref := it.SizedRef()
	rc, _, err := it.s.FetchBlob(it.ctx, ref.Ref)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return schema.Decode(rc)
}
