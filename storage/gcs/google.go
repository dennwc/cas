package gcs

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

var (
	_ storage.Storage = (*Storage)(nil)
)

const (
	dirBlobs = "cas/blobs/"
	dirPins  = "cas/pins/"
	dirTmp   = "cas/tmp/"
	metaRef  = "cas:ref"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

func init() {
	storage.RegisterConfig("gcs:ClientConfig", &Config{})
}

type Config struct {
	Bucket string `json:"bucket"`
}

func (c *Config) References() []types.Ref {
	return nil
}

func (c *Config) OpenStorage(ctx context.Context) (storage.Storage, error) {
	cli, err := New(ctx, c.Bucket)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func New(ctx context.Context, bucket string, opts ...option.ClientOption) (*Storage, error) {
	cli, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	b := cli.Bucket(bucket)
	return &Storage{cli: cli, b: b}, nil
}

type Storage struct {
	cli *gcs.Client
	b   *gcs.BucketHandle
}

func (s *Storage) Close() error {
	return s.cli.Close()
}

func (s *Storage) blobObject(ref types.Ref) *gcs.ObjectHandle {
	return s.b.Object(dirBlobs + ref.String())
}

func (s *Storage) pinObject(name string) *gcs.ObjectHandle {
	return s.b.Object(dirPins + name)
}

func (s *Storage) StatBlob(ctx context.Context, ref types.Ref) (uint64, error) {
	if ref.Zero() {
		return 0, storage.ErrInvalidRef
	}
	info, err := s.blobObject(ref).Attrs(ctx)
	if err == gcs.ErrObjectNotExist {
		return 0, storage.ErrNotFound
	} else if err != nil {
		return 0, err
	}
	return uint64(info.Size), nil
}

func (s *Storage) FetchBlob(ctx context.Context, ref types.Ref) (io.ReadCloser, uint64, error) {
	if ref.Zero() {
		return nil, 0, storage.ErrInvalidRef
	}
	r, err := s.blobObject(ref).NewReader(ctx)
	if err == gcs.ErrObjectNotExist {
		return nil, 0, storage.ErrNotFound
	} else if err != nil {
		return nil, 0, err
	}
	return r, uint64(r.Attrs.Size), nil
}

func (s *Storage) iterate(ctx context.Context, pref string) objectsIterator {
	it := s.b.Objects(ctx, &gcs.Query{Delimiter: "/", Prefix: pref})
	return objectsIterator{
		it: it, pref: pref,
	}
}

func (s *Storage) IterateBlobs(ctx context.Context) storage.Iterator {
	return &blobIterator{
		objectsIterator: s.iterate(ctx, dirBlobs),
	}
}

func (s *Storage) BeginBlob(ctx context.Context) (storage.BlobWriter, error) {
	for {
		name := dirTmp + strconv.FormatUint(rnd.Uint64(), 16)
		_, err := s.b.Object(name).Attrs(ctx)
		if err == nil {
			continue
		} else if err != gcs.ErrObjectNotExist {
			return nil, err
		}
		ctx, discard := context.WithCancel(ctx)
		w := s.b.Object(name).If(gcs.Conditions{DoesNotExist: true}).NewWriter(ctx)
		return &blobWriter{s: s, ctx: ctx, w: w, discard: discard, hw: storage.Hash()}, nil
	}
}

func (s *Storage) SetPin(ctx context.Context, name string, ref types.Ref) error {
	w := s.pinObject(name).NewWriter(ctx)
	w.ObjectAttrs.Metadata = map[string]string{
		metaRef: ref.String(),
	}
	return w.Close()
}

func (s *Storage) DeletePin(ctx context.Context, name string) error {
	return s.pinObject(name).Delete(ctx)
}

func (s *Storage) GetPin(ctx context.Context, name string) (types.Ref, error) {
	if strings.ContainsAny(name, "/?&") {
		return types.Ref{}, fmt.Errorf("invalid pin name: %q", name)
	}
	info, err := s.pinObject(name).Attrs(ctx)
	if err == gcs.ErrObjectNotExist {
		return types.Ref{}, storage.ErrNotFound
	} else if err != nil {
		return types.Ref{}, err
	}
	ref, err := types.ParseRef(info.Metadata[metaRef])
	if err != nil {
		return types.Ref{}, err
	}
	return ref, nil
}

func (s *Storage) IteratePins(ctx context.Context) storage.PinIterator {
	return &pinsIterator{
		objectsIterator: s.iterate(ctx, dirPins),
	}
}

type blobWriter struct {
	s       *Storage
	ctx     context.Context
	discard func()
	w       *gcs.Writer
	hw      storage.BlobWriter
	sr      types.SizedRef
}

func (w *blobWriter) Write(p []byte) (int, error) {
	if _, err := w.hw.Write(p); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

func (w *blobWriter) Size() uint64 {
	return w.hw.Size()
}

func (w *blobWriter) Complete() (types.SizedRef, error) {
	sr, err := w.hw.Complete()
	if err != nil {
		return types.SizedRef{}, err
	}
	w.sr = sr
	err = w.w.Close()
	if err != nil {
		w.Close()
		return types.SizedRef{}, err
	}
	return sr, nil
}

func (w *blobWriter) Close() error {
	w.discard()
	if err := w.hw.Close(); err != nil {
		return err
	}
	return w.w.Close()
}

func (w *blobWriter) Commit() error {
	if err := w.hw.Commit(); err != nil {
		return err
	}
	src := w.s.b.Object(w.w.Name)
	dst := w.s.blobObject(w.sr.Ref).If(gcs.Conditions{DoesNotExist: true})
	_, err := dst.CopierFrom(src).Run(w.ctx)
	_ = src.Delete(w.ctx)
	w.discard()
	return err
}

type objectsIterator struct {
	it   *gcs.ObjectIterator
	pref string

	cur *gcs.ObjectAttrs
	err error
}

func (it *objectsIterator) Next() bool {
	it.cur, it.err = it.it.Next()
	if it.err == iterator.Done {
		it.err = nil
		return false
	}
	if it.cur != nil {
		it.cur.Name = strings.TrimPrefix(it.cur.Name, it.pref)
	}
	return it.err == nil
}

func (it *objectsIterator) Err() error {
	return it.err
}

func (it *objectsIterator) Close() error {
	return nil
}

type blobIterator struct {
	objectsIterator
	sr types.SizedRef
}

func (it *blobIterator) SizedRef() types.SizedRef {
	return it.sr
}

func (it *blobIterator) Next() bool {
	if !it.objectsIterator.Next() || it.cur == nil {
		return false
	}
	it.sr.Size = uint64(it.cur.Size)
	it.sr.Ref, it.err = types.ParseRef(it.cur.Name)
	return it.err == nil
}

type pinsIterator struct {
	objectsIterator
	pin types.Pin
}

func (it *pinsIterator) Pin() types.Pin {
	return it.pin
}

func (it *pinsIterator) Next() bool {
	if !it.objectsIterator.Next() || it.cur == nil {
		return false
	}
	it.pin.Name = it.cur.Name
	it.pin.Ref, it.err = types.ParseRef(it.cur.Metadata[metaRef])
	return it.err == nil
}
