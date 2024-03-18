package cas

import (
	"context"
	"io"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

func checkConfig(c *StoreConfig) *StoreConfig {
	if c == nil {
		return &StoreConfig{}
	}
	return c
}

type StoreConfig struct {
	Expect    types.SizedRef // expected size and ref; can be set separately
	IndexOnly bool           // write metadata only
	Split     *SplitConfig
}

func (c *StoreConfig) checkRef(sr SizedRef) error {
	if !c.Expect.Ref.Zero() && c.Expect.Ref != sr.Ref {
		return storage.ErrRefMissmatch{Exp: c.Expect.Ref, Got: sr.Ref}
	} else if c.Expect.Size != 0 && c.Expect.Size != sr.Size {
		return storage.ErrSizeMissmatch{Exp: c.Expect.Size, Got: sr.Size}
	}
	return nil
}

type SplitFunc func(p []byte) int

type SplitConfig struct {
	Splitter SplitFunc // use this split function instead of size-based
	Min, Max uint64    // in bytes
	PerLevel uint      // chunks on each schema level
}

func (s *Storage) BeginBlob(ctx context.Context) (storage.BlobWriter, error) {
	return s.st.BeginBlob(ctx)
}

// StoreBlob writes the data from r according to a config.
func (s *Storage) StoreBlob(ctx context.Context, r io.Reader, conf *StoreConfig) (SizedRef, error) {
	conf = checkConfig(conf)

	if conf.Split != nil {
		// we need to split the blob - use a different code path
		href, sr, err := s.splitBlob(ctx, r, conf.Split, conf.IndexOnly)
		if err != nil {
			return SizedRef{}, err
		}
		// if we know the expected ref - check the content ref now before we lose it
		if err = conf.checkRef(sr); err != nil {
			return SizedRef{}, err
		}
		// return synthetic ref: size from content, but the ref from a schema blob
		// TODO: it might cause problems
		return SizedRef{Ref: href.Ref, Size: sr.Size}, nil
	}

	if !conf.Expect.Ref.Zero() {
		// if we have this blob already, don't bother saving it again
		if sz, err := s.StatBlob(ctx, conf.Expect.Ref); err == nil {
			// TODO: hash the reader to make sure that caller provided the right file?
			return SizedRef{Ref: conf.Expect.Ref, Size: sz}, nil
		}
	}

	// store content as a single blob
	var (
		w   storage.BlobWriter
		err error
	)
	if conf.IndexOnly {
		w = storage.Hash()
	} else {
		w, err = s.st.BeginBlob(ctx)
	}
	if err != nil {
		return SizedRef{}, err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	if err != nil {
		return SizedRef{}, err
	}
	sr, err := s.completeBlob(ctx, w, conf.Expect.Ref)
	if err != nil {
		return SizedRef{}, err
	}
	if err = conf.checkRef(sr); err != nil {
		return SizedRef{}, err
	}
	return sr, nil
}

// completeBlob commits the blob to the storage.
// It will ignore empty blobs and will ensure that the blob matches expected ref.
func (s *Storage) completeBlob(ctx context.Context, w storage.BlobWriter, exp Ref) (SizedRef, error) {
	defer w.Close()
	sr, err := w.Complete()
	if err != nil {
		return SizedRef{}, err
	}
	if !exp.Zero() && exp != sr.Ref {
		// wrong content
		return SizedRef{}, storage.ErrRefMissmatch{Exp: exp, Got: sr.Ref}
	}
	if sr.Ref.Empty() {
		// do not store empty blobs - we can generate them
		w.Close()
		return SizedRef{Ref: exp, Size: 0}, nil
	}
	err = w.Commit()
	return sr, err
}

// splitBlob stores blob while splitting it according to config.
// It returns a ref of a splitted blob and a virtual sized ref that describes the whole blob.
func (s *Storage) splitBlob(ctx context.Context, r io.Reader, conf *SplitConfig, indexOnly bool) (meta, cont types.SizedRef, _ error) {
	// set defaults
	if conf.PerLevel == 0 {
		conf.PerLevel = maxDirEntries
	}
	if conf.Splitter == nil && conf.Max == 0 {
		conf.Max = 64 * 1024 * 1024
	}
	// hash whole stream content in the background
	h := types.NewRef().Hash()
	r = io.TeeReader(r, h)

	bsize := 128 * 1024
	if conf.Max != 0 && conf.Max < uint64(bsize) {
		bsize = int(conf.Max)
	}
	var (
		isEOF = false
		refs  []types.SizedRef
		buf   = make([]byte, 0, bsize) // read buffer
	)
	for !isEOF {
		var (
			cur uint64             // size of current chunk
			bw  storage.BlobWriter // chunk writer
			err error
		)
		if indexOnly {
			bw = storage.Hash()
		} else {
			bw, err = s.BeginBlob(ctx)
		}
		if err != nil {
			return types.SizedRef{}, types.SizedRef{}, err
		}
		// read into current chunk until we hit a size limit, or until splitter triggers
		for {
			// if nothing to process from the previous chunk, read new data
			if len(buf) == 0 {
				buf = buf[:cap(buf)]
				n, err := r.Read(buf)
				buf = buf[:n]
				if n != 0 && err == io.EOF {
					err = nil // suppress EOF if the reader actually returned some data
				}
				if err == io.EOF {
					// nothing was read, break read loop and commit current chunk
					// don't forget to terminate the main loop as well, since we are done
					isEOF = true
					break
				} else if err != nil {
					bw.Close() // discard current chunk
					return types.SizedRef{}, types.SizedRef{}, err
				}
			}
			// select what part of the buffer will be written
			// it will be smaller in case we want to split
			wbuf := buf
			splitted := false
			// only run split function if we are above the min size threshold
			if conf.Splitter != nil && (conf.Min == 0 || cur > conf.Min) {
				if i := conf.Splitter(buf); i >= 0 && i < len(buf) {
					// write chunk including the separator
					wbuf = buf[:i+1]
					// everything else will be written to the next chunk - defer it
					buf = buf[i+1:]
					splitted = true
				}
			}
			if !splitted {
				// whole buffer will be written, nothing to defer to the next chunk
				buf = buf[:0]
			}
			// write buffer before the separator (or just the whole buffer)
			n, err := bw.Write(wbuf)
			if err != nil {
				bw.Close()
				return types.SizedRef{}, types.SizedRef{}, err
			}
			cur += uint64(n)
			// terminate the read loop is we want to split, or we hit a max size limit
			if splitted || (conf.Max > 0 && cur >= conf.Max) {
				break
			}
		}
		// complete current part; we don't know the ref, unfortunately
		sr, err := s.completeBlob(ctx, bw, Ref{})
		if err != nil {
			return types.SizedRef{}, types.SizedRef{}, err
		}
		refs = append(refs, sr)
	}
	// calculate the content ref
	ref := types.NewRef().WithHash(h)
	// collect all chunk refs to a schema blob
	list := &schema.InlineList{
		Ref:  &ref,
		Elem: typeSizedRef,
		List: make([]schema.Object, 0, len(refs)),
	}
	var size uint64
	for _, ref := range refs {
		ref := ref
		list.List = append(list.List, &ref)
		size += ref.Size
	}
	list.Stats = Stats{schema.StatDataSize: size}
	// store the schema blob and return both refs
	lref, err := s.StoreSchema(ctx, list)
	if err != nil {
		return types.SizedRef{}, types.SizedRef{}, err
	}
	return lref, SizedRef{Ref: ref, Size: size}, nil
}
