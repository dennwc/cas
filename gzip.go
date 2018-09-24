package cas

import (
	"compress/gzip"
	"context"
	"io"

	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

func (s *Storage) indexGZIP(ctx context.Context) storage.BlobWriter {
	pr, pw := io.Pipe()

	errc := make(chan error, 1)
	ch := make(chan storage.BlobWriter, 1)
	go func() {
		errc, ch := errc, ch
		defer func() {
			if errc != nil {
				close(errc)
			}
		}()
		defer pr.Close()
		zr, err := gzip.NewReader(pr)
		if err != nil {
			errc <- err
			return
		}
		defer zr.Close()
		hw := storage.Hash()
		_, err = io.Copy(hw, zr)
		if err != nil {
			hw.Close()
			errc <- err
			return
		}
		ch <- hw
		errc = nil
	}()
	return &gzipIndexWriter{pw: pw, errc: errc, ch: ch}
}

type gzipIndexWriter struct {
	pw   *io.PipeWriter
	errc <-chan error
	ch   <-chan storage.BlobWriter
	hw   storage.BlobWriter
}

func (w *gzipIndexWriter) Size() uint64 {
	return w.hw.Size()
}

func (w *gzipIndexWriter) Close() error {
	if w.errc == nil {
		return nil
	} else if w.hw != nil {
		return w.hw.Close()
	}
	if err := w.pw.Close(); err != nil {
		return err
	}
	select {
	case err := <-w.errc:
		w.errc = nil
		return err
	case hw := <-w.ch:
		hw.Close()
		return nil
	}
}

func (w *gzipIndexWriter) Commit() error {
	if w.errc == nil {
		return storage.ErrBlobDiscarded
	}
	if w.hw == nil {
		if _, err := w.Complete(); err != nil {
			return err
		}
	}
	return w.hw.Commit()
}

func (w *gzipIndexWriter) Complete() (types.SizedRef, error) {
	if w.errc == nil {
		return types.SizedRef{}, storage.ErrBlobDiscarded
	}
	if w.hw != nil {
		return w.hw.Complete()
	}
	if err := w.pw.Close(); err != nil {
		return types.SizedRef{}, err
	}
	select {
	case err := <-w.errc:
		w.errc = nil
		return types.SizedRef{}, err
	case hw := <-w.ch:
		w.hw = hw
		return hw.Complete()
	}
}

func (w *gzipIndexWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}
