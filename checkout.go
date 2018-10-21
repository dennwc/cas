package cas

import (
	"context"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/dennwc/cas/schema"
	"github.com/dennwc/cas/storage"
	"github.com/dennwc/cas/types"
)

// Checkout restores content of ref into the dst.
func (s *Storage) Checkout(ctx context.Context, ref Ref, dst string) error {
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("path already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	return s.checkoutFileOrDir(ctx, ref, dst)
}

func (s *Storage) checkoutBlobData(ctx context.Context, r io.Reader, sr SizedRef, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	var h hash.Hash
	if !sr.Ref.Zero() {
		h = sr.Ref.Hash()
		r = io.TeeReader(r, h)
	}
	if sr.Size != 0 {
		_, err = io.CopyN(f, r, int64(sr.Size))
	} else {
		_, err = io.Copy(f, r)
	}
	if err != nil {
		return err
	}
	if h != nil {
		ref := sr.Ref.WithHash(h)
		if sr.Ref != ref {
			f.Close()
			os.Remove(dst)
			return storage.ErrRefMissmatch{Exp: sr.Ref, Got: ref}
		}
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	err = SaveRefFile(ctx, f, fi, sr.Ref)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) checkoutBlob(ctx context.Context, ref Ref, dst string) error {
	rc, sz, err := s.FetchBlob(ctx, ref)
	if err != nil {
		return err
	}
	defer rc.Close()

	return s.checkoutBlobData(ctx, rc, SizedRef{Ref: ref, Size: sz}, dst)
}

type multipartReader struct {
	s   *Storage
	ctx context.Context

	parts []types.Ref // stack
	cur   io.ReadCloser
	err   error
}

func (r *multipartReader) addPartsFrom(obj schema.Object) error {
	switch obj := obj.(type) {
	case *schema.InlineList:
		if obj.Elem != typeSizedRef {
			return fmt.Errorf("expected sized ref, got: %q", obj.Elem)
		}
		for i := len(obj.List) - 1; i >= 0; i-- {
			sub, ok := obj.List[i].(schema.BlobWrapper)
			if !ok {
				return fmt.Errorf("expected sized ref, got: %T", obj.List[i])
			}
			r.parts = append(r.parts, sub.DataBlob())
		}
		return nil
	case *schema.List:
		if obj.Elem != typeSizedRef {
			return fmt.Errorf("expected sized ref, got: %q", obj.Elem)
		}
		for i := len(obj.List) - 1; i >= 0; i-- {
			r.parts = append(r.parts, obj.List[i])
		}
		return nil
	case schema.BlobWrapper:
		r.parts = append(r.parts, obj.DataBlob())
		return nil
	default:
		return fmt.Errorf("unsupported file part: %T", obj)
	}
}

func (r *multipartReader) Read(p []byte) (int, error) {
	for {
		if r.err != nil {
			return 0, r.err
		}

		if r.cur == nil {
			if len(r.parts) == 0 {
				return 0, io.EOF
			}
			i := len(r.parts) - 1
			ref := r.parts[i]
			r.parts = r.parts[:i]

			obj, err := r.s.DecodeSchema(r.ctx, ref)
			if err == nil {
				err = r.addPartsFrom(obj)
				if err != nil {
					r.err = err
					return 0, err
				}
			} else if err == schema.ErrNotSchema {
				rc, _, err := r.s.FetchBlob(r.ctx, ref)
				if err != nil {
					r.err = err
					return 0, err
				}
				r.cur = rc
			} else {
				r.err = err
				return 0, err
			}
			continue
		}

		n, err := r.cur.Read(p)
		if err == io.EOF {
			err = nil
			r.cur.Close()
			r.cur = nil
			if n == 0 {
				continue
			}
		}
		if err != nil {
			r.err = err
		}
		return n, err
	}
}

func (r *multipartReader) Close() error {
	if r.cur != nil {
		r.cur.Close()
		r.cur = nil
	}
	r.parts = nil
	return r.err
}

func (s *Storage) openMultipart(ctx context.Context, _ Ref, obj schema.Object) (io.ReadCloser, SizedRef, error) {
	r := &multipartReader{s: s, ctx: ctx}
	var sr SizedRef
	switch obj := obj.(type) {
	case *schema.InlineList:
		if obj.Ref != nil {
			sr.Ref = *obj.Ref
		}
		sr.Size = obj.Stats.Size()
	case *schema.List:
		if obj.Ref != nil {
			sr.Ref = *obj.Ref
		}
		sr.Size = obj.Stats.Size()
	}
	r.addPartsFrom(obj)
	if r.err != nil {
		r.Close()
		return nil, SizedRef{}, r.err
	}
	return r, sr, nil
}

func (s *Storage) checkoutMultipart(ctx context.Context, ref Ref, obj schema.Object, dst string) error {
	rc, sr, err := s.openMultipart(ctx, ref, obj)
	if err != nil {
		return err
	}
	defer rc.Close()

	return s.checkoutBlobData(ctx, rc, sr, dst)
}

func (s *Storage) checkoutDir(ctx context.Context, ref Ref, obj schema.Object, dst string) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}
	switch obj := obj.(type) {
	case *schema.InlineList:
		for _, e := range obj.List {
			ent, ok := e.(*schema.DirEntry)
			if !ok {
				return fmt.Errorf("expected dir entry, got: %T", e)
			}
			spath := filepath.Join(dst, ent.Name)
			sub, err := s.DecodeSchema(ctx, ent.Ref)
			if err == nil {
				// schema object - sub directory, or schema blob
				err = s.checkoutObject(ctx, ent.Ref, sub, spath)
			} else if err == schema.ErrNotSchema {
				// file blob
				err = s.checkoutBlob(ctx, ent.Ref, spath)
			}
			if err != nil {
				return err
			}
		}
		return nil
	case *schema.List:
		for _, ref := range obj.List {
			sub, err := s.DecodeSchema(ctx, ref)
			if err == schema.ErrNotSchema {
				return fmt.Errorf("expected a schema blob in JoinDirectories")
			} else if err != nil {
				return err
			}
			switch sub := sub.(type) {
			case *schema.List, *schema.InlineList:
				// continue checking up this directory
				if err := s.checkoutObject(ctx, ref, sub, dst); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected a schema blob in JoinDirectories: %T", sub)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported dir object: %T", obj)
	}
}

func (s *Storage) checkoutFileOrDir(ctx context.Context, ref Ref, dst string) error {
	obj, err := s.DecodeSchema(ctx, ref)
	if err == schema.ErrNotSchema {
		return s.checkoutBlob(ctx, ref, dst)
	} else if err != nil {
		return err
	}
	return s.checkoutObject(ctx, ref, obj, dst)
}

func (s *Storage) checkoutObject(ctx context.Context, oref Ref, obj schema.Object, dst string) error {
	switch obj := obj.(type) {
	case *schema.InlineList:
		switch obj.Elem {
		case typeDirEnt:
			return s.checkoutDir(ctx, oref, obj, dst)
		case typeSizedRef:
			return s.checkoutMultipart(ctx, oref, obj, dst)
		default:
			return fmt.Errorf("unsupported list element: %q", obj.Elem)
		}
	case *schema.List:
		switch obj.Elem {
		case typeDirEnt:
			return s.checkoutDir(ctx, oref, obj, dst)
		case typeSizedRef:
			return s.checkoutMultipart(ctx, oref, obj, dst)
		default:
			return fmt.Errorf("unsupported list element: %q", obj.Elem)
		}
	case schema.BlobWrapper:
		// unwrap blob
		// TODO: might require recursion
		return s.checkoutBlob(ctx, obj.DataBlob(), dst)
	default:
		// unknown schema blob - store as json
		return s.checkoutBlob(ctx, oref, dst)
	}
}
