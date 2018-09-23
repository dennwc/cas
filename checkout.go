package cas

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dennwc/cas/schema"
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

func (s *Storage) checkoutBlob(ctx context.Context, ref Ref, dst string) error {
	rc, sz, err := s.FetchBlob(ctx, ref)
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.CopyN(f, rc, int64(sz))
	if err != nil {
		return err
	}
	// TODO: write the hash to xattrs
	return f.Close()
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
	case *schema.Directory:
		if err := os.MkdirAll(dst, 0755); err != nil {
			return err
		}
		for _, ent := range obj.List {
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
	case *schema.JoinDirectories:
		if err := os.MkdirAll(dst, 0755); err != nil {
			return err
		}
		for _, ref := range obj.List {
			sub, err := s.DecodeSchema(ctx, ref)
			if err == schema.ErrNotSchema {
				return fmt.Errorf("expected a schema blob in JoinDirectories")
			} else if err != nil {
				return err
			}
			switch sub := sub.(type) {
			case *schema.JoinDirectories:
				// continue checking up this directory
				if err := s.checkoutObject(ctx, ref, sub, dst); err != nil {
					return err
				}
			case *schema.Directory:
				// hit a leaf
				if err := s.checkoutObject(ctx, ref, sub, dst); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected a schema blob in JoinDirectories: %T", sub)
			}
		}
		return nil
	case schema.BlobWrapper:
		// unwrap blob
		return s.checkoutBlob(ctx, obj.DataBlob(), dst)
	default:
		// unknown schema blob - store as json
		return s.checkoutBlob(ctx, oref, dst)
	}
}
