package storage

import (
	"context"

	"github.com/dennwc/cas/types"
)

func WriteBytes(ctx context.Context, s BlobStorage, data []byte) (types.SizedRef, error) {
	w, err := s.BeginBlob(ctx)
	if err != nil {
		return types.SizedRef{}, err
	}
	defer w.Close()

	_, err = w.Write(data)
	if err != nil {
		return types.SizedRef{}, err
	}

	sr, err := w.Complete()
	if err != nil {
		return types.SizedRef{}, err
	}
	err = w.Commit()
	if err != nil {
		return types.SizedRef{}, err
	}
	return sr, nil
}
