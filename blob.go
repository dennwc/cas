package cas

import (
	"context"
	"io"
)

type Blob interface {
	Ref() Ref
	Fetch(ctx context.Context) (io.ReadCloser, uint64, error)
}
